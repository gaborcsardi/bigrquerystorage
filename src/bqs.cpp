// [[Rcpp::plugins(cpp11)]]
// [[Rcpp::depends(RcppThread)]]

#include <fstream>
#include <string>
#include <vector>
#include <grpc/grpc.h>
#include <grpc/support/log.h>
#include <grpcpp/grpcpp.h>
#include "google/cloud/bigquery/storage/v1/stream.pb.h"
#include "google/cloud/bigquery/storage/v1/storage.pb.h"
#include "google/cloud/bigquery/storage/v1/storage.grpc.pb.h"
#include <Rcpp.h>
#include <RcppThread.h>
#include <RProgress.h>

using google::cloud::bigquery::storage::v1::ReadSession;
using google::cloud::bigquery::storage::v1::BigQueryRead;
using google::cloud::bigquery::storage::v1::CreateReadSessionRequest;
using google::cloud::bigquery::storage::v1::DataFormat;
using google::cloud::bigquery::storage::v1::ReadRowsRequest;
using google::cloud::bigquery::storage::v1::ReadRowsResponse;
using google::cloud::bigquery::storage::v1::ReadStream;
using google::cloud::bigquery::storage::v1::SplitReadStreamRequest;
using google::cloud::bigquery::storage::v1::SplitReadStreamResponse;
using grpc::CallCredentials;
using grpc::ChannelCredentials;
using grpc::CompositeChannelCredentials;
using grpc::SslCredentials;
using grpc::SslCredentialsOptions;
using grpc::GoogleDefaultCredentials;
using grpc::AccessTokenCredentials;
using grpc::GoogleRefreshTokenCredentials;
using grpc::Channel;
using grpc::ChannelArguments;
using grpc::CreateCustomChannel;
using grpc::Status;
using grpc::ClientContext;
using grpc::ClientReader;
using std::shared_ptr;
using std::endl;
using std::ifstream;
using std::istreambuf_iterator;
using std::unique_ptr;
using Rcpp::Rcerr;
using Rcpp::stop;
using Rcpp::checkUserInterrupt;
using Rcpp::XPtr;
using Rcpp::List;

// Define a default logger for gRPC
void bqs_default_log(gpr_log_func_args* args) {
  Rcerr << args->message << endl;
}

// Set gRPC verbosity level
// [[Rcpp::export]]
void bqs_set_log_verbosity(int severity) {
  //-1 UNSET
  // 0 DEBUG
  // 1 INFO
  // 2 ERROR
  // 3 QUIET
  gpr_set_log_verbosity(static_cast<gpr_log_severity>(severity));
}

// Set gRPC default logger
// [[Rcpp::export]]
void bqs_init_logger() {
  gpr_set_log_function(bqs_default_log);
  bqs_set_log_verbosity(2);
}

// Check gRPC version
// [[Rcpp::export]]
std::string grpc_version() {
  std::string version;
  version += grpc_version_string();
  version += " ";
  version += grpc_g_stands_for();
  return version;
}

// Simple read file to read configuration from json
// [[Rcpp::export]]
std::string readfile(std::string filename)
{
  ifstream ifs(filename);
  std::string content( (istreambuf_iterator<char>(ifs) ),
                       (istreambuf_iterator<char>()    ) );
  return content;
}

// append std::string at the end of a std::vector<std::uint8_t> std::vector
void to_raw(const std::string input, std::vector<std::uint8_t>* output) {
  output->insert(output->end(), input.begin(), input.end());
}

class BigQueryReadClient {
public:
  BigQueryReadClient(shared_ptr<Channel> channel)
    : stub_(BigQueryRead::NewStub(channel)) {
  }
  void SetClientInfo(const std::string &client_info) {
    client_info_ = client_info;
  }
  std::string GetClientInfo() {
    return client_info_;
  }
  ReadSession CreateReadSession(const std::string& project,
                                const std::string& dataset,
                                const std::string& table,
                                const std::string& parent,
                                const std::int64_t& timestamp_seconds,
                                const std::int32_t& timestamp_nanos,
                                const std::vector<std::string>& selected_fields,
                                const std::string& row_restriction
  ) {
    CreateReadSessionRequest method_request;
    ReadSession *read_session = method_request.mutable_read_session();
    std::string table_fullname =
      "projects/" + project + "/datasets/" + dataset + "/tables/" + table;
    read_session->set_table(table_fullname);
    read_session->set_data_format(DataFormat::ARROW);
    if (timestamp_seconds > 0 || timestamp_nanos > 0) {
      read_session->mutable_table_modifiers()->
        mutable_snapshot_time()->set_seconds(timestamp_seconds);
      read_session->mutable_table_modifiers()->
        mutable_snapshot_time()->set_nanos(timestamp_nanos);
    }
    if (!row_restriction.empty()) {
      read_session->mutable_read_options()->
        set_row_restriction(row_restriction);
    }
    for (int i = 0; i < int(selected_fields.size()); i++) {
      read_session->mutable_read_options()->
        add_selected_fields(selected_fields[i]);
    }
    method_request.set_parent("projects/" + parent);
    ClientContext context;
    context.AddMetadata("x-goog-request-params",
                        "read_session.table=" + table_fullname);
    context.AddMetadata("x-goog-api-client", client_info_);
    ReadSession method_response;

    // The actual RPC.
    Status status = stub_->
      CreateReadSession(&context, method_request, &method_response);
    if (!status.ok()) {
      std::string err;
      err += "gRPC method CreateReadSession error -> ";
      err += status.error_message();
      stop(err.c_str());
    }
    return method_response;
  }
  std::vector<std::string> SplitReadStream(
      std::string& stream, double& fraction) {

    SplitReadStreamRequest method_request;
    method_request.set_name(stream);
    method_request.set_fraction(fraction);

    ClientContext context;
    context.AddMetadata("x-goog-request-params",
                        "name=" + stream);
    context.AddMetadata("x-goog-api-client", client_info_);
    SplitReadStreamResponse method_response;

    // The actual RPC.
    Status status = stub_->
      SplitReadStream(&context, method_request, &method_response);
    if (!status.ok()) {
      std::string err;
      err += "gRPC method SplitReadStream error -> ";
      err += status.error_message();
      stop(err.c_str());
    }

    return {method_response.primary_stream().name(),
            method_response.remainder_stream().name()};
  }
  void ReadRows(const std::string stream,
                std::vector<std::uint8_t>* ipc_stream,
                std::int64_t& n,
                long int& rows_count,
                long int& pages_count,
                bool quiet) {

    ClientContext context;
    context.AddMetadata("x-goog-request-params", "read_stream=" + stream);
    context.AddMetadata("x-goog-api-client", client_info_);

    ReadRowsRequest method_request;
    method_request.set_read_stream(stream);
    method_request.set_offset(0);

    ReadRowsResponse method_response;

    unique_ptr<ClientReader<ReadRowsResponse> > reader(
        stub_->ReadRows(&context, method_request));

    RProgress::RProgress pb(
        "\033[42m\033[30mReading (:current%)\033[39m\033[49m [:bar] ETA :eta");
    pb.set_cursor_char(">");

    while (reader->Read(&method_response)) {
      if (n > 0 && method_request.offset() + rows_count >= n) {
        break;
      }
      to_raw(method_response.arrow_record_batch().serialized_record_batch(),
             ipc_stream);
      method_request.set_offset(
        method_request.offset() + method_response.row_count()
      );
      pages_count += 1;
      if (!quiet) {
        pb.update(method_response.stats().progress().at_response_end() * 2);
      } else {
        checkUserInterrupt();
      }
    }
    if (n < 0) {
      Status status = reader->Finish();
      if (!status.ok()) {
        std::string err;
        err += "grpc method ReadRows error -> ";
        err += status.error_message();
        stop(err.c_str());
      }
    }
    rows_count += method_request.offset();
  }
  std::vector<std::uint8_t> ReadRowsAllGasNoBrakes(const std::string& stream) {
    ClientContext context;
    context.AddMetadata("x-goog-request-params", "read_stream=" + stream);
    context.AddMetadata("x-goog-api-client", client_info_);

    ReadRowsRequest method_request;
    method_request.set_read_stream(stream);
    method_request.set_offset(0);

    ReadRowsResponse method_response;

    unique_ptr<ClientReader<ReadRowsResponse> > reader(
        stub_->ReadRows(&context, method_request));
    std::vector<std::uint8_t> ipc_stream;
    while (reader->Read(&method_response)) {
      to_raw(method_response.arrow_record_batch().serialized_record_batch(),
             &ipc_stream);
      method_request.set_offset(
        method_request.offset() + method_response.row_count()
      );
    }
    Status status = reader->Finish();
    return ipc_stream;
  }
private:
  unique_ptr<BigQueryRead::Stub> stub_;
  std::string client_info_;
};



// -- Credentials functions ----------------------------------------------------

std::shared_ptr<ChannelCredentials> bqs_ssl(std::string root_certificate) {
  SslCredentialsOptions ssl_options;
  if (!root_certificate.empty()) {
    ssl_options.pem_root_certs = root_certificate;
  }
  return SslCredentials(ssl_options);
}

std::shared_ptr<ChannelCredentials> bqs_credentials(
    shared_ptr<ChannelCredentials> channel_cred,
    shared_ptr<CallCredentials> call_cred = nullptr) {
  if (channel_cred) {
    if (call_cred) {
      channel_cred = CompositeChannelCredentials(
        channel_cred,
        call_cred
      );
    }
  }
  return channel_cred;
}

std::shared_ptr<ChannelCredentials> bqs_google_credentials() {
  auto gcp_cred = GoogleDefaultCredentials();
  return bqs_credentials(gcp_cred);
}

std::shared_ptr<ChannelCredentials> bqs_refresh_token_credentials(
    std::string refresh_token, std::string root_certificate = "") {
  auto ssl_cred = bqs_ssl(root_certificate);
  auto token_cred = GoogleRefreshTokenCredentials(refresh_token);
  return bqs_credentials(ssl_cred, token_cred);
}

std::shared_ptr<ChannelCredentials> bqs_access_token_credentials(
    std::string access_token, std::string root_certificate = "") {
  auto ssl_cred = bqs_ssl(root_certificate);
  auto token_cred = AccessTokenCredentials(access_token);
  return bqs_credentials(ssl_cred, token_cred);
}

// -- Client functions ---------------------------------------------------------

SEXP bqs_read_client(shared_ptr<ChannelCredentials> cred,
                     std::string client_info,
                     std::string service_configuration,
                     std::string target) {

  ChannelArguments channel_arguments;
  channel_arguments.SetServiceConfigJSON(service_configuration);

  BigQueryReadClient *client = new BigQueryReadClient(
    CreateCustomChannel(target, cred, channel_arguments)
  );

  client->SetClientInfo(client_info);

  XPtr<BigQueryReadClient> ptr(client, true);

  return ptr;

}

// [[Rcpp::export]]
SEXP bqs_client(std::string client_info,
                std::string service_configuration,
                std::string refresh_token = "",
                std::string access_token = "",
                std::string root_certificate = "",
                std::string target = "bigquerystorage.googleapis.com:443") {

  shared_ptr<ChannelCredentials> cred = bqs_google_credentials();
  if (!cred) {
    cred = bqs_refresh_token_credentials(refresh_token, root_certificate);
  }
  if (!cred) {
    cred = bqs_access_token_credentials(access_token, root_certificate);
  }
  if (!cred) {
    stop("Could not create credentials.");
  }

  return bqs_read_client(cred, client_info, service_configuration, target);

}

// [[Rcpp::export]]
SEXP bqs_method_read_session(SEXP client,
                             std::string project,
                             std::string dataset,
                             std::string table,
                             std::string parent,
                             std::vector<std::string> selected_fields,
                             std::string row_restriction = "",
                             std::int64_t timestamp_seconds = 0,
                             std::int32_t timestamp_nanos = 0) {

  XPtr<BigQueryReadClient> client_ptr(client);

  ReadSession *read_session = new ReadSession(client_ptr->
    CreateReadSession(project,
                      dataset,
                      table,
                      parent,
                      timestamp_seconds,
                      timestamp_nanos,
                      selected_fields,
                      row_restriction)
  );

  XPtr<ReadSession> x_ptr(read_session, true);

  return x_ptr;

}

// [[Rcpp::export]]
std::vector<std::uint8_t> bqs_read_session_schema(SEXP read_session) {
  XPtr<ReadSession> read_session_ptr(read_session);
  std::vector<std::uint8_t> schema;
  to_raw(read_session_ptr->arrow_schema().serialized_schema(), &schema);
  return schema;
}

// [[Rcpp::export]]
std::vector<std::string> bqs_read_session_stream_names(SEXP read_session) {

  XPtr<ReadSession> read_session_ptr(read_session);
  std::vector<std::string> streams;
  for (int i = 0; i < read_session_ptr->streams_size(); i++) {
    streams.push_back(read_session_ptr->streams(i).name());
  }
  return streams;

}

// [[Rcpp::export]]
std::vector<std::string> bqs_method_split_read_stream(SEXP client,
                                                      std::string stream,
                                                      double fraction = 0.5) {

  XPtr<BigQueryReadClient> client_ptr(client);

  return client_ptr->SplitReadStream(stream, fraction);

}

// [[Rcpp::export]]
std::vector< std::vector<uint8_t> > bqs_method_read_rows(
    SEXP client,
    std::vector<std::string> streams) {

  XPtr<BigQueryReadClient> client_ptr(client);

  std::vector< std::vector<uint8_t> > ipc_streams(streams.size());
  RcppThread::parallelFor(0, streams.size(), [&ipc_streams, &client_ptr, &streams] (int i) {
    ipc_streams[i] = client_ptr->ReadRowsAllGasNoBrakes(streams[i]);
  });

  return ipc_streams;

}

//' @noRd
// [[Rcpp::export]]
SEXP bqs_ipc_stream(std::string project,
                    std::string dataset,
                    std::string table,
                    std::string parent,
                    std::int64_t n,
                    std::string client_info,
                    std::string service_configuration,
                    std::string access_token,
                    std::string root_certificate,
                    std::int64_t timestamp_seconds,
                    std::int32_t timestamp_nanos,
                    std::vector<std::string> selected_fields,
                    std::string row_restriction,
                    bool quiet = false) {

  shared_ptr<ChannelCredentials> channel_credentials;
  if (access_token.empty()) {
    channel_credentials = GoogleDefaultCredentials();
  } else {
    SslCredentialsOptions ssl_options;
    if (!root_certificate.empty()) {
      ssl_options.pem_root_certs = readfile(root_certificate);
    }
    channel_credentials = CompositeChannelCredentials(
      SslCredentials(ssl_options),
      AccessTokenCredentials(access_token));
  }
  ChannelArguments channel_arguments;
  channel_arguments.SetServiceConfigJSON(readfile(service_configuration));

  BigQueryReadClient client(
      CreateCustomChannel("bigquerystorage.googleapis.com:443",
                          channel_credentials,
                          channel_arguments));

  client.SetClientInfo(client_info);

  std::vector<std::uint8_t> schema;
  std::vector<std::uint8_t> ipc_stream;
  long int rows_count = 0;
  long int pages_count = 0;

  // Retrieve ReadSession
  ReadSession read_session = client.CreateReadSession(project,
                                                      dataset,
                                                      table,
                                                      parent,
                                                      timestamp_seconds,
                                                      timestamp_nanos,
                                                      selected_fields,
                                                      row_restriction);
  // Add schema to IPC stream
  to_raw(read_session.arrow_schema().serialized_schema(), &schema);

  // Add batches to IPC stream
  for (int i = 0; i < read_session.streams_size(); i++) {
    client.ReadRows(read_session.streams(i).name(), &ipc_stream, n, rows_count, pages_count, quiet);
  }

  if (!quiet) {
    REprintf("Streamed %ld rows in %ld messages.\n", rows_count, pages_count);
  }

  // Return stream
  return List::create(schema, ipc_stream);
}
