// Generated by using Rcpp::compileAttributes() -> do not edit by hand
// Generator token: 10BE3573-1514-4C36-9D1C-5A225CD40393

#include "../inst/include/bigrquerystorage.h"
#include <Rcpp.h>

using namespace Rcpp;

#ifdef RCPP_USE_GLOBAL_ROSTREAM
Rcpp::Rostream<true>&  Rcpp::Rcout = Rcpp::Rcpp_cout_get();
Rcpp::Rostream<false>& Rcpp::Rcerr = Rcpp::Rcpp_cerr_get();
#endif

// bqs_set_log_verbosity
void bqs_set_log_verbosity(int severity);
RcppExport SEXP _bigrquerystorage_bqs_set_log_verbosity(SEXP severitySEXP) {
BEGIN_RCPP
    Rcpp::traits::input_parameter< int >::type severity(severitySEXP);
    bqs_set_log_verbosity(severity);
    return R_NilValue;
END_RCPP
}
// bqs_init_logger
void bqs_init_logger();
RcppExport SEXP _bigrquerystorage_bqs_init_logger() {
BEGIN_RCPP
    bqs_init_logger();
    return R_NilValue;
END_RCPP
}
// grpc_version
std::string grpc_version();
RcppExport SEXP _bigrquerystorage_grpc_version() {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    rcpp_result_gen = Rcpp::wrap(grpc_version());
    return rcpp_result_gen;
END_RCPP
}
// bqs_client
SEXP bqs_client(std::string client_info, std::string service_configuration, std::string refresh_token, std::string access_token, std::string root_certificate, std::string target);
RcppExport SEXP _bigrquerystorage_bqs_client(SEXP client_infoSEXP, SEXP service_configurationSEXP, SEXP refresh_tokenSEXP, SEXP access_tokenSEXP, SEXP root_certificateSEXP, SEXP targetSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::traits::input_parameter< std::string >::type client_info(client_infoSEXP);
    Rcpp::traits::input_parameter< std::string >::type service_configuration(service_configurationSEXP);
    Rcpp::traits::input_parameter< std::string >::type refresh_token(refresh_tokenSEXP);
    Rcpp::traits::input_parameter< std::string >::type access_token(access_tokenSEXP);
    Rcpp::traits::input_parameter< std::string >::type root_certificate(root_certificateSEXP);
    Rcpp::traits::input_parameter< std::string >::type target(targetSEXP);
    rcpp_result_gen = Rcpp::wrap(bqs_client(client_info, service_configuration, refresh_token, access_token, root_certificate, target));
    return rcpp_result_gen;
END_RCPP
}
// bqs_ipc_stream
SEXP bqs_ipc_stream(SEXP client, std::string project, std::string dataset, std::string table, std::string parent, std::int64_t n, std::vector<std::string> selected_fields, std::string row_restriction, std::double_t sample_percentage, std::int64_t timestamp_seconds, std::int32_t timestamp_nanos, bool quiet);
RcppExport SEXP _bigrquerystorage_bqs_ipc_stream(SEXP clientSEXP, SEXP projectSEXP, SEXP datasetSEXP, SEXP tableSEXP, SEXP parentSEXP, SEXP nSEXP, SEXP selected_fieldsSEXP, SEXP row_restrictionSEXP, SEXP sample_percentageSEXP, SEXP timestamp_secondsSEXP, SEXP timestamp_nanosSEXP, SEXP quietSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::traits::input_parameter< SEXP >::type client(clientSEXP);
    Rcpp::traits::input_parameter< std::string >::type project(projectSEXP);
    Rcpp::traits::input_parameter< std::string >::type dataset(datasetSEXP);
    Rcpp::traits::input_parameter< std::string >::type table(tableSEXP);
    Rcpp::traits::input_parameter< std::string >::type parent(parentSEXP);
    Rcpp::traits::input_parameter< std::int64_t >::type n(nSEXP);
    Rcpp::traits::input_parameter< std::vector<std::string> >::type selected_fields(selected_fieldsSEXP);
    Rcpp::traits::input_parameter< std::string >::type row_restriction(row_restrictionSEXP);
    Rcpp::traits::input_parameter< std::double_t >::type sample_percentage(sample_percentageSEXP);
    Rcpp::traits::input_parameter< std::int64_t >::type timestamp_seconds(timestamp_secondsSEXP);
    Rcpp::traits::input_parameter< std::int32_t >::type timestamp_nanos(timestamp_nanosSEXP);
    Rcpp::traits::input_parameter< bool >::type quiet(quietSEXP);
    rcpp_result_gen = Rcpp::wrap(bqs_ipc_stream(client, project, dataset, table, parent, n, selected_fields, row_restriction, sample_percentage, timestamp_seconds, timestamp_nanos, quiet));
    return rcpp_result_gen;
END_RCPP
}

static const R_CallMethodDef CallEntries[] = {
    {"_bigrquerystorage_bqs_set_log_verbosity", (DL_FUNC) &_bigrquerystorage_bqs_set_log_verbosity, 1},
    {"_bigrquerystorage_bqs_init_logger", (DL_FUNC) &_bigrquerystorage_bqs_init_logger, 0},
    {"_bigrquerystorage_grpc_version", (DL_FUNC) &_bigrquerystorage_grpc_version, 0},
    {"_bigrquerystorage_bqs_client", (DL_FUNC) &_bigrquerystorage_bqs_client, 6},
    {"_bigrquerystorage_bqs_ipc_stream", (DL_FUNC) &_bigrquerystorage_bqs_ipc_stream, 12},
    {NULL, NULL, 0}
};

RcppExport void R_init_bigrquerystorage(DllInfo *dll) {
    R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
    R_useDynamicSymbols(dll, FALSE);
}
