// Generated by cpp11: do not edit by hand
// clang-format off


#include "cpp11/declarations.hpp"

// bqs.cpp
void bqs_init_logger();
extern "C" SEXP _bigrquerystorage_bqs_init_logger() {
  BEGIN_CPP11
    bqs_init_logger();
    return R_NilValue;
  END_CPP11
}
// bqs.cpp
void bqs_set_log_verbosity(int severity);
extern "C" SEXP _bigrquerystorage_bqs_set_log_verbosity(SEXP severity) {
  BEGIN_CPP11
    bqs_set_log_verbosity(cpp11::as_cpp<cpp11::decay_t<int>>(severity));
    return R_NilValue;
  END_CPP11
}
// bqs.cpp
std::string grpc_version();
extern "C" SEXP _bigrquerystorage_grpc_version() {
  BEGIN_CPP11
    return cpp11::as_sexp(grpc_version());
  END_CPP11
}
// bqs.cpp
cpp11::list bqs_ipc_stream(std::string project, std::string dataset, std::string table, std::string parent, std::string client_info, std::string service_configuration, std::string access_token, std::int64_t timestamp_seconds, std::int32_t timestamp_nanos, std::vector<std::string> selected_fields, std::string row_restriction);
extern "C" SEXP _bigrquerystorage_bqs_ipc_stream(SEXP project, SEXP dataset, SEXP table, SEXP parent, SEXP client_info, SEXP service_configuration, SEXP access_token, SEXP timestamp_seconds, SEXP timestamp_nanos, SEXP selected_fields, SEXP row_restriction) {
  BEGIN_CPP11
    return cpp11::as_sexp(bqs_ipc_stream(cpp11::as_cpp<cpp11::decay_t<std::string>>(project), cpp11::as_cpp<cpp11::decay_t<std::string>>(dataset), cpp11::as_cpp<cpp11::decay_t<std::string>>(table), cpp11::as_cpp<cpp11::decay_t<std::string>>(parent), cpp11::as_cpp<cpp11::decay_t<std::string>>(client_info), cpp11::as_cpp<cpp11::decay_t<std::string>>(service_configuration), cpp11::as_cpp<cpp11::decay_t<std::string>>(access_token), cpp11::as_cpp<cpp11::decay_t<std::int64_t>>(timestamp_seconds), cpp11::as_cpp<cpp11::decay_t<std::int32_t>>(timestamp_nanos), cpp11::as_cpp<cpp11::decay_t<std::vector<std::string>>>(selected_fields), cpp11::as_cpp<cpp11::decay_t<std::string>>(row_restriction)));
  END_CPP11
}

extern "C" {
/* .Call calls */
extern SEXP _bigrquerystorage_bqs_init_logger();
extern SEXP _bigrquerystorage_bqs_ipc_stream(SEXP, SEXP, SEXP, SEXP, SEXP, SEXP, SEXP, SEXP, SEXP, SEXP, SEXP);
extern SEXP _bigrquerystorage_bqs_set_log_verbosity(SEXP);
extern SEXP _bigrquerystorage_grpc_version();

static const R_CallMethodDef CallEntries[] = {
    {"_bigrquerystorage_bqs_init_logger",       (DL_FUNC) &_bigrquerystorage_bqs_init_logger,        0},
    {"_bigrquerystorage_bqs_ipc_stream",        (DL_FUNC) &_bigrquerystorage_bqs_ipc_stream,        11},
    {"_bigrquerystorage_bqs_set_log_verbosity", (DL_FUNC) &_bigrquerystorage_bqs_set_log_verbosity,  1},
    {"_bigrquerystorage_grpc_version",          (DL_FUNC) &_bigrquerystorage_grpc_version,           0},
    {NULL, NULL, 0}
};
}

extern "C" void R_init_bigrquerystorage(DllInfo* dll){
  R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
  R_useDynamicSymbols(dll, FALSE);
}
