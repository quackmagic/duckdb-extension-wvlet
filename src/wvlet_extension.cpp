#define DUCKDB_EXTENSION_MAIN
#include "wvlet_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>
#include <fstream>
#include <sstream>

#include <iostream>
#include <cstdio>
#include <stdexcept>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

extern "C" {
    int wvlet_compile_main(const char*);
}

struct WvletBindData : public TableFunctionData {
    string query;
    bool has_returned = false;
};

static unique_ptr<FunctionData> WvletBind(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names) {
    // Get all the lineitem columns here
    auto result = make_uniq<WvletBindData>();
    result->query = input.inputs[0].GetValue<string>();
    
    // TODO: We should probably get these from the schema of the target table
    return_types = {LogicalType::INTEGER, LogicalType::VARCHAR}; // Example columns
    names = {"id", "name"}; // Example column names
    
    return std::move(result);
}

static void WvletFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = (WvletBindData &)*data_p.bind_data;
    
    if (bind_data.has_returned) {
        output.SetCardinality(0);
        return;
    }

    std::ostringstream captured_output;
    FILE* original_stdout = stdout;
    stdout = fdopen(fileno(stdout), "w");
    std::ostringstream captured_error;
    FILE* original_stderr = stderr;
    stderr = fdopen(fileno(stderr), "w");

    // Convert script to JSON array format as expected by wvlet_compile_main
    std::string json = "[\"-x\", \"-q\", \"" + bind_data.query + "\"]";

    // Call wvlet compiler - it will print the SQL
    int compile_result = wvlet_compile_main(json.c_str());

    if (compile_result != 0) {
        throw std::runtime_error("Failed to compile wvlet script");
    }

    std::string query = captured_output.str();
    std::cout << "Captured Output: " << query << std::endl;

    stdout = original_stdout;
    stderr = original_stderr;

    // The SQL has been printed, now we can execute it
    // TODO: Execute the printed SQL and fill the output chunk with results
    
    bind_data.has_returned = true;
}

static void LoadInternal(DatabaseInstance &instance) {
    TableFunction wvlet_func("wvlet", {LogicalType::VARCHAR},
                           WvletFunction, WvletBind);
    ExtensionUtil::RegisterFunction(instance, wvlet_func);
}

void WvletExtension::Load(DuckDB &db) {
    LoadInternal(*db.instance);
}

std::string WvletExtension::Name() {
    return "wvlet";
}

std::string WvletExtension::Version() const {
#ifdef EXT_VERSION_WVLET
    return EXT_VERSION_WVLET;
#else
    return "";
#endif
}

} // namespace duckdb

extern "C" {
DUCKDB_EXTENSION_API void wvlet_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::WvletExtension>();
}

DUCKDB_EXTENSION_API const char *wvlet_version() {
    return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
