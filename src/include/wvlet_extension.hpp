#pragma once

#include "duckdb.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"

// Declare the external wvlet_compile_query function
extern "C" {
    int wvlet_compile_main(const char*);
    const char* wvlet_compile_query(const char* json_query);
}

namespace duckdb {

struct WvletQueryResult {
    unique_ptr<QueryResult> result;
    bool initialized;

    WvletQueryResult() : initialized(false) {}
};

struct WvletBindData : public TableFunctionData {
    string query;
    unique_ptr<WvletQueryResult> query_result;

    WvletBindData() : query_result(make_uniq<WvletQueryResult>()) {}
};

struct WvletScriptFunction {
    static void ParseWvletScript(DataChunk &args, ExpressionState &state, Vector &result);
    static unique_ptr<FunctionData> Bind(ClientContext &context, ScalarFunction &bound_function,
                                       vector<unique_ptr<Expression>> &arguments);
};

class WvletExtension : public Extension {
public:
    void Load(DuckDB &db) override;
    std::string Name() override;
    std::string Version() const override;
};

} // namespace duckdb
