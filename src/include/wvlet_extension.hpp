#pragma once

#include "duckdb.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

class WvletExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
        std::string Version() const override;
};

struct WvletScriptFunction {
    static void ParseWvletScript(DataChunk &args, ExpressionState &state, Vector &result);
    static unique_ptr<FunctionData> Bind(ClientContext &context, ScalarFunction &bound_function,
                                       vector<unique_ptr<Expression>> &arguments);
};

} // namespace duckdb
