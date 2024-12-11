#pragma once

#include "duckdb.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"

extern "C" {
    extern int ScalaNativeInit(void);
    extern int wvlet_compile_main(const char*);
    extern const char* wvlet_compile_query(const char* json_query);  // Changed from wvlet_compile_compile
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
  std::string Name() override { return "wvlet"; }
};

BoundStatement wvlet_bind(ClientContext &context, Binder &binder,
                          OperatorExtensionInfo *info, SQLStatement &statement);

struct WvletOperatorExtension : public OperatorExtension {
  WvletOperatorExtension() : OperatorExtension() { Bind = wvlet_bind; }

  std::string GetName() override { return "wvlet"; }

  unique_ptr<LogicalExtensionOperator>
  Deserialize(Deserializer &deserializer) override {
    throw InternalException("wvlet operator should not be serialized");
  }
};

ParserExtensionParseResult wvlet_parse(ParserExtensionInfo *,
                                       const std::string &query);

ParserExtensionPlanResult wvlet_plan(ParserExtensionInfo *, ClientContext &,
                                     unique_ptr<ParserExtensionParseData>);

struct WvletParserExtension : public ParserExtension {
  WvletParserExtension() : ParserExtension() {
    parse_function = wvlet_parse;
    plan_function = wvlet_plan;
  }
};

struct WvletParseData : ParserExtensionParseData {
  unique_ptr<SQLStatement> statement;

  unique_ptr<ParserExtensionParseData> Copy() const override {
    return make_uniq_base<ParserExtensionParseData, WvletParseData>(
        statement->Copy());
  }

  virtual string ToString() const override { return "WvletParseData"; }

  WvletParseData(unique_ptr<SQLStatement> statement)
      : statement(std::move(statement)) {}
};

class WvletState : public ClientContextState {
public:
  explicit WvletState(unique_ptr<ParserExtensionParseData> parse_data)
      : parse_data(std::move(parse_data)) {}

  void QueryEnd() override { parse_data.reset(); }

  unique_ptr<ParserExtensionParseData> parse_data;
};

} // namespace duckdb
