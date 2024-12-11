#define DUCKDB_EXTENSION_MAIN

#include "wvlet_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/extension_statement.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <codecvt>
#include <string>

#ifdef __cplusplus
extern "C" {
#endif
    extern int ScalaNativeInit(void);
    extern int wvlet_compile_main(const char*);
    extern const char* wvlet_compile_query(const char* json_query);
#ifdef __cplusplus
}
#endif


namespace duckdb {

// EXPERIMENT INIT
bool InitializeWvletRuntime() {
    try {
        // Set heap sizes via environment variables
        setenv("GC_INITIAL_HEAP_SIZE", "2097152", 1);  // 64MB
        setenv("GC_MAXIMUM_HEAP_SIZE", "8388608", 1); // 256MB
        
        // fprintf(stderr, "Initializing Scala Native Runtime...\n");
        int init_result = ScalaNativeInit();
        if (init_result != 0) {
            fprintf(stderr, "Failed to initialize Scala Native Runtime: %d\n", init_result);
            return false;
        }
        
        // fprintf(stderr, "Scala Native Runtime initialized successfully!\n");
        return true;
    } catch (...) {
        fprintf(stderr, "Scala Runtime Initialization failed with exception!\n");
        return false;
    }
}

static void LoadInternal(DatabaseInstance &instance) {
  auto &config = DBConfig::GetConfig(instance);
  // Register the custom Wvlet parser extension
  WvletParserExtension wvlet_parser;
  config.parser_extensions.push_back(wvlet_parser);
  // No operator extensions added for now
}

void WvletExtension::Load(DuckDB &db) {
  LoadInternal(*db.instance);
  if (!InitializeWvletRuntime()) {
        throw std::runtime_error("Failed to initialize Wvlet runtime");
  }
}

ParserExtensionParseResult wvlet_parse(ParserExtensionInfo *, const std::string &query) {
  // Directly pass through the query with no transformation
  auto sql_query = query;

  std::string json = "[\"-q\", \"" + query + "\"]";
  std::cout << "in: " << json << "\n";
  wvlet_compile_main(json.c_str());
  std::cout << "in2: " << json.c_str() << "\n";
  const char* sql_result = wvlet_compile_query(json.c_str());
  std::cout << "out: " << sql_result << "\n";
  if (!sql_result || strlen(sql_result) == 0) {
    throw std::runtime_error("Failed to compile wvlet script");
  }

  Parser parser; // Parse the SQL query
  parser.ParseQuery(sql_query);
  auto statements = std::move(parser.statements);

  return ParserExtensionParseResult(
      make_uniq_base<ParserExtensionParseData, WvletParseData>(
          std::move(statements[0])));
}

ParserExtensionPlanResult wvlet_plan(ParserExtensionInfo *, ClientContext &context,
                                     unique_ptr<ParserExtensionParseData> parse_data) {
  // Placeholder plan result
  return ParserExtensionPlanResult();
}

BoundStatement wvlet_bind(ClientContext &context, Binder &binder,
                           OperatorExtensionInfo *info, SQLStatement &statement) {
  // Directly return a no-op bound statement
  return {};
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void wvlet_init(duckdb::DatabaseInstance &db) {
  LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *wvlet_version() {
  return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
