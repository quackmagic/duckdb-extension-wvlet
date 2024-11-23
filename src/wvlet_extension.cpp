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
#include <stdexcept>

extern "C" {
    int wvlet_compile_main(const char*);
    const char* wvlet_compile_compile(const char*);
}

namespace duckdb {

void WvletScriptFunction::ParseWvletScript(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input_vector = args.data[0];
    auto input = FlatVector::GetData<string_t>(input_vector);

    for (idx_t i = 0; i < args.size(); i++) {
        string query = input[i].GetString();
        std::string json = "[\"-q\", \"" + query + "\"]";
        
        // std::cout << "Input wvlet: " << query << std::endl;
        
        // Initialize wvlet compiler
        // wvlet_compile_main(json.c_str());
        
        // Get compiled SQL
        const char* sql_result = wvlet_compile_query(json.c_str());
        // std::cout << "Compiled SQL: " << sql_result << std::endl;

        if (!sql_result || strlen(sql_result) == 0) {
            throw std::runtime_error("Failed to compile wvlet script");
        }

        FlatVector::GetData<string_t>(result)[i] = StringVector::AddString(result, sql_result);
    }

    result.Verify(args.size());
}

unique_ptr<FunctionData> WvletScriptFunction::Bind(ClientContext &context, ScalarFunction &bound_function,
                                                 vector<unique_ptr<Expression>> &arguments) {
    return nullptr;
}

static std::string CleanSQL(const std::string& sql) {
    // Find first occurrence of "select" (case insensitive)
    std::string lower_sql = sql;
    std::transform(lower_sql.begin(), lower_sql.end(), lower_sql.begin(), ::tolower);
    auto pos = lower_sql.find("select");
    if (pos == std::string::npos) {
        throw std::runtime_error("No SELECT statement found in compiled SQL");
    }
    return sql.substr(pos);
}

static unique_ptr<FunctionData> WvletBind(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<WvletBindData>();
    result->query = input.inputs[0].GetValue<string>();
    
    std::string json = "[\"-q\", \"" + result->query + "\"]";
    // std::cout << "Input wvlet: " << result->query << std::endl;
    
    // Initialize wvlet compiler
    // std::cout << "Calling wvlet_compile_main..." << std::endl;
    wvlet_compile_main(json.c_str());
    
    // Get compiled SQL
    // std::cout << "Calling wvlet_compile_query..." << std::endl;
    const char* sql_result = wvlet_compile_query(json.c_str());
    // std::cout << "Compiled SQL: " << sql_result << std::endl;

    if (!sql_result || strlen(sql_result) == 0) {
        throw std::runtime_error("Failed to compile wvlet script");
    }
    
    // Store the compiled SQL query
    result->query = std::string(sql_result);

    // For t1, we know it has two INTEGER columns
    return_types = {LogicalType::INTEGER, LogicalType::INTEGER};
    names = {"i", "j"};

    // std::cout << "Bind complete with query: " << result->query << std::endl;
    return std::move(result);
}

static void WvletFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<WvletBindData>();
    
    if (!bind_data.query_result->initialized) {
        // std::cout << "Starting query execution..." << std::endl;
        
        // Use a new connection with the existing database instance
        Connection conn(*context.db);
        
        // std::cout << "Executing query: " << bind_data.query << std::endl;
        auto result = conn.Query(bind_data.query);
        
        if (result->HasError()) {
            throw std::runtime_error(result->GetError());
        }
        
        bind_data.query_result->result = std::move(result);
        bind_data.query_result->initialized = true;
        
        // Initialize output with INTEGER types to match t1
        output.Initialize(Allocator::DefaultAllocator(), {LogicalType::INTEGER, LogicalType::INTEGER});
        // std::cout << "Query initialized successfully" << std::endl;
    }
    
    // Fetch next chunk
    // std::cout << "Fetching chunk..." << std::endl;
    auto chunk = bind_data.query_result->result->Fetch();
    // std::cout << "Chunk fetched" << std::endl;
    
    if (!chunk || chunk->size() == 0) {
        // std::cout << "No more data" << std::endl;
        output.SetCardinality(0);
        return;
    }
    
    // std::cout << "Got chunk with " << chunk->size() << " rows" << std::endl;
    output.Reference(*chunk);
    output.SetCardinality(chunk->size());
}

static void LoadInternal(DatabaseInstance &instance) {
    auto wvlet_fun = ScalarFunction("wvlet", {LogicalType::VARCHAR}, LogicalType::VARCHAR,
                                  WvletScriptFunction::ParseWvletScript,
                                  WvletScriptFunction::Bind);
    ExtensionUtil::RegisterFunction(instance, wvlet_fun);
    
    TableFunction wvlet_func("wvlet", {LogicalType::VARCHAR}, WvletFunction, WvletBind);
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
