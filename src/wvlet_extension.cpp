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
        
        const char* sql_result = wvlet_compile_query(json.c_str());

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

static unique_ptr<FunctionData> WvletBind(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<WvletBindData>();
    result->query = input.inputs[0].GetValue<string>();
    
    std::string json = "[\"-q\", \"" + result->query + "\"]";
    
    wvlet_compile_main(json.c_str());
    const char* sql_result = wvlet_compile_query(json.c_str());

    if (!sql_result || strlen(sql_result) == 0) {
        throw std::runtime_error("Failed to compile wvlet script");
    }
    
    result->query = std::string(sql_result);

    // Create a temporary connection to execute the query and get the schema
    Connection conn(*context.db);
    auto result_set = conn.Query(result->query);

    if (result_set->HasError()) {
        throw std::runtime_error(result_set->GetError());
    }

    // Get the types and names of the columns from the result set
    for (auto &column : result_set->types) {
        return_types.push_back(column);
    }
    for (auto &name : result_set->names) {
        names.push_back(name);
    }

    return std::move(result);
}

static void WvletFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<WvletBindData>();
    
    if (!bind_data.query_result) {
        throw std::runtime_error("query_result is nullptr");
    }
    
    if (!bind_data.query_result->initialized) {
        
        try {
            Connection conn(*context.db);
            
            auto result = conn.Query(bind_data.query);
            
            if (result->HasError()) {
                throw std::runtime_error(result->GetError());
            }
            
            bind_data.query_result->result = std::move(result);
            bind_data.query_result->initialized = true;
            
            auto &types = bind_data.query_result->result->types;
            
            output.Destroy();  // Clean up the existing chunk
            output.Initialize(context, types);  // Initialize with actual types
        } catch (const std::exception &e) {
            throw;
        }
    }
    
    auto chunk = bind_data.query_result->result->Fetch();
    
    if (!chunk || chunk->size() == 0) {
        output.SetCardinality(0);
        return;
    }
    
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
