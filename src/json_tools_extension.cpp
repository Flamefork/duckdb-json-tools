#define DUCKDB_EXTENSION_MAIN

#include "json_tools_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "../duckdb/extension/json/include/json_common.hpp"
#include "yyjson.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#if JSON_TOOLS_EXTENSION_HAS_LOADER
#include <duckdb/main/extension/extension_loader.hpp>
#else
#include <duckdb/main/extension_util.hpp>
#endif

#include <algorithm>
#include <cstdlib>
#include <limits>
#include <memory>
#include <string>

namespace duckdb {

namespace {

#if JSON_TOOLS_EXTENSION_HAS_LOADER
using JsonToolsLoadContext = ExtensionLoader;
#else
using JsonToolsLoadContext = DuckDB;
#endif

#if JSON_TOOLS_EXTENSION_HAS_LOADER
static void RegisterScalarFunction(JsonToolsLoadContext &ctx, const ScalarFunction &function) {
	ctx.RegisterFunction(function);
}
#else
static void RegisterScalarFunction(JsonToolsLoadContext &ctx, const ScalarFunction &function) {
	ExtensionUtil::RegisterFunction(*ctx.instance, function);
}
#endif

struct JsonFlattenLocalState : public FunctionLocalState {
	explicit JsonFlattenLocalState(Allocator &allocator) : json_allocator(std::make_shared<JSONAllocator>(allocator)) {
	}

	shared_ptr<JSONAllocator> json_allocator;
	std::string key_buffer;
};

static unique_ptr<FunctionLocalState> JsonFlattenInitLocalState(ExpressionState &state, const BoundFunctionExpression &,
                                                                FunctionData *) {
	auto &context = state.GetContext();
	return make_uniq<JsonFlattenLocalState>(BufferAllocator::Get(context));
}

struct JsonAddPrefixLocalState : public FunctionLocalState {
	explicit JsonAddPrefixLocalState(Allocator &allocator)
	    : json_allocator(std::make_shared<JSONAllocator>(allocator)) {
	}

	shared_ptr<JSONAllocator> json_allocator;
};

static unique_ptr<FunctionLocalState> JsonAddPrefixInitLocalState(ExpressionState &state,
                                                                  const BoundFunctionExpression &, FunctionData *) {
	auto &context = state.GetContext();
	return make_uniq<JsonAddPrefixLocalState>(BufferAllocator::Get(context));
}

using duckdb_yyjson::yyjson_arr_iter;
using duckdb_yyjson::yyjson_arr_iter_next;
using duckdb_yyjson::yyjson_arr_iter_with;
using duckdb_yyjson::yyjson_doc;
using duckdb_yyjson::yyjson_doc_free;
using duckdb_yyjson::yyjson_doc_get_root;
using duckdb_yyjson::yyjson_is_arr;
using duckdb_yyjson::yyjson_is_null;
using duckdb_yyjson::yyjson_is_obj;
using duckdb_yyjson::yyjson_mut_doc;
using duckdb_yyjson::yyjson_mut_doc_free;
using duckdb_yyjson::yyjson_mut_doc_new;
using duckdb_yyjson::yyjson_mut_doc_set_root;
using duckdb_yyjson::yyjson_mut_obj;
using duckdb_yyjson::yyjson_mut_obj_add_val;
using duckdb_yyjson::yyjson_mut_strncpy;
using duckdb_yyjson::yyjson_mut_val;
using duckdb_yyjson::yyjson_mut_write_opts;
using duckdb_yyjson::yyjson_obj_iter;
using duckdb_yyjson::yyjson_obj_iter_get_val;
using duckdb_yyjson::yyjson_obj_iter_next;
using duckdb_yyjson::yyjson_obj_iter_with;
using duckdb_yyjson::yyjson_read_opts;
using duckdb_yyjson::yyjson_val;
using duckdb_yyjson::yyjson_val_mut_copy;

// Maximum nesting depth to prevent stack exhaustion from pathological inputs
constexpr idx_t MAX_JSON_NESTING_DEPTH = 1000;

// Default initial capacity for the key buffer
constexpr idx_t DEFAULT_KEY_BUFFER_SIZE = 512;

// Append digits of the array index without creating temporary strings.
static void AppendIndexToKeyBuffer(std::string &key_buffer, idx_t index) {
	char digits[std::numeric_limits<idx_t>::digits10 + 2];
	idx_t value = index;
	idx_t pos = 0;
	do {
		digits[pos++] = static_cast<char>('0' + (value % 10));
		value /= 10;
	} while (value > 0);
	auto old_size = key_buffer.size();
	key_buffer.resize(old_size + pos);
	for (idx_t i = 0; i < pos; i++) {
		key_buffer[old_size + i] = digits[pos - 1 - i];
	}
}

// Depth-first traversal that materializes dotted key paths for leaf values.
static void FlattenIntoObject(yyjson_val *node, yyjson_mut_doc *out_doc, yyjson_mut_val *out_obj,
                              std::string &key_buffer, idx_t depth = 0) {
	if (depth > MAX_JSON_NESTING_DEPTH) {
		throw InvalidInputException("json_flatten: nesting depth exceeds maximum limit of " +
		                            std::to_string(MAX_JSON_NESTING_DEPTH));
	}
	if (yyjson_is_obj(node)) {
		yyjson_val *key;
		yyjson_obj_iter iter = yyjson_obj_iter_with(node);
		while ((key = yyjson_obj_iter_next(&iter))) {
			auto child = yyjson_obj_iter_get_val(key);
			auto previous_size = key_buffer.size();
			if (previous_size != 0) {
				key_buffer.push_back('.');
			}
			auto key_str = duckdb_yyjson::yyjson_get_str(key);
			auto key_len = duckdb_yyjson::yyjson_get_len(key);
			key_buffer.append(key_str, key_len);
			FlattenIntoObject(child, out_doc, out_obj, key_buffer, depth + 1);
			key_buffer.resize(previous_size);
		}
	} else if (yyjson_is_arr(node)) {
		yyjson_val *child;
		yyjson_arr_iter iter = yyjson_arr_iter_with(node);
		idx_t index = 0;
		while ((child = yyjson_arr_iter_next(&iter))) {
			auto previous_size = key_buffer.size();
			if (previous_size != 0) {
				key_buffer.push_back('.');
			}
			AppendIndexToKeyBuffer(key_buffer, index);
			FlattenIntoObject(child, out_doc, out_obj, key_buffer, depth + 1);
			key_buffer.resize(previous_size);
			index++;
		}
	} else {
		// Preserve empty keys (e.g., {"": 1}) by always serializing the current path.
		auto key_val = yyjson_mut_strncpy(out_doc, key_buffer.c_str(), key_buffer.size());
		if (!key_val) {
			throw InternalException("json_flatten: failed to allocate key storage");
		}
		auto key_ptr = duckdb_yyjson::yyjson_mut_get_str(key_val);
		auto value_copy = yyjson_val_mut_copy(out_doc, node);
		if (!value_copy) {
			throw InternalException("json_flatten: failed to allocate value storage");
		}
		if (!yyjson_mut_obj_add_val(out_doc, out_obj, key_ptr, value_copy)) {
			throw InternalException("json_flatten: failed to append flattened value");
		}
	}
}

// Parse the input JSON, flatten into a new document, and return the serialized payload.
inline string_t JsonFlattenSingle(Vector &result, const string_t &input, JsonFlattenLocalState &local_state) {
	auto &allocator = *local_state.json_allocator;
	allocator.Reset();
	auto alc = allocator.GetYYAlc();
	auto input_data = input.GetDataUnsafe();
	auto input_length = input.GetSize();
	duckdb_yyjson::yyjson_read_err err;
	auto doc =
	    yyjson_read_opts(const_cast<char *>(input_data), input_length, duckdb_yyjson::YYJSON_READ_NOFLAG, alc, &err);
	if (!doc) {
		throw InvalidInputException(StringUtil::Format("json_flatten: invalid JSON at position %llu: %s",
		                                               static_cast<unsigned long long>(err.pos),
		                                               err.msg ? err.msg : "unknown error"));
	}
	std::unique_ptr<yyjson_doc, decltype(&yyjson_doc_free)> doc_handle(doc, yyjson_doc_free);
	auto root = yyjson_doc_get_root(doc);
	if (!root || yyjson_is_null(root) || (!yyjson_is_obj(root) && !yyjson_is_arr(root))) {
		return StringVector::AddString(result, input);
	}
	auto out_doc = yyjson_mut_doc_new(alc);
	if (!out_doc) {
		throw InternalException("json_flatten: failed to allocate output document");
	}
	std::unique_ptr<yyjson_mut_doc, decltype(&yyjson_mut_doc_free)> out_handle(out_doc, yyjson_mut_doc_free);
	auto out_root = yyjson_mut_obj(out_doc);
	if (!out_root) {
		throw InternalException("json_flatten: failed to allocate output object");
	}
	yyjson_mut_doc_set_root(out_doc, out_root);
	auto &key_buffer = local_state.key_buffer;
	key_buffer.clear();
	key_buffer.reserve(static_cast<size_t>(std::min<idx_t>(input_length, DEFAULT_KEY_BUFFER_SIZE)));
	FlattenIntoObject(root, out_doc, out_root, key_buffer, 0);
	size_t output_length = 0;
	auto output_cstr =
	    yyjson_mut_write_opts(out_doc, duckdb_yyjson::YYJSON_WRITE_NOFLAG, nullptr, &output_length, nullptr);
	if (!output_cstr) {
		throw InternalException("json_flatten: failed to serialize flattened JSON");
	}
	std::unique_ptr<char, decltype(&free)> output_handle(output_cstr, free);
	return StringVector::AddString(result, output_cstr, output_length);
}

// Add prefix to all top-level keys in a JSON object.
inline string_t JsonAddPrefixSingle(Vector &result, const string_t &input, const string_t &prefix,
                                    JSONAllocator &allocator) {
	allocator.Reset();
	auto alc = allocator.GetYYAlc();
	auto input_data = input.GetDataUnsafe();
	auto input_length = input.GetSize();
	duckdb_yyjson::yyjson_read_err err;
	auto doc =
	    yyjson_read_opts(const_cast<char *>(input_data), input_length, duckdb_yyjson::YYJSON_READ_NOFLAG, alc, &err);
	if (!doc) {
		throw InvalidInputException(StringUtil::Format("json_add_prefix: invalid JSON at position %llu: %s",
		                                               static_cast<unsigned long long>(err.pos),
		                                               err.msg ? err.msg : "unknown error"));
	}

	std::unique_ptr<yyjson_doc, decltype(&yyjson_doc_free)> doc_handle(doc, yyjson_doc_free);
	auto root = yyjson_doc_get_root(doc);
	if (!root || !yyjson_is_obj(root)) {
		throw InvalidInputException("json_add_prefix: expected JSON object input");
	}

	auto out_doc = yyjson_mut_doc_new(alc);
	if (!out_doc) {
		throw InternalException("json_add_prefix: failed to allocate output document");
	}
	std::unique_ptr<yyjson_mut_doc, decltype(&yyjson_mut_doc_free)> out_handle(out_doc, yyjson_mut_doc_free);
	auto out_root = yyjson_mut_obj(out_doc);
	if (!out_root) {
		throw InternalException("json_add_prefix: failed to allocate output object");
	}
	yyjson_mut_doc_set_root(out_doc, out_root);

	auto prefix_data = prefix.GetDataUnsafe();
	auto prefix_length = prefix.GetSize();

	yyjson_val *key;
	yyjson_obj_iter iter = yyjson_obj_iter_with(root);
	while ((key = yyjson_obj_iter_next(&iter))) {
		auto value = yyjson_obj_iter_get_val(key);
		auto key_str = duckdb_yyjson::yyjson_get_str(key);
		auto key_len = duckdb_yyjson::yyjson_get_len(key);

		// Construct prefixed key using stack buffer for common case
		char buffer[512];
		size_t prefixed_len = prefix_length + key_len;
		yyjson_mut_val *new_key_val;

		if (prefixed_len < sizeof(buffer)) {
			// Use stack buffer for common case
			memcpy(buffer, prefix_data, prefix_length);
			memcpy(buffer + prefix_length, key_str, key_len);
			new_key_val = yyjson_mut_strncpy(out_doc, buffer, prefixed_len);
		} else {
			// Fallback to heap for large keys
			std::string new_key;
			new_key.reserve(prefixed_len);
			new_key.append(prefix_data, prefix_length);
			new_key.append(key_str, key_len);
			new_key_val = yyjson_mut_strncpy(out_doc, new_key.c_str(), prefixed_len);
		}

		if (!new_key_val) {
			throw InternalException("json_add_prefix: failed to allocate key storage");
		}
		auto new_key_ptr = duckdb_yyjson::yyjson_mut_get_str(new_key_val);

		auto value_copy = yyjson_val_mut_copy(out_doc, value);
		if (!value_copy) {
			throw InternalException("json_add_prefix: failed to allocate value storage");
		}

		if (!yyjson_mut_obj_add_val(out_doc, out_root, new_key_ptr, value_copy)) {
			throw InternalException("json_add_prefix: failed to add prefixed key-value pair");
		}
	}

	size_t output_length = 0;
	auto output_cstr =
	    yyjson_mut_write_opts(out_doc, duckdb_yyjson::YYJSON_WRITE_NOFLAG, nullptr, &output_length, nullptr);
	if (!output_cstr) {
		throw InternalException("json_add_prefix: failed to serialize output JSON");
	}
	std::unique_ptr<char, decltype(&free)> output_handle(output_cstr, free);
	return StringVector::AddString(result, output_cstr, output_length);
}

} // namespace

inline void JsonFlattenScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	// Thread-safety: per-invocation state lives in JsonFlattenLocalState, so vectors can execute in parallel.
	auto state_ptr = ExecuteFunctionState::GetFunctionState(state);
	D_ASSERT(state_ptr);
	auto &local_state = state_ptr->Cast<JsonFlattenLocalState>();
	auto &input = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(input, result, args.size(), [&](const string_t &json_input) {
		return JsonFlattenSingle(result, json_input, local_state);
	});
}

inline void JsonAddPrefixScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto state_ptr = ExecuteFunctionState::GetFunctionState(state);
	D_ASSERT(state_ptr);
	auto &local_state = state_ptr->Cast<JsonAddPrefixLocalState>();
	auto &json_input = args.data[0];
	auto &prefix_input = args.data[1];

	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    json_input, prefix_input, result, args.size(), [&](const string_t &json, const string_t &prefix) {
		    return JsonAddPrefixSingle(result, json, prefix, *local_state.json_allocator);
	    });
}

static void LoadInternal(JsonToolsLoadContext &ctx) {
	auto json_flatten_scalar_function =
	    ScalarFunction("json_flatten", {LogicalType::JSON()}, LogicalType::JSON(), JsonFlattenScalarFun, nullptr,
	                   nullptr, nullptr, JsonFlattenInitLocalState);
	RegisterScalarFunction(ctx, json_flatten_scalar_function);

	auto json_add_prefix_scalar_function =
	    ScalarFunction("json_add_prefix", {LogicalType::JSON(), LogicalType::VARCHAR}, LogicalType::JSON(),
	                   JsonAddPrefixScalarFun, nullptr, nullptr, nullptr, JsonAddPrefixInitLocalState);
	RegisterScalarFunction(ctx, json_add_prefix_scalar_function);
}

#if JSON_TOOLS_EXTENSION_HAS_LOADER
void JsonToolsExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
#else
void JsonToolsExtension::Load(DuckDB &db) {
	LoadInternal(db);
}
#endif
std::string JsonToolsExtension::Name() {
	return "json_tools";
}

std::string JsonToolsExtension::Version() const {
#ifdef EXT_VERSION_JSON_TOOLS
	return EXT_VERSION_JSON_TOOLS;
#else
	return "";
#endif
}

} // namespace duckdb

#if JSON_TOOLS_EXTENSION_HAS_LOADER
extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(json_tools, loader) {
	duckdb::LoadInternal(loader);
}
}
#else
extern "C" {

DUCKDB_EXTENSION_API void json_tools_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::JsonToolsExtension>();
}

DUCKDB_EXTENSION_API const char *json_tools_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}
#endif
