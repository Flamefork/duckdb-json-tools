#define DUCKDB_EXTENSION_MAIN

#include "json_tools_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/scalar/regexp.hpp"
#include "re2/set.h"
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
#include <type_traits>
#include <unordered_map>
#include <unordered_set>

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

#if JSON_TOOLS_EXTENSION_HAS_LOADER
static void RegisterAggregateFunction(JsonToolsLoadContext &ctx, AggregateFunctionSet function) {
	ctx.RegisterFunction(std::move(function));
}
#else
static void RegisterAggregateFunction(JsonToolsLoadContext &ctx, AggregateFunctionSet function) {
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

struct JsonFlattenBindData : public FunctionData {
	explicit JsonFlattenBindData(string separator_p) : separator(std::move(separator_p)) {
	}

	string separator;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<JsonFlattenBindData>(separator);
	}

	bool Equals(const FunctionData &other_p) const override {
		const auto &other = other_p.Cast<JsonFlattenBindData>();
		return separator == other.separator;
	}
};

static const string &GetJsonFlattenSeparator(optional_ptr<FunctionData> bind_data) {
	static const string DEFAULT_SEPARATOR = ".";
	if (!bind_data) {
		return DEFAULT_SEPARATOR;
	}
	return bind_data->Cast<JsonFlattenBindData>().separator;
}

static idx_t UTF8CharacterCount(const string &input) {
	idx_t count = 0;
	for (auto c : input) {
		count += (static_cast<uint8_t>(c) & 0xc0) != 0x80;
	}
	return count;
}

static unique_ptr<FunctionData> JsonFlattenBind(ClientContext &context, ScalarFunction &function,
                                                vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() != 2) {
		throw BinderException("json_flatten expects a JSON argument plus a separator parameter");
	}
	auto &separator_arg = arguments[1];
	if (separator_arg->return_type.id() == LogicalTypeId::UNKNOWN || separator_arg->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!separator_arg->IsFoldable()) {
		throw BinderException("json_flatten separator argument must be constant");
	}
	auto separator_value = ExpressionExecutor::EvaluateScalar(context, *separator_arg);
	if (separator_value.IsNull() || separator_value.type().id() != LogicalTypeId::VARCHAR) {
		throw BinderException("json_flatten separator must be a VARCHAR literal of length 1");
	}
	auto separator = separator_value.ToString();
	if (UTF8CharacterCount(separator) != 1) {
		throw BinderException("json_flatten separator must be a VARCHAR literal of length 1");
	}
	Function::EraseArgument(function, arguments, 1);
	return make_uniq<JsonFlattenBindData>(std::move(separator));
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

// Hash map based JSON value representation for O(1) key operations
struct JsonValue;
using JsonArray = std::vector<JsonValue>;

enum class JsonValueType : uint8_t {
	UNINITIALIZED = 0,
	JSON_NULL,
	BOOL_VAL,
	INT64_VAL,
	UINT64_VAL,
	DOUBLE_VAL,
	STRING_VAL,
	ARRAY_VAL,
	OBJECT_VAL
};

// Forward declare JsonObject (defined after JsonValue)
class JsonObject;

struct JsonValue {
	JsonValueType type;
	union {
		bool bool_val;
		int64_t int64_val;
		uint64_t uint64_val;
		double double_val;
	} primitive;
	std::string string_val;
	unique_ptr<JsonArray> array_ptr;
	unique_ptr<JsonObject> object_ptr;

	JsonValue();
	~JsonValue();

	// Copy constructor - deep copy
	JsonValue(const JsonValue &other);

	// Move constructor
	JsonValue(JsonValue &&other) noexcept;

	// Copy assignment - deep copy
	JsonValue &operator=(const JsonValue &other);

	// Move assignment
	JsonValue &operator=(JsonValue &&other) noexcept;

	static JsonValue MakeNull();
	static JsonValue MakeBool(bool b);
	static JsonValue MakeInt64(int64_t n);
	static JsonValue MakeUint64(uint64_t n);
	static JsonValue MakeDouble(double d);
	static JsonValue MakeString(std::string s);
	static JsonValue MakeArray(JsonArray arr);
	static JsonValue MakeObject(JsonObject obj);

	bool IsNull() const { return type == JsonValueType::JSON_NULL; }
	bool IsObject() const { return type == JsonValueType::OBJECT_VAL; }
	bool IsArray() const { return type == JsonValueType::ARRAY_VAL; }
	bool IsString() const { return type == JsonValueType::STRING_VAL; }
	bool IsBool() const { return type == JsonValueType::BOOL_VAL; }
	bool IsInt64() const { return type == JsonValueType::INT64_VAL; }
	bool IsUint64() const { return type == JsonValueType::UINT64_VAL; }
	bool IsDouble() const { return type == JsonValueType::DOUBLE_VAL; }
	bool IsUninitialized() const { return type == JsonValueType::UNINITIALIZED; }

	JsonObject &AsObject();
	const JsonObject &AsObject() const;
	JsonArray &AsArray() { return *array_ptr; }
	const JsonArray &AsArray() const { return *array_ptr; }
	const std::string &AsString() const { return string_val; }
	bool AsBool() const { return primitive.bool_val; }
	int64_t AsInt64() const { return primitive.int64_val; }
	uint64_t AsUint64() const { return primitive.uint64_val; }
	double AsDouble() const { return primitive.double_val; }
};

// Insertion-order-preserving JSON object with O(1) key lookup
class JsonObject {
public:
	struct Entry {
		std::string key;
		JsonValue value;
		bool deleted;

		Entry(std::string k, JsonValue v) : key(std::move(k)), value(std::move(v)), deleted(false) {}
	};

	JsonObject() = default;
	~JsonObject() = default;

	JsonObject(const JsonObject &other) : entries_(other.entries_) {
		RebuildIndex();
	}

	JsonObject(JsonObject &&other) noexcept
	    : entries_(std::move(other.entries_)), index_(std::move(other.index_)) {
	}

	JsonObject &operator=(const JsonObject &other) {
		if (this != &other) {
			entries_ = other.entries_;
			RebuildIndex();
		}
		return *this;
	}

	JsonObject &operator=(JsonObject &&other) noexcept {
		if (this != &other) {
			entries_ = std::move(other.entries_);
			index_ = std::move(other.index_);
		}
		return *this;
	}

	// O(1) amortized lookup
	JsonValue *Find(const std::string &key) {
		auto it = index_.find(key);
		if (it == index_.end()) {
			return nullptr;
		}
		return &entries_[it->second].value;
	}

	const JsonValue *Find(const std::string &key) const {
		auto it = index_.find(key);
		if (it == index_.end()) {
			return nullptr;
		}
		return &entries_[it->second].value;
	}

	// O(1) amortized insert or update
	JsonValue &operator[](const std::string &key) {
		auto it = index_.find(key);
		if (it != index_.end()) {
			return entries_[it->second].value;
		}
		idx_t new_idx = entries_.size();
		entries_.emplace_back(key, JsonValue());
		index_[key] = new_idx;
		return entries_.back().value;
	}

	// O(1) erase (marks as deleted, doesn't shift)
	void Erase(const std::string &key) {
		auto it = index_.find(key);
		if (it != index_.end()) {
			idx_t idx = it->second;
			entries_[idx].deleted = true;
			index_.erase(it);
		}
	}

	void Clear() {
		entries_.clear();
		index_.clear();
	}

	bool Empty() const { return index_.empty(); }
	idx_t Size() const { return index_.size(); }

	// Iterate over non-deleted entries (preserves insertion order)
	class Iterator {
	public:
		Iterator(std::vector<Entry> *entries, idx_t pos) : entries_(entries), pos_(pos) {
			SkipDeleted();
		}

		bool operator!=(const Iterator &other) const { return pos_ != other.pos_; }

		Iterator &operator++() {
			++pos_;
			SkipDeleted();
			return *this;
		}

		std::pair<const std::string &, JsonValue &> operator*() const {
			return {(*entries_)[pos_].key, (*entries_)[pos_].value};
		}

	private:
		void SkipDeleted() {
			while (pos_ < entries_->size() && (*entries_)[pos_].deleted) {
				++pos_;
			}
		}

		std::vector<Entry> *entries_;
		idx_t pos_;
	};

	class ConstIterator {
	public:
		ConstIterator(const std::vector<Entry> *entries, idx_t pos) : entries_(entries), pos_(pos) {
			SkipDeleted();
		}

		bool operator!=(const ConstIterator &other) const { return pos_ != other.pos_; }

		ConstIterator &operator++() {
			++pos_;
			SkipDeleted();
			return *this;
		}

		std::pair<const std::string &, const JsonValue &> operator*() const {
			return {(*entries_)[pos_].key, (*entries_)[pos_].value};
		}

	private:
		void SkipDeleted() {
			while (pos_ < entries_->size() && (*entries_)[pos_].deleted) {
				++pos_;
			}
		}

		const std::vector<Entry> *entries_;
		idx_t pos_;
	};

	Iterator begin() { return Iterator(&entries_, 0); }
	Iterator end() { return Iterator(&entries_, entries_.size()); }
	ConstIterator begin() const { return ConstIterator(&entries_, 0); }
	ConstIterator end() const { return ConstIterator(&entries_, entries_.size()); }

private:
	void RebuildIndex() {
		index_.clear();
		for (idx_t i = 0; i < entries_.size(); ++i) {
			if (!entries_[i].deleted) {
				index_[entries_[i].key] = i;
			}
		}
	}

	std::vector<Entry> entries_;
	std::unordered_map<std::string, idx_t> index_;
};

// JsonValue method implementations (after JsonObject is defined)
inline JsonValue::JsonValue() : type(JsonValueType::UNINITIALIZED) { primitive.int64_val = 0; }
inline JsonValue::~JsonValue() = default;

inline JsonValue::JsonValue(const JsonValue &other)
    : type(other.type), primitive(other.primitive), string_val(other.string_val) {
	if (other.array_ptr) {
		array_ptr = make_uniq<JsonArray>(*other.array_ptr);
	}
	if (other.object_ptr) {
		object_ptr = make_uniq<JsonObject>(*other.object_ptr);
	}
}

inline JsonValue::JsonValue(JsonValue &&other) noexcept
    : type(other.type), primitive(other.primitive), string_val(std::move(other.string_val)),
      array_ptr(std::move(other.array_ptr)), object_ptr(std::move(other.object_ptr)) {
	other.type = JsonValueType::UNINITIALIZED;
}

inline JsonValue &JsonValue::operator=(const JsonValue &other) {
	if (this != &other) {
		type = other.type;
		primitive = other.primitive;
		string_val = other.string_val;
		if (other.array_ptr) {
			array_ptr = make_uniq<JsonArray>(*other.array_ptr);
		} else {
			array_ptr.reset();
		}
		if (other.object_ptr) {
			object_ptr = make_uniq<JsonObject>(*other.object_ptr);
		} else {
			object_ptr.reset();
		}
	}
	return *this;
}

inline JsonValue &JsonValue::operator=(JsonValue &&other) noexcept {
	if (this != &other) {
		type = other.type;
		primitive = other.primitive;
		string_val = std::move(other.string_val);
		array_ptr = std::move(other.array_ptr);
		object_ptr = std::move(other.object_ptr);
		other.type = JsonValueType::UNINITIALIZED;
	}
	return *this;
}

inline JsonValue JsonValue::MakeNull() {
	JsonValue v;
	v.type = JsonValueType::JSON_NULL;
	return v;
}

inline JsonValue JsonValue::MakeBool(bool b) {
	JsonValue v;
	v.type = JsonValueType::BOOL_VAL;
	v.primitive.bool_val = b;
	return v;
}

inline JsonValue JsonValue::MakeInt64(int64_t n) {
	JsonValue v;
	v.type = JsonValueType::INT64_VAL;
	v.primitive.int64_val = n;
	return v;
}

inline JsonValue JsonValue::MakeUint64(uint64_t n) {
	JsonValue v;
	v.type = JsonValueType::UINT64_VAL;
	v.primitive.uint64_val = n;
	return v;
}

inline JsonValue JsonValue::MakeDouble(double d) {
	JsonValue v;
	v.type = JsonValueType::DOUBLE_VAL;
	v.primitive.double_val = d;
	return v;
}

inline JsonValue JsonValue::MakeString(std::string s) {
	JsonValue v;
	v.type = JsonValueType::STRING_VAL;
	v.string_val = std::move(s);
	return v;
}

inline JsonValue JsonValue::MakeArray(JsonArray arr) {
	JsonValue v;
	v.type = JsonValueType::ARRAY_VAL;
	v.array_ptr = make_uniq<JsonArray>(std::move(arr));
	return v;
}

inline JsonValue JsonValue::MakeObject(JsonObject obj) {
	JsonValue v;
	v.type = JsonValueType::OBJECT_VAL;
	v.object_ptr = make_uniq<JsonObject>(std::move(obj));
	return v;
}

inline JsonObject &JsonValue::AsObject() { return *object_ptr; }
inline const JsonObject &JsonValue::AsObject() const { return *object_ptr; }

struct JsonGroupMergeState {
	JsonObject *result_map;
	JsonObject *patch_map;
	JsonValue *scalar_replacement;  // Set when a non-object patch replaces the entire result
	bool result_has_input;
	bool patch_has_input;
	bool patch_has_nulls;
};

enum class JsonNullTreatment : uint8_t { DELETE_NULLS = 0, IGNORE_NULLS = 1 };

struct JsonGroupMergeBindData : public FunctionData {
	explicit JsonGroupMergeBindData(JsonNullTreatment treatment_p) : treatment(treatment_p) {
	}

	JsonNullTreatment treatment;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<JsonGroupMergeBindData>(treatment);
	}

	bool Equals(const FunctionData &other_p) const override {
		const auto &other = other_p.Cast<JsonGroupMergeBindData>();
		return treatment == other.treatment;
	}
};

static JsonNullTreatment GetNullTreatment(optional_ptr<FunctionData> bind_data) {
	if (!bind_data) {
		return JsonNullTreatment::DELETE_NULLS;
	}
	return bind_data->Cast<JsonGroupMergeBindData>().treatment;
}

static unique_ptr<FunctionData> JsonGroupMergeBind(ClientContext &context, AggregateFunction &function,
                                                   vector<unique_ptr<Expression>> &arguments);

// Maximum nesting depth to prevent stack exhaustion from pathological inputs
constexpr idx_t MAX_JSON_NESTING_DEPTH = 1000;

static void JsonGroupMergeStateInit(JsonGroupMergeState &state) {
	state.result_map = new JsonObject();
	state.patch_map = nullptr;
	state.scalar_replacement = nullptr;
	state.result_has_input = false;
	state.patch_has_input = false;
	state.patch_has_nulls = false;
}

static void JsonGroupMergeStateDestroy(JsonGroupMergeState &state) {
	if (state.result_map) {
		delete state.result_map;
		state.result_map = nullptr;
	}
	if (state.patch_map) {
		delete state.patch_map;
		state.patch_map = nullptr;
	}
	if (state.scalar_replacement) {
		delete state.scalar_replacement;
		state.scalar_replacement = nullptr;
	}
	state.result_has_input = false;
	state.patch_has_input = false;
	state.patch_has_nulls = false;
}

// Convert read-only yyjson_val to JsonValue
static JsonValue ParseYyjsonValue(yyjson_val *val, idx_t depth) {
	if (!val) {
		return JsonValue();
	}
	if (depth > MAX_JSON_NESTING_DEPTH) {
		throw InvalidInputException("json_group_merge: nesting depth exceeds maximum limit of " +
		                            std::to_string(MAX_JSON_NESTING_DEPTH));
	}

	auto tag = duckdb_yyjson::yyjson_get_tag(val);
	switch (tag) {
	case YYJSON_TYPE_NULL | YYJSON_SUBTYPE_NONE:
		return JsonValue::MakeNull();
	case YYJSON_TYPE_BOOL | YYJSON_SUBTYPE_TRUE:
		return JsonValue::MakeBool(true);
	case YYJSON_TYPE_BOOL | YYJSON_SUBTYPE_FALSE:
		return JsonValue::MakeBool(false);
	case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_UINT:
		return JsonValue::MakeUint64(duckdb_yyjson::yyjson_get_uint(val));
	case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_SINT:
		return JsonValue::MakeInt64(duckdb_yyjson::yyjson_get_sint(val));
	case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_REAL:
		return JsonValue::MakeDouble(duckdb_yyjson::yyjson_get_real(val));
	case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NONE:
	case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NOESC: {
		auto str = duckdb_yyjson::yyjson_get_str(val);
		auto len = duckdb_yyjson::yyjson_get_len(val);
		return JsonValue::MakeString(std::string(str, len));
	}
	case YYJSON_TYPE_ARR | YYJSON_SUBTYPE_NONE: {
		JsonArray arr;
		yyjson_val *elem;
		duckdb_yyjson::yyjson_arr_iter iter = duckdb_yyjson::yyjson_arr_iter_with(val);
		while ((elem = duckdb_yyjson::yyjson_arr_iter_next(&iter))) {
			arr.push_back(ParseYyjsonValue(elem, depth + 1));
		}
		return JsonValue::MakeArray(std::move(arr));
	}
	case YYJSON_TYPE_OBJ | YYJSON_SUBTYPE_NONE: {
		JsonObject obj;
		yyjson_val *key;
		duckdb_yyjson::yyjson_obj_iter iter = duckdb_yyjson::yyjson_obj_iter_with(val);
		while ((key = duckdb_yyjson::yyjson_obj_iter_next(&iter))) {
			auto key_str = duckdb_yyjson::yyjson_get_str(key);
			auto key_len = duckdb_yyjson::yyjson_get_len(key);
			auto child_val = duckdb_yyjson::yyjson_obj_iter_get_val(key);
			obj[std::string(key_str, key_len)] = ParseYyjsonValue(child_val, depth + 1);
		}
		return JsonValue::MakeObject(std::move(obj));
	}
	default:
		throw InternalException("json_group_merge: unknown yyjson type tag");
	}
}

// Forward declaration for recursive call
static yyjson_mut_val *BuildYyjsonValue(yyjson_mut_doc *doc, const JsonValue &val);

static yyjson_mut_val *BuildYyjsonValue(yyjson_mut_doc *doc, const JsonValue &val) {
	if (val.IsUninitialized()) {
		return nullptr;
	}
	if (val.IsNull()) {
		return duckdb_yyjson::yyjson_mut_null(doc);
	}
	if (val.IsBool()) {
		return duckdb_yyjson::yyjson_mut_bool(doc, val.AsBool());
	}
	if (val.IsInt64()) {
		return duckdb_yyjson::yyjson_mut_sint(doc, val.AsInt64());
	}
	if (val.IsUint64()) {
		return duckdb_yyjson::yyjson_mut_uint(doc, val.AsUint64());
	}
	if (val.IsDouble()) {
		return duckdb_yyjson::yyjson_mut_real(doc, val.AsDouble());
	}
	if (val.IsString()) {
		const auto &s = val.AsString();
		return duckdb_yyjson::yyjson_mut_strncpy(doc, s.c_str(), s.size());
	}
	if (val.IsArray()) {
		auto arr = duckdb_yyjson::yyjson_mut_arr(doc);
		if (!arr) {
			throw InternalException("json_group_merge: failed to allocate JSON array");
		}
		for (const auto &elem : val.AsArray()) {
			auto elem_val = BuildYyjsonValue(doc, elem);
			if (elem_val && !duckdb_yyjson::yyjson_mut_arr_append(arr, elem_val)) {
				throw InternalException("json_group_merge: failed to append array element");
			}
		}
		return arr;
	}
	if (val.IsObject()) {
		auto obj = duckdb_yyjson::yyjson_mut_obj(doc);
		if (!obj) {
			throw InternalException("json_group_merge: failed to allocate JSON object");
		}
		for (const auto &kv : val.AsObject()) {
			auto key = duckdb_yyjson::yyjson_mut_strncpy(doc, kv.first.c_str(), kv.first.size());
			auto child = BuildYyjsonValue(doc, kv.second);
			if (key && child && !duckdb_yyjson::yyjson_mut_obj_add(obj, key, child)) {
				throw InternalException("json_group_merge: failed to add object member");
			}
		}
		return obj;
	}
	throw InternalException("json_group_merge: unexpected JsonValue variant");
}

// Apply patch to target map (result_map) - implements JSON merge patch semantics
static void ApplyPatchToMap(JsonObject &target, yyjson_val *patch, idx_t depth,
                            JsonNullTreatment null_treatment, bool *saw_nulls) {
	if (!patch) {
		return;
	}
	if (depth > MAX_JSON_NESTING_DEPTH) {
		throw InvalidInputException("json_group_merge: nesting depth exceeds maximum limit of " +
		                            std::to_string(MAX_JSON_NESTING_DEPTH));
	}

	yyjson_val *patch_key = nullptr;
	duckdb_yyjson::yyjson_obj_iter patch_iter = duckdb_yyjson::yyjson_obj_iter_with(patch);
	while ((patch_key = duckdb_yyjson::yyjson_obj_iter_next(&patch_iter))) {
		auto key_str = duckdb_yyjson::yyjson_get_str(patch_key);
		auto key_len = duckdb_yyjson::yyjson_get_len(patch_key);
		auto patch_val = duckdb_yyjson::yyjson_obj_iter_get_val(patch_key);

		if (!key_str) {
			throw InvalidInputException("json_group_merge: encountered non-string object key");
		}

		std::string key(key_str, key_len);

		if (duckdb_yyjson::yyjson_is_null(patch_val)) {
			if (saw_nulls) {
				*saw_nulls = true;
			}
			if (null_treatment == JsonNullTreatment::DELETE_NULLS) {
				target.Erase(key);  // O(1) average
			}
			continue;
		}

		if (duckdb_yyjson::yyjson_is_obj(patch_val)) {
			// Recursive merge for nested objects
			auto existing = target.Find(key);  // O(1) average
			if (existing && existing->IsObject()) {
				// Existing value is object - merge into it
				ApplyPatchToMap(existing->AsObject(), patch_val, depth + 1, null_treatment, saw_nulls);
			} else {
				// Create new object and recursively apply patch (to filter nulls in IGNORE_NULLS mode)
				JsonObject new_obj;
				ApplyPatchToMap(new_obj, patch_val, depth + 1, null_treatment, saw_nulls);
				// Only store if non-empty (all-null patches should not create empty objects)
				if (!new_obj.Empty()) {
					target[key] = JsonValue::MakeObject(std::move(new_obj));
				}
			}
			continue;
		}

		// For all other types, replace directly
		target[key] = ParseYyjsonValue(patch_val, depth + 1);  // O(1) average
	}
}

// Compose patch into patch_map (for DELETE_NULLS mode) - preserves null markers
static void ComposePatchToMap(JsonObject &target, yyjson_val *patch, idx_t depth,
                              JsonNullTreatment null_treatment) {
	if (!patch) {
		return;
	}
	if (depth > MAX_JSON_NESTING_DEPTH) {
		throw InvalidInputException("json_group_merge: nesting depth exceeds maximum limit of " +
		                            std::to_string(MAX_JSON_NESTING_DEPTH));
	}

	yyjson_val *patch_key = nullptr;
	duckdb_yyjson::yyjson_obj_iter patch_iter = duckdb_yyjson::yyjson_obj_iter_with(patch);
	while ((patch_key = duckdb_yyjson::yyjson_obj_iter_next(&patch_iter))) {
		auto key_str = duckdb_yyjson::yyjson_get_str(patch_key);
		auto key_len = duckdb_yyjson::yyjson_get_len(patch_key);
		auto patch_val = duckdb_yyjson::yyjson_obj_iter_get_val(patch_key);

		if (!key_str) {
			throw InvalidInputException("json_group_merge: encountered non-string object key");
		}

		std::string key(key_str, key_len);

		if (duckdb_yyjson::yyjson_is_null(patch_val)) {
			if (null_treatment == JsonNullTreatment::IGNORE_NULLS) {
				continue;
			}
			// DELETE_NULLS: store the null marker for replay during Combine
			target[key] = JsonValue::MakeNull();
			continue;
		}

		if (duckdb_yyjson::yyjson_is_obj(patch_val)) {
			auto existing = target.Find(key);
			if (existing && existing->IsObject()) {
				ComposePatchToMap(existing->AsObject(), patch_val, depth + 1, null_treatment);
			} else {
				target[key] = ParseYyjsonValue(patch_val, depth + 1);
			}
			continue;
		}

		target[key] = ParseYyjsonValue(patch_val, depth + 1);
	}
}

// Apply JsonObject patch to target map (for Combine)
static void ApplyMapToMap(JsonObject &target, const JsonObject &source, idx_t depth,
                          JsonNullTreatment null_treatment, bool *saw_nulls) {
	if (depth > MAX_JSON_NESTING_DEPTH) {
		throw InvalidInputException("json_group_merge: nesting depth exceeds maximum limit of " +
		                            std::to_string(MAX_JSON_NESTING_DEPTH));
	}

	for (const auto &kv : source) {
		const auto &key = kv.first;
		const auto &val = kv.second;

		if (val.IsNull()) {
			if (saw_nulls) {
				*saw_nulls = true;
			}
			if (null_treatment == JsonNullTreatment::DELETE_NULLS) {
				target.Erase(key);
			}
			continue;
		}

		if (val.IsObject()) {
			auto existing = target.Find(key);
			if (existing && existing->IsObject()) {
				ApplyMapToMap(existing->AsObject(), val.AsObject(), depth + 1, null_treatment, saw_nulls);
			} else {
				target[key] = val;
			}
			continue;
		}

		target[key] = val;
	}
}

// Compose map into patch_map (for Combine in DELETE_NULLS mode)
static void ComposeMapToMap(JsonObject &target, const JsonObject &source, idx_t depth,
                            JsonNullTreatment null_treatment) {
	if (depth > MAX_JSON_NESTING_DEPTH) {
		throw InvalidInputException("json_group_merge: nesting depth exceeds maximum limit of " +
		                            std::to_string(MAX_JSON_NESTING_DEPTH));
	}

	for (const auto &kv : source) {
		const auto &key = kv.first;
		const auto &val = kv.second;

		if (val.IsNull()) {
			if (null_treatment == JsonNullTreatment::IGNORE_NULLS) {
				continue;
			}
			target[key] = JsonValue::MakeNull();
			continue;
		}

		if (val.IsObject()) {
			auto existing = target.Find(key);
			if (existing && existing->IsObject()) {
				ComposeMapToMap(existing->AsObject(), val.AsObject(), depth + 1, null_treatment);
			} else {
				target[key] = val;
			}
			continue;
		}

		target[key] = val;
	}
}

class JsonGroupMergeFunction {
public:
	static void Initialize(JsonGroupMergeState &state) {
		JsonGroupMergeStateInit(state);
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &) {
		JsonGroupMergeStateDestroy(state);
	}

	static inline string JsonParseError(const string_t &input, duckdb_yyjson::yyjson_read_err &err) {
		return JSONCommon::FormatParseError(input.GetDataUnsafe(), input.GetSize(), err);
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input) {
		static_assert(std::is_same<INPUT_TYPE, string_t>::value, "json_group_merge expects string_t input");
		duckdb_yyjson::yyjson_read_err err;
		auto doc = duckdb_yyjson::yyjson_read_opts(const_cast<char *>(input.GetDataUnsafe()), input.GetSize(),
		                                           JSONCommon::READ_FLAG, nullptr, &err);
		if (!doc) {
			throw InvalidInputException("json_group_merge: %s", JsonParseError(input, err));
		}
		yyjson_doc_ptr patch_doc(doc);
		auto patch_root = duckdb_yyjson::yyjson_doc_get_root(patch_doc.get());
		if (!patch_root) {
			throw InvalidInputException("json_group_merge: invalid JSON payload");
		}

		auto null_treatment = GetNullTreatment(unary_input.input.bind_data);

		// Non-object patch replaces the entire result (JSON merge patch semantics)
		if (!duckdb_yyjson::yyjson_is_obj(patch_root)) {
			if (state.scalar_replacement) {
				delete state.scalar_replacement;
			}
			state.scalar_replacement = new JsonValue(ParseYyjsonValue(patch_root, 0));
			// Clear the hash map since it's replaced
			state.result_map->Clear();
			state.result_has_input = true;
			// Clear patch tracking as well
			if (state.patch_map) {
				delete state.patch_map;
				state.patch_map = nullptr;
			}
			state.patch_has_input = false;
			state.patch_has_nulls = false;
			return;
		}

		// Object patch: if there was a scalar replacement, reset to object mode
		if (state.scalar_replacement) {
			delete state.scalar_replacement;
			state.scalar_replacement = nullptr;
		}

		bool saw_nulls = false;

		// Apply patch to result_map using O(1) hash map operations
		ApplyPatchToMap(*state.result_map, patch_root, 0, null_treatment, &saw_nulls);
		state.result_has_input = true;

		// For DELETE_NULLS mode, also compose into patch_map for Combine replay
		if (null_treatment == JsonNullTreatment::DELETE_NULLS) {
			if (state.patch_has_nulls || saw_nulls) {
				if (!state.patch_has_nulls) {
					// First time seeing nulls: copy result_map to patch_map
					state.patch_map = new JsonObject(*state.result_map);
					state.patch_has_nulls = true;
				}
				ComposePatchToMap(*state.patch_map, patch_root, 0, null_treatment);
				state.patch_has_input = true;
			}
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr_input_data) {
		if (!source.result_has_input) {
			return;
		}

		auto null_treatment = GetNullTreatment(aggr_input_data.bind_data);

		// If source has a scalar replacement, it replaces everything
		if (source.scalar_replacement) {
			if (target.scalar_replacement) {
				delete target.scalar_replacement;
			}
			target.scalar_replacement = new JsonValue(*source.scalar_replacement);
			target.result_map->Clear();
			target.result_has_input = true;
			if (target.patch_map) {
				delete target.patch_map;
				target.patch_map = nullptr;
			}
			target.patch_has_input = false;
			target.patch_has_nulls = false;
			return;
		}

		// Source is an object: if target has scalar replacement, clear it first
		if (target.scalar_replacement) {
			delete target.scalar_replacement;
			target.scalar_replacement = nullptr;
		}

		// For IGNORE_NULLS: use result_map directly
		// For DELETE_NULLS: use patch_map which preserves explicit null markers for replay
		const JsonObject *source_map = nullptr;
		bool source_has_input = false;

		if (null_treatment == JsonNullTreatment::IGNORE_NULLS || !source.patch_has_nulls) {
			source_map = source.result_map;
			source_has_input = source.result_has_input;
		} else {
			source_map = source.patch_map;
			source_has_input = source.patch_has_input;
		}

		if (!source_has_input || !source_map || source_map->Empty()) {
			return;
		}

		bool saw_nulls = false;
		ApplyMapToMap(*target.result_map, *source_map, 0, null_treatment, &saw_nulls);
		target.result_has_input = true;

		// For DELETE_NULLS, update target's patch_map for further combines when needed
		if (null_treatment == JsonNullTreatment::DELETE_NULLS) {
			if (target.patch_has_nulls || saw_nulls) {
				if (!target.patch_has_nulls) {
					target.patch_map = new JsonObject(*target.result_map);
					target.patch_has_nulls = true;
				}
				ComposeMapToMap(*target.patch_map, *source_map, 0, null_treatment);
				target.patch_has_input = true;
			}
		}
	}

	template <class RESULT_TYPE, class STATE>
	static void Finalize(STATE &state, RESULT_TYPE &target, AggregateFinalizeData &finalize_data) {
		if (!state.result_map) {
			finalize_data.ReturnNull();
			return;
		}

		// Build yyjson document
		auto doc = duckdb_yyjson::yyjson_mut_doc_new(nullptr);
		if (!doc) {
			throw InternalException("json_group_merge: failed to allocate output document");
		}
		std::unique_ptr<duckdb_yyjson::yyjson_mut_doc, decltype(&duckdb_yyjson::yyjson_mut_doc_free)>
		    doc_ptr(doc, duckdb_yyjson::yyjson_mut_doc_free);

		duckdb_yyjson::yyjson_mut_val *root;

		// If scalar replacement is set, use it instead of the hash map
		if (state.scalar_replacement) {
			root = BuildYyjsonValue(doc, *state.scalar_replacement);
			if (!root) {
				throw InternalException("json_group_merge: failed to build output value");
			}
		} else {
			// Build object from hash map
			root = duckdb_yyjson::yyjson_mut_obj(doc);
			if (!root) {
				throw InternalException("json_group_merge: failed to allocate output object");
			}

			// Populate from hash map
			for (const auto &kv : *state.result_map) {
				auto key = duckdb_yyjson::yyjson_mut_strncpy(doc, kv.first.c_str(), kv.first.size());
				auto val = BuildYyjsonValue(doc, kv.second);
				if (key && val && !duckdb_yyjson::yyjson_mut_obj_add(root, key, val)) {
					throw InternalException("json_group_merge: failed to add object member");
				}
			}
		}
		duckdb_yyjson::yyjson_mut_doc_set_root(doc, root);

		size_t output_length = 0;
		auto output_cstr = duckdb_yyjson::yyjson_mut_write_opts(doc, JSONCommon::WRITE_FLAG, nullptr,
		                                                        &output_length, nullptr);
		if (!output_cstr) {
			throw InternalException("json_group_merge: failed to serialize aggregate result");
		}
		std::unique_ptr<char, decltype(&free)> output_handle(output_cstr, free);
		target = StringVector::AddString(finalize_data.result, output_cstr, output_length);
	}

	static bool IgnoreNull() {
		return true;
	}
};

static string NormalizedNullTreatment(const string &input) {
	auto normalized = input;
	StringUtil::Trim(normalized);
	return StringUtil::Lower(normalized);
}

static string JsonGroupMergeNullOptionsText() {
	return "DELETE NULLS, IGNORE NULLS";
}

static unique_ptr<FunctionData> JsonGroupMergeBind(ClientContext &context, AggregateFunction &function,
                                                   vector<unique_ptr<Expression>> &arguments) {
	if (arguments.empty() || arguments.size() > 2) {
		throw BinderException(
		    "json_group_merge expects one JSON argument plus an optional treat_null_values parameter");
	}
	auto treatment = JsonNullTreatment::DELETE_NULLS;
	if (arguments.size() == 2) {
		auto &mode_arg = arguments[1];
		if (mode_arg->return_type.id() == LogicalTypeId::UNKNOWN || mode_arg->HasParameter()) {
			throw ParameterNotResolvedException();
		}
		if (!mode_arg->IsFoldable()) {
			throw BinderException("json_group_merge treat_null_values argument must be constant");
		}
		auto mode_value = ExpressionExecutor::EvaluateScalar(context, *mode_arg);
		if (mode_value.IsNull()) {
			throw InvalidInputException("json_group_merge: treat_null_values must be one of %s",
			                            JsonGroupMergeNullOptionsText().c_str());
		}
		if (mode_value.type().id() != LogicalTypeId::VARCHAR) {
			throw BinderException("json_group_merge treat_null_values must be a VARCHAR literal");
		}
		auto normalized = NormalizedNullTreatment(mode_value.ToString());
		if (normalized == "delete nulls") {
			treatment = JsonNullTreatment::DELETE_NULLS;
		} else if (normalized == "ignore nulls") {
			treatment = JsonNullTreatment::IGNORE_NULLS;
		} else {
			throw InvalidInputException("json_group_merge: treat_null_values must be one of %s",
			                            JsonGroupMergeNullOptionsText().c_str());
		}
		Function::EraseArgument(function, arguments, 1);
	}
	return make_uniq<JsonGroupMergeBindData>(treatment);
}

static AggregateFunction CreateJsonGroupMergeAggregate(const LogicalType &input_type) {
	AggregateFunction function(
	    "json_group_merge", {input_type}, LogicalType::JSON(), AggregateFunction::StateSize<JsonGroupMergeState>,
	    AggregateFunction::StateInitialize<JsonGroupMergeState, JsonGroupMergeFunction>,
	    AggregateFunction::UnaryScatterUpdate<JsonGroupMergeState, string_t, JsonGroupMergeFunction>,
	    AggregateFunction::StateCombine<JsonGroupMergeState, JsonGroupMergeFunction>,
	    AggregateFunction::StateFinalize<JsonGroupMergeState, string_t, JsonGroupMergeFunction>,
	    AggregateFunction::UnaryUpdate<JsonGroupMergeState, string_t, JsonGroupMergeFunction>);
	function.destructor = AggregateFunction::StateDestroy<JsonGroupMergeState, JsonGroupMergeFunction>;
	function.order_dependent = AggregateOrderDependent::ORDER_DEPENDENT;
	function.bind = JsonGroupMergeBind;
	return function;
}

static void AddJsonGroupMergeAggregate(AggregateFunctionSet &set, const LogicalType &input_type) {
	auto function = CreateJsonGroupMergeAggregate(input_type);
	set.AddFunction(function);
	function.arguments.emplace_back(LogicalType::VARCHAR);
	set.AddFunction(function);
}

using duckdb_yyjson::yyjson_arr_iter;
using duckdb_yyjson::yyjson_arr_iter_next;
using duckdb_yyjson::yyjson_arr_iter_with;
using duckdb_yyjson::yyjson_doc;
using duckdb_yyjson::yyjson_doc_free;
using duckdb_yyjson::yyjson_doc_get_root;
using duckdb_yyjson::yyjson_get_tag;
using duckdb_yyjson::yyjson_is_arr;
using duckdb_yyjson::yyjson_is_null;
using duckdb_yyjson::yyjson_is_obj;
using duckdb_yyjson::yyjson_is_str;
using duckdb_yyjson::yyjson_mut_doc;
using duckdb_yyjson::yyjson_mut_doc_free;
using duckdb_yyjson::yyjson_mut_doc_new;
using duckdb_yyjson::yyjson_mut_doc_set_root;
using duckdb_yyjson::yyjson_mut_is_obj;
using duckdb_yyjson::yyjson_mut_obj;
using duckdb_yyjson::yyjson_mut_obj_add;
using duckdb_yyjson::yyjson_mut_obj_add_val;
using duckdb_yyjson::yyjson_mut_obj_getn;
using duckdb_yyjson::yyjson_mut_obj_remove_keyn;
using duckdb_yyjson::yyjson_mut_strncpy;
using duckdb_yyjson::yyjson_mut_val;
using duckdb_yyjson::yyjson_mut_val_mut_copy;
using duckdb_yyjson::yyjson_mut_write_opts;
using duckdb_yyjson::yyjson_obj_iter;
using duckdb_yyjson::yyjson_obj_iter_get_val;
using duckdb_yyjson::yyjson_obj_iter_next;
using duckdb_yyjson::yyjson_obj_iter_with;
using duckdb_yyjson::yyjson_read_opts;
using duckdb_yyjson::yyjson_val;
using duckdb_yyjson::yyjson_val_mut_copy;

// Hash function for JSON keys (FNV-1a)
static inline uint64_t HashKeyBytes(const char *data, idx_t len) {
	uint64_t hash = 14695981039346656037ULL;
	for (idx_t i = 0; i < len; i++) {
		hash ^= static_cast<uint64_t>(static_cast<uint8_t>(data[i]));
		hash *= 1099511628211ULL;
	}
	return hash;
}

// Count trailing zeros for bitset iteration (x must be non-zero)
static inline idx_t CountTrailingZeros(uint64_t x) {
	D_ASSERT(x != 0);
#if defined(__GNUC__) || defined(__clang__)
	return static_cast<idx_t>(__builtin_ctzll(x));
#elif defined(_MSC_VER)
	unsigned long idx;
	_BitScanForward64(&idx, x);
	return static_cast<idx_t>(idx);
#else
	idx_t count = 0;
	while ((x & 1) == 0) {
		x >>= 1;
		count++;
	}
	return count;
#endif
}

struct JsonExtractColumnsBindData : public FunctionData {
	JsonExtractColumnsBindData(vector<string> column_names_p, vector<string> patterns_p,
	                           child_list_t<LogicalType> children_p, duckdb_re2::RE2::Options options_p)
	    : column_names(std::move(column_names_p)), patterns(std::move(patterns_p)), children(std::move(children_p)),
	      options(std::move(options_p)), column_count(this->patterns.size()) {
		BuildPatternSet();
	}

	vector<string> column_names;
	vector<string> patterns;
	child_list_t<LogicalType> children;
	duckdb_re2::RE2::Options options;
	unique_ptr<duckdb_re2::RE2::Set> pattern_set;
	idx_t column_count;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<JsonExtractColumnsBindData>(column_names, patterns, children, options);
	}

	bool Equals(const FunctionData &other_p) const override {
		const auto &other = other_p.Cast<JsonExtractColumnsBindData>();
		return column_names == other.column_names && patterns == other.patterns &&
		       options.case_sensitive() == other.options.case_sensitive();
	}

private:
	void BuildPatternSet() {
		pattern_set = make_uniq<duckdb_re2::RE2::Set>(options, duckdb_re2::RE2::UNANCHORED);
		for (idx_t i = 0; i < patterns.size(); i++) {
			std::string error;
			int idx = pattern_set->Add(patterns[i], &error);
			if (idx < 0) {
				throw BinderException("json_extract_columns: %s", error);
			}
			D_ASSERT(static_cast<idx_t>(idx) == i);
		}
		if (!pattern_set->Compile()) {
			throw BinderException("json_extract_columns: failed to compile pattern set");
		}
	}
};

// Cache for key→matched columns mapping
static constexpr idx_t KEY_MATCH_CACHE_SIZE = 8192;
static constexpr idx_t KEY_MATCH_CACHE_WAYS = 2;
static constexpr idx_t KEY_MATCH_CACHE_SETS = KEY_MATCH_CACHE_SIZE / KEY_MATCH_CACHE_WAYS;
static constexpr idx_t KEY_MATCH_MAX_KEY_STORAGE = 256 * 1024;

struct KeyMatchCacheEntry {
	idx_t key_offset = 0;
	idx_t key_len = 0;
	uint64_t key_hash = 0;
	uint64_t match_bitset = 0;
	bool valid = false;
};

struct KeyMatchCache {
	std::array<KeyMatchCacheEntry, KEY_MATCH_CACHE_SIZE> entries;
	vector<char> key_storage;
	idx_t key_storage_offset = 0;

	KeyMatchCache() {
		key_storage.resize(64 * 1024);
	}

	void Reset() {
		for (auto &e : entries) {
			e.valid = false;
		}
		key_storage_offset = 0;
	}

	bool IsFull() const {
		return key_storage_offset >= KEY_MATCH_MAX_KEY_STORAGE;
	}

	const char *GetKeyData(const KeyMatchCacheEntry &entry) const {
		return key_storage.data() + entry.key_offset;
	}

	// Returns true if entry was inserted, false if storage is exhausted
	bool TryInsert(idx_t set_idx, const char *key_str, idx_t key_len, uint64_t key_hash, uint64_t match_bitset) {
		// Find target slot: prefer invalid, else evict way 0
		idx_t target_way = 0;
		for (idx_t way = 0; way < KEY_MATCH_CACHE_WAYS; way++) {
			if (!entries[set_idx + way].valid) {
				target_way = way;
				break;
			}
		}

		// Grow storage if needed and possible
		if (key_storage_offset + key_len > key_storage.size()) {
			if (key_storage.size() >= KEY_MATCH_MAX_KEY_STORAGE) {
				return false;
			}
			key_storage.resize(key_storage.size() + 64 * 1024);
		}

		auto &entry = entries[set_idx + target_way];
		std::memcpy(key_storage.data() + key_storage_offset, key_str, key_len);
		entry.key_offset = key_storage_offset;
		key_storage_offset += key_len;
		entry.key_len = key_len;
		entry.key_hash = key_hash;
		entry.match_bitset = match_bitset;
		entry.valid = true;
		return true;
	}
};

// Chunked cache for >64 patterns - stores match results as array of uint64_t chunks
struct ChunkedKeyMatchCacheEntry {
	idx_t key_offset = 0;
	idx_t key_len = 0;
	uint64_t key_hash = 0;
	idx_t match_offset = 0;
	bool valid = false;
};

struct ChunkedKeyMatchCache {
	std::array<ChunkedKeyMatchCacheEntry, KEY_MATCH_CACHE_SIZE> entries;
	vector<char> key_storage;
	vector<uint64_t> match_storage;
	idx_t key_storage_offset = 0;
	idx_t match_storage_offset = 0;
	idx_t chunks_per_entry = 0;

	void Init(idx_t column_count) {
		chunks_per_entry = (column_count + 63) / 64;
		key_storage.resize(64 * 1024);
		match_storage.resize(KEY_MATCH_CACHE_SIZE * chunks_per_entry);
	}

	void Reset() {
		for (auto &e : entries) {
			e.valid = false;
		}
		key_storage_offset = 0;
		match_storage_offset = 0;
	}

	bool IsFull() const {
		return key_storage_offset >= KEY_MATCH_MAX_KEY_STORAGE ||
		       match_storage_offset + chunks_per_entry > match_storage.size();
	}

	const char *GetKeyData(const ChunkedKeyMatchCacheEntry &entry) const {
		return key_storage.data() + entry.key_offset;
	}

	const uint64_t *GetMatchChunks(const ChunkedKeyMatchCacheEntry &entry) const {
		return match_storage.data() + entry.match_offset;
	}

	// Returns pointer to match chunks if inserted, nullptr if storage is exhausted
	const uint64_t *TryInsert(idx_t set_idx, const char *key_str, idx_t key_len, uint64_t key_hash,
	                          const vector<int> &match_results) {
		if (match_storage_offset + chunks_per_entry > match_storage.size()) {
			return nullptr;
		}

		// Find target slot: prefer invalid, else evict way 0
		idx_t target_way = 0;
		for (idx_t way = 0; way < KEY_MATCH_CACHE_WAYS; way++) {
			if (!entries[set_idx + way].valid) {
				target_way = way;
				break;
			}
		}

		// Grow key storage if needed and possible
		if (key_storage_offset + key_len > key_storage.size()) {
			if (key_storage.size() >= KEY_MATCH_MAX_KEY_STORAGE) {
				return nullptr;
			}
			key_storage.resize(key_storage.size() + 64 * 1024);
		}

		auto &entry = entries[set_idx + target_way];

		// Store key
		std::memcpy(key_storage.data() + key_storage_offset, key_str, key_len);
		entry.key_offset = key_storage_offset;
		key_storage_offset += key_len;
		entry.key_len = key_len;
		entry.key_hash = key_hash;

		// Store match chunks
		idx_t match_offset = match_storage_offset;
		match_storage_offset += chunks_per_entry;
		uint64_t *chunks = match_storage.data() + match_offset;
		std::memset(chunks, 0, chunks_per_entry * sizeof(uint64_t));
		for (int idx : match_results) {
			chunks[idx / 64] |= (1ULL << (idx % 64));
		}
		entry.match_offset = match_offset;
		entry.valid = true;

		return chunks;
	}
};

struct JsonExtractColumnsLocalState : public FunctionLocalState {
	JsonExtractColumnsLocalState(Allocator &allocator, idx_t column_count)
	    : json_allocator(std::make_shared<JSONAllocator>(allocator)), buffers(column_count),
	      has_match(column_count, false), use_bitset_cache(column_count <= 64), column_count(column_count) {
		match_results.reserve(column_count);
		if (!use_bitset_cache) {
			chunked_cache.Init(column_count);
		}
	}

	shared_ptr<JSONAllocator> json_allocator;
	vector<string> buffers;
	vector<bool> has_match;

	// Cache for key→matches (≤64 patterns)
	KeyMatchCache cache;
	// Chunked cache for key→matches (>64 patterns)
	ChunkedKeyMatchCache chunked_cache;
	vector<int> match_results;
	bool use_bitset_cache;
	idx_t column_count;
};

static unique_ptr<FunctionLocalState>
JsonExtractColumnsInitLocalState(ExpressionState &state, const BoundFunctionExpression &expr, FunctionData *bind_data) {
	auto &context = state.GetContext();
	auto column_count = bind_data->Cast<JsonExtractColumnsBindData>().column_names.size();
	return make_uniq<JsonExtractColumnsLocalState>(BufferAllocator::Get(context), column_count);
}

static void AppendJsonValue(string &target, yyjson_val *val, yyjson_alc *alc) {
	switch (yyjson_get_tag(val)) {
	case YYJSON_TYPE_NULL | YYJSON_SUBTYPE_NONE:
		target.append("null");
		break;
	case YYJSON_TYPE_BOOL | YYJSON_SUBTYPE_TRUE:
		target.append("true");
		break;
	case YYJSON_TYPE_BOOL | YYJSON_SUBTYPE_FALSE:
		target.append("false");
		break;
	case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NOESC:
	case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NONE: {
		auto str = duckdb_yyjson::yyjson_get_str(val);
		auto len = duckdb_yyjson::yyjson_get_len(val);
		target.append(str, len);
		break;
	}
	default: {
		idx_t len;
		auto data = JSONCommon::WriteVal<yyjson_val>(val, alc, len);
		target.append(data, len);
		break;
	}
	}
}

static unique_ptr<FunctionData> JsonExtractColumnsBind(ClientContext &context, ScalarFunction &function,
                                                       vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() < 2 || arguments.size() > 3) {
		throw BinderException("json_extract_columns expects json input, columns mapping, and optional separator");
	}
	auto &columns_arg = arguments[1];
	if (columns_arg->return_type.id() == LogicalTypeId::UNKNOWN || columns_arg->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!columns_arg->IsFoldable()) {
		throw BinderException("json_extract_columns columns argument must be constant");
	}

	auto columns_value = ExpressionExecutor::EvaluateScalar(context, *columns_arg);
	if (columns_value.IsNull()) {
		throw BinderException("json_extract_columns: columns argument must be a JSON object");
	}
	auto type_id = columns_value.type().id();
	if (type_id != LogicalTypeId::VARCHAR) {
		throw BinderException("json_extract_columns columns argument must be a JSON string");
	}
	auto columns_input = StringValue::Get(columns_value);

	duckdb_yyjson::yyjson_read_err err;
	auto doc = yyjson_doc_ptr(yyjson_read_opts(const_cast<char *>(columns_input.c_str()), columns_input.size(),
	                                           JSONCommon::READ_FLAG, nullptr, &err));
	if (!doc) {
		throw BinderException("json_extract_columns: %s",
		                      JSONCommon::FormatParseError(columns_input.c_str(), columns_input.size(), err));
	}
	auto root = yyjson_doc_get_root(doc.get());
	if (!root || !yyjson_is_obj(root)) {
		throw BinderException("json_extract_columns: columns argument must be a JSON object");
	}

	vector<string> column_names;
	vector<string> patterns;
	child_list_t<LogicalType> children;
	unordered_set<string> seen_columns;

	yyjson_val *key = nullptr;
	yyjson_obj_iter iter = yyjson_obj_iter_with(root);
	while ((key = yyjson_obj_iter_next(&iter))) {
		auto key_str = duckdb_yyjson::yyjson_get_str(key);
		auto key_len = duckdb_yyjson::yyjson_get_len(key);
		auto value = yyjson_obj_iter_get_val(key);
		if (!yyjson_is_str(value)) {
			throw BinderException("json_extract_columns: column patterns must be strings");
		}
		string column_name(key_str, key_len);
		if (!seen_columns.insert(column_name).second) {
			throw BinderException("json_extract_columns: duplicate output column name \"%s\"", column_name.c_str());
		}
		auto pattern_str = string(duckdb_yyjson::yyjson_get_str(value), duckdb_yyjson::yyjson_get_len(value));
		column_names.push_back(std::move(column_name));
		patterns.push_back(std::move(pattern_str));
	}

	children.reserve(column_names.size());
	for (idx_t i = 0; i < column_names.size(); i++) {
		children.emplace_back(column_names[i], LogicalType::VARCHAR);
	}
	function.return_type = LogicalType::STRUCT(children);

	duckdb_re2::RE2::Options options;
	options.set_log_errors(false);
	return make_uniq<JsonExtractColumnsBindData>(std::move(column_names), std::move(patterns), std::move(children),
	                                             options);
}

static void JsonExtractColumnsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &bind_data = func_expr.bind_info->Cast<JsonExtractColumnsBindData>();
	auto &local_state = ExecuteFunctionState::GetFunctionState(state)->Cast<JsonExtractColumnsLocalState>();

	auto column_count = bind_data.column_names.size();
	auto &children = StructVector::GetEntries(result);
	D_ASSERT(children.size() == column_count);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto &result_validity = FlatVector::Validity(result);
	for (auto &child : children) {
		child->SetVectorType(VectorType::FLAT_VECTOR);
	}

	vector<string_t *> child_data(column_count, nullptr);
	vector<ValidityMask *> child_validities(column_count, nullptr);
	for (idx_t i = 0; i < column_count; i++) {
		child_data[i] = FlatVector::GetData<string_t>(*children[i]);
		child_validities[i] = &FlatVector::Validity(*children[i]);
	}

	UnifiedVectorFormat json_data;
	args.data[0].ToUnifiedFormat(args.size(), json_data);
	auto json_inputs = UnifiedVectorFormat::GetData<string_t>(json_data);

	auto has_separator_argument = args.ColumnCount() == 3;
	idx_t separator_column_index = has_separator_argument ? args.ColumnCount() - 1 : 0;
	bool separator_is_constant = false;
	std::string separator_constant_storage;
	const char *separator_constant_ptr = nullptr;
	idx_t separator_constant_len = 0;
	if (has_separator_argument) {
		auto &separator_vec = args.data[separator_column_index];
		separator_is_constant = separator_vec.GetVectorType() == VectorType::CONSTANT_VECTOR;
		if (separator_is_constant) {
			if (ConstantVector::IsNull(separator_vec)) {
				throw InvalidInputException("json_extract_columns: separator cannot be NULL");
			}
			auto separator_value = separator_vec.GetValue(0);
			separator_constant_storage = StringValue::Get(separator_value);
			separator_constant_ptr = separator_constant_storage.c_str();
			separator_constant_len = separator_constant_storage.size();
		}
	}

	for (idx_t row = 0; row < args.size(); row++) {
		auto json_idx = json_data.sel->get_index(row);

		std::string separator_storage;
		const char *separator_data_ptr;
		idx_t separator_len;
		if (has_separator_argument) {
			if (separator_is_constant) {
				separator_data_ptr = separator_constant_ptr;
				separator_len = separator_constant_len;
			} else {
				auto separator_value = args.GetValue(separator_column_index, row);
				if (separator_value.IsNull()) {
					throw InvalidInputException("json_extract_columns: separator cannot be NULL");
				}
				separator_storage = StringValue::Get(separator_value);
				separator_data_ptr = separator_storage.c_str();
				separator_len = separator_storage.size();
			}
		} else {
			separator_data_ptr = "";
			separator_len = 0;
		}

		if (!json_data.validity.RowIsValid(json_idx)) {
			result_validity.SetInvalid(row);
			for (auto &validity : child_validities) {
				validity->SetInvalid(row);
			}
			continue;
		}
		result_validity.SetValid(row);

		auto &allocator = *local_state.json_allocator;
		allocator.Reset();
		auto alc = allocator.GetYYAlc();

		auto input = json_inputs[json_idx];
		auto input_data = input.GetDataUnsafe();
		auto input_length = input.GetSize();
		duckdb_yyjson::yyjson_read_err err;
		auto doc = yyjson_doc_ptr(
		    yyjson_read_opts(const_cast<char *>(input_data), input_length, JSONCommon::READ_FLAG, alc, &err));
		if (!doc) {
			throw InvalidInputException("json_extract_columns: %s",
			                            JSONCommon::FormatParseError(input_data, input_length, err));
		}
		auto root = yyjson_doc_get_root(doc.get());
		if (!root || !yyjson_is_obj(root)) {
			throw InvalidInputException("json_extract_columns: expected JSON object input");
		}

		for (auto &buffer : local_state.buffers) {
			buffer.clear();
		}
		std::fill(local_state.has_match.begin(), local_state.has_match.end(), false);

		// Reset cache if storage is full
		auto &cache = local_state.cache;
		auto &chunked_cache = local_state.chunked_cache;
		if (local_state.use_bitset_cache) {
			if (cache.IsFull()) {
				cache.Reset();
			}
		} else {
			if (chunked_cache.IsFull()) {
				chunked_cache.Reset();
			}
		}

		auto &pattern_set = *bind_data.pattern_set;
		const bool use_bitset = local_state.use_bitset_cache;

		yyjson_val *key = nullptr;
		yyjson_obj_iter iter = yyjson_obj_iter_with(root);
		while ((key = yyjson_obj_iter_next(&iter))) {
			auto key_str = duckdb_yyjson::yyjson_get_str(key);
			auto key_len = duckdb_yyjson::yyjson_get_len(key);
			if (!key_str) {
				throw InvalidInputException("json_extract_columns: encountered non-string object key");
			}

			// Hash the key for cache lookup
			uint64_t key_hash = HashKeyBytes(key_str, key_len);
			idx_t set_idx = (key_hash & (KEY_MATCH_CACHE_SETS - 1)) * KEY_MATCH_CACHE_WAYS;

			auto value = yyjson_obj_iter_get_val(key);

			if (use_bitset) {
				// Fast path for ≤64 columns: use cache with bitset
				// Cache lookup (2-way associative)
				bool cache_hit = false;
				uint64_t match_bitset = 0;
				for (idx_t way = 0; way < KEY_MATCH_CACHE_WAYS; way++) {
					auto &entry = cache.entries[set_idx + way];
					if (entry.valid && entry.key_hash == key_hash && entry.key_len == key_len &&
					    std::memcmp(cache.GetKeyData(entry), key_str, key_len) == 0) {
						cache_hit = true;
						match_bitset = entry.match_bitset;
						break;
					}
				}

				// On cache miss, compute matches and store in cache
				if (!cache_hit) {
					duckdb_re2::StringPiece key_piece(key_str, key_len);
					local_state.match_results.clear();
					pattern_set.Match(key_piece, &local_state.match_results);

					// Convert to bitset
					match_bitset = 0;
					for (int idx : local_state.match_results) {
						match_bitset |= (1ULL << idx);
					}

					cache.TryInsert(set_idx, key_str, key_len, key_hash, match_bitset);
				}

				// Iterate set bits
				uint64_t remaining = match_bitset;
				while (remaining != 0) {
					idx_t col_idx = CountTrailingZeros(remaining);
					remaining &= (remaining - 1);

					if (local_state.has_match[col_idx]) {
						local_state.buffers[col_idx].append(separator_data_ptr, separator_len);
					} else {
						local_state.has_match[col_idx] = true;
					}
					AppendJsonValue(local_state.buffers[col_idx], value, alc);
				}
			} else {
				// Fallback for > 64 columns: use chunked cache
				idx_t chunked_set_idx = (key_hash & (KEY_MATCH_CACHE_SETS - 1)) * KEY_MATCH_CACHE_WAYS;

				// Cache lookup (2-way associative)
				bool cache_hit = false;
				const uint64_t *match_chunks = nullptr;
				for (idx_t way = 0; way < KEY_MATCH_CACHE_WAYS; way++) {
					auto &entry = chunked_cache.entries[chunked_set_idx + way];
					if (entry.valid && entry.key_hash == key_hash && entry.key_len == key_len &&
					    std::memcmp(chunked_cache.GetKeyData(entry), key_str, key_len) == 0) {
						cache_hit = true;
						match_chunks = chunked_cache.GetMatchChunks(entry);
						break;
					}
				}

				// On cache miss, compute matches and store in cache
				if (!cache_hit) {
					duckdb_re2::StringPiece key_piece(key_str, key_len);
					local_state.match_results.clear();
					pattern_set.Match(key_piece, &local_state.match_results);

					match_chunks = chunked_cache.TryInsert(chunked_set_idx, key_str, key_len, key_hash,
					                                       local_state.match_results);

					// If cache insert failed, process directly from match_results
					if (!match_chunks) {
						for (int col_idx : local_state.match_results) {
							if (local_state.has_match[col_idx]) {
								local_state.buffers[col_idx].append(separator_data_ptr, separator_len);
							} else {
								local_state.has_match[col_idx] = true;
							}
							AppendJsonValue(local_state.buffers[col_idx], value, alc);
						}
						continue;
					}
				}

				// Iterate over chunks and their set bits
				for (idx_t chunk_idx = 0; chunk_idx < chunked_cache.chunks_per_entry; chunk_idx++) {
					uint64_t remaining = match_chunks[chunk_idx];
					idx_t base_col = chunk_idx * 64;
					while (remaining != 0) {
						idx_t bit_idx = CountTrailingZeros(remaining);
						remaining &= (remaining - 1);
						idx_t col_idx = base_col + bit_idx;

						if (local_state.has_match[col_idx]) {
							local_state.buffers[col_idx].append(separator_data_ptr, separator_len);
						} else {
							local_state.has_match[col_idx] = true;
						}
						AppendJsonValue(local_state.buffers[col_idx], value, alc);
					}
				}
			}
		}

		for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
			if (!local_state.has_match[col_idx]) {
				child_validities[col_idx]->SetInvalid(row);
				continue;
			}
			child_validities[col_idx]->SetValid(row);
			child_data[col_idx][row] = StringVector::AddString(*children[col_idx], local_state.buffers[col_idx]);
		}
	}
}

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

// Depth-first traversal that materializes key paths for leaf values.
static void FlattenIntoObject(yyjson_val *node, yyjson_mut_doc *out_doc, yyjson_mut_val *out_obj,
                              std::string &key_buffer, const string &separator, idx_t depth = 0) {
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
				key_buffer.append(separator);
			}
			auto key_str = duckdb_yyjson::yyjson_get_str(key);
			auto key_len = duckdb_yyjson::yyjson_get_len(key);
			key_buffer.append(key_str, key_len);
			FlattenIntoObject(child, out_doc, out_obj, key_buffer, separator, depth + 1);
			key_buffer.resize(previous_size);
		}
	} else if (yyjson_is_arr(node)) {
		yyjson_val *child;
		yyjson_arr_iter iter = yyjson_arr_iter_with(node);
		idx_t index = 0;
		while ((child = yyjson_arr_iter_next(&iter))) {
			auto previous_size = key_buffer.size();
			if (previous_size != 0) {
				key_buffer.append(separator);
			}
			AppendIndexToKeyBuffer(key_buffer, index);
			FlattenIntoObject(child, out_doc, out_obj, key_buffer, separator, depth + 1);
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
inline string_t JsonFlattenSingle(Vector &result, const string_t &input, JsonFlattenLocalState &local_state,
                                  const string &separator) {
	auto &allocator = *local_state.json_allocator;
	allocator.Reset();
	auto alc = allocator.GetYYAlc();
	auto input_data = input.GetDataUnsafe();
	auto input_length = input.GetSize();
	duckdb_yyjson::yyjson_read_err err;
	auto doc = yyjson_doc_ptr(
	    yyjson_read_opts(const_cast<char *>(input_data), input_length, JSONCommon::READ_FLAG, alc, &err));
	if (!doc) {
		throw InvalidInputException("json_flatten: %s", JSONCommon::FormatParseError(input_data, input_length, err));
	}
	auto root = yyjson_doc_get_root(doc.get());
	if (!root || yyjson_is_null(root) || (!yyjson_is_obj(root) && !yyjson_is_arr(root))) {
		return StringVector::AddString(result, input);
	}
	auto out_doc = yyjson_mut_doc_ptr(yyjson_mut_doc_new(alc));
	if (!out_doc) {
		throw InternalException("json_flatten: failed to allocate output document");
	}
	auto out_root = yyjson_mut_obj(out_doc.get());
	if (!out_root) {
		throw InternalException("json_flatten: failed to allocate output object");
	}
	yyjson_mut_doc_set_root(out_doc.get(), out_root);
	auto &key_buffer = local_state.key_buffer;
	key_buffer.clear();
	key_buffer.reserve(static_cast<size_t>(std::min<idx_t>(input_length, DEFAULT_KEY_BUFFER_SIZE)));
	FlattenIntoObject(root, out_doc.get(), out_root, key_buffer, separator, 0);
	size_t output_length = 0;
	auto output_cstr = yyjson_mut_write_opts(out_doc.get(), JSONCommon::WRITE_FLAG, nullptr, &output_length, nullptr);
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
	auto doc = yyjson_doc_ptr(
	    yyjson_read_opts(const_cast<char *>(input_data), input_length, JSONCommon::READ_FLAG, alc, &err));
	if (!doc) {
		throw InvalidInputException("json_add_prefix: %s", JSONCommon::FormatParseError(input_data, input_length, err));
	}

	auto root = yyjson_doc_get_root(doc.get());
	if (!root || !yyjson_is_obj(root)) {
		throw InvalidInputException("json_add_prefix: expected JSON object input");
	}

	auto out_doc = yyjson_mut_doc_ptr(yyjson_mut_doc_new(alc));
	if (!out_doc) {
		throw InternalException("json_add_prefix: failed to allocate output document");
	}
	auto out_root = yyjson_mut_obj(out_doc.get());
	if (!out_root) {
		throw InternalException("json_add_prefix: failed to allocate output object");
	}
	yyjson_mut_doc_set_root(out_doc.get(), out_root);

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
			new_key_val = yyjson_mut_strncpy(out_doc.get(), buffer, prefixed_len);
		} else {
			// Fallback to heap for large keys
			std::string new_key;
			new_key.reserve(prefixed_len);
			new_key.append(prefix_data, prefix_length);
			new_key.append(key_str, key_len);
			new_key_val = yyjson_mut_strncpy(out_doc.get(), new_key.c_str(), prefixed_len);
		}

		if (!new_key_val) {
			throw InternalException("json_add_prefix: failed to allocate key storage");
		}
		auto new_key_ptr = duckdb_yyjson::yyjson_mut_get_str(new_key_val);

		auto value_copy = yyjson_val_mut_copy(out_doc.get(), value);
		if (!value_copy) {
			throw InternalException("json_add_prefix: failed to allocate value storage");
		}

		if (!yyjson_mut_obj_add_val(out_doc.get(), out_root, new_key_ptr, value_copy)) {
			throw InternalException("json_add_prefix: failed to add prefixed key-value pair");
		}
	}

	size_t output_length = 0;
	auto output_cstr = yyjson_mut_write_opts(out_doc.get(), JSONCommon::WRITE_FLAG, nullptr, &output_length, nullptr);
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
	auto bind_data = state.expr.Cast<BoundFunctionExpression>().bind_info.get();
	auto &separator = GetJsonFlattenSeparator(bind_data);
	auto &input = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(input, result, args.size(), [&](const string_t &json_input) {
		return JsonFlattenSingle(result, json_input, local_state, separator);
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
	child_list_t<LogicalType> empty_children;
	auto json_extract_columns_function =
	    ScalarFunction("json_extract_columns", {LogicalType::JSON(), LogicalType::VARCHAR, LogicalType::VARCHAR},
	                   LogicalType::STRUCT(empty_children), JsonExtractColumnsFunction, JsonExtractColumnsBind, nullptr,
	                   nullptr, JsonExtractColumnsInitLocalState);
	json_extract_columns_function.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	RegisterScalarFunction(ctx, json_extract_columns_function);
	auto json_extract_columns_default_separator_function = ScalarFunction(
	    "json_extract_columns", {LogicalType::JSON(), LogicalType::VARCHAR}, LogicalType::STRUCT(empty_children),
	    JsonExtractColumnsFunction, JsonExtractColumnsBind, nullptr, nullptr, JsonExtractColumnsInitLocalState);
	json_extract_columns_default_separator_function.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	RegisterScalarFunction(ctx, json_extract_columns_default_separator_function);

	auto json_flatten_scalar_function =
	    ScalarFunction("json_flatten", {LogicalType::JSON()}, LogicalType::JSON(), JsonFlattenScalarFun, nullptr,
	                   nullptr, nullptr, JsonFlattenInitLocalState);
	RegisterScalarFunction(ctx, json_flatten_scalar_function);

	auto json_flatten_separator_scalar_function =
	    ScalarFunction("json_flatten", {LogicalType::JSON(), LogicalType::VARCHAR}, LogicalType::JSON(),
	                   JsonFlattenScalarFun, JsonFlattenBind, nullptr, nullptr, JsonFlattenInitLocalState);
	RegisterScalarFunction(ctx, json_flatten_separator_scalar_function);

	auto json_add_prefix_scalar_function =
	    ScalarFunction("json_add_prefix", {LogicalType::JSON(), LogicalType::VARCHAR}, LogicalType::JSON(),
	                   JsonAddPrefixScalarFun, nullptr, nullptr, nullptr, JsonAddPrefixInitLocalState);
	RegisterScalarFunction(ctx, json_add_prefix_scalar_function);

	AggregateFunctionSet json_group_merge_set("json_group_merge");
	AddJsonGroupMergeAggregate(json_group_merge_set, LogicalType::JSON());
	AddJsonGroupMergeAggregate(json_group_merge_set, LogicalType::VARCHAR);
	if (json_group_merge_set.Size() != 4) {
		throw InternalException("json_group_merge: expected 4 overloads, found %llu",
		                        (long long)json_group_merge_set.Size());
	}
	idx_t double_arg_count = 0;
	for (idx_t i = 0; i < json_group_merge_set.Size(); i++) {
		if (json_group_merge_set.GetFunctionReferenceByOffset(i).arguments.size() == 2) {
			double_arg_count++;
		}
	}
	if (double_arg_count != 2) {
		throw InternalException("json_group_merge: expected 2 overloads with treat_null_values parameter, found %llu",
		                        (long long)double_arg_count);
	}
	RegisterAggregateFunction(ctx, std::move(json_group_merge_set));
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
