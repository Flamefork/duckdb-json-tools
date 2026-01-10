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

struct JsonGroupMergeState {
	yyjson_mut_doc *result_doc;
	yyjson_mut_doc *patch_doc;
	bool result_has_input;
	bool patch_has_input;
	idx_t result_replacements_since_compact;
	idx_t patch_replacements_since_compact;
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

static void JsonGroupMergeStateInit(JsonGroupMergeState &state) {
	state.result_doc = yyjson_mut_doc_new(nullptr);
	if (!state.result_doc) {
		throw InternalException("json_group_merge: failed to allocate aggregate state");
	}
	state.patch_doc = yyjson_mut_doc_new(nullptr);
	if (!state.patch_doc) {
		yyjson_mut_doc_free(state.result_doc);
		state.result_doc = nullptr;
		throw InternalException("json_group_merge: failed to allocate aggregate state");
	}
	auto result_root = yyjson_mut_obj(state.result_doc);
	if (!result_root) {
		yyjson_mut_doc_free(state.patch_doc);
		yyjson_mut_doc_free(state.result_doc);
		state.patch_doc = nullptr;
		state.result_doc = nullptr;
		throw InternalException("json_group_merge: failed to allocate initial JSON object");
	}
	auto patch_root = yyjson_mut_obj(state.patch_doc);
	if (!patch_root) {
		yyjson_mut_doc_free(state.patch_doc);
		yyjson_mut_doc_free(state.result_doc);
		state.patch_doc = nullptr;
		state.result_doc = nullptr;
		throw InternalException("json_group_merge: failed to allocate initial JSON object");
	}
	yyjson_mut_doc_set_root(state.result_doc, result_root);
	yyjson_mut_doc_set_root(state.patch_doc, patch_root);
	state.result_has_input = false;
	state.patch_has_input = false;
	state.result_replacements_since_compact = 0;
	state.patch_replacements_since_compact = 0;
}

static void JsonGroupMergeStateDestroy(JsonGroupMergeState &state) {
	if (state.result_doc) {
		yyjson_mut_doc_free(state.result_doc);
		state.result_doc = nullptr;
	}
	if (state.patch_doc) {
		yyjson_mut_doc_free(state.patch_doc);
		state.patch_doc = nullptr;
	}
	state.result_has_input = false;
	state.patch_has_input = false;
	state.result_replacements_since_compact = 0;
	state.patch_replacements_since_compact = 0;
}

constexpr idx_t JSON_GROUP_MERGE_COMPACT_THRESHOLD = 1024;
// Maximum nesting depth to prevent stack exhaustion from pathological inputs
constexpr idx_t MAX_JSON_NESTING_DEPTH = 1000;

static yyjson_mut_val *JsonGroupMergeApplyPatchInternal(yyjson_mut_doc *doc, yyjson_mut_val *base, yyjson_val *patch,
                                                        idx_t depth, idx_t &replacements_since_compact,
                                                        JsonNullTreatment null_treatment);

static void JsonGroupMergeCompactDoc(yyjson_mut_doc *&doc) {
	if (!doc || !doc->root) {
		return;
	}
	auto new_doc = yyjson_mut_doc_new(nullptr);
	if (!new_doc) {
		throw InternalException("json_group_merge: failed to compact aggregate state");
	}
	auto root_copy = yyjson_mut_val_mut_copy(new_doc, doc->root);
	if (!root_copy) {
		yyjson_mut_doc_free(new_doc);
		throw InternalException("json_group_merge: failed to copy aggregate state during compaction");
	}
	yyjson_mut_doc_set_root(new_doc, root_copy);
	yyjson_mut_doc_free(doc);
	doc = new_doc;
}

static void JsonGroupMergeMaybeCompact(JsonGroupMergeState &state) {
	if (state.result_replacements_since_compact >= JSON_GROUP_MERGE_COMPACT_THRESHOLD) {
		JsonGroupMergeCompactDoc(state.result_doc);
		state.result_replacements_since_compact = 0;
	}
	if (state.patch_replacements_since_compact >= JSON_GROUP_MERGE_COMPACT_THRESHOLD) {
		JsonGroupMergeCompactDoc(state.patch_doc);
		state.patch_replacements_since_compact = 0;
	}
}

static void JsonGroupMergeApplyResultPatch(JsonGroupMergeState &state, yyjson_val *patch_root,
                                           JsonNullTreatment null_treatment) {
	if (!patch_root) {
		throw InvalidInputException("json_group_merge: invalid JSON payload");
	}
	auto base_root = state.result_has_input ? state.result_doc->root : nullptr;
	auto merged_root = JsonGroupMergeApplyPatchInternal(state.result_doc, base_root, patch_root, 0,
	                                                    state.result_replacements_since_compact, null_treatment);
	if (!merged_root) {
		throw InternalException("json_group_merge: failed to merge JSON documents");
	}
	if (!state.result_has_input || merged_root != state.result_doc->root) {
		yyjson_mut_doc_set_root(state.result_doc, merged_root);
	}
	state.result_has_input = true;
	JsonGroupMergeMaybeCompact(state);
}

static yyjson_mut_val *JsonGroupMergeComposePatchInternal(yyjson_mut_doc *doc, yyjson_mut_val *base, yyjson_val *patch,
                                                          idx_t depth, idx_t &replacements_since_compact,
                                                          JsonNullTreatment null_treatment);

static void JsonGroupMergeComposePatch(JsonGroupMergeState &state, yyjson_val *patch_root,
                                       JsonNullTreatment null_treatment) {
	if (!patch_root) {
		throw InvalidInputException("json_group_merge: invalid JSON payload");
	}
	auto base_root = state.patch_has_input ? state.patch_doc->root : nullptr;
	auto merged_root = JsonGroupMergeComposePatchInternal(state.patch_doc, base_root, patch_root, 0,
	                                                      state.patch_replacements_since_compact, null_treatment);
	if (!merged_root) {
		throw InternalException("json_group_merge: failed to merge JSON documents");
	}
	if (!state.patch_has_input || merged_root != state.patch_doc->root) {
		yyjson_mut_doc_set_root(state.patch_doc, merged_root);
	}
	state.patch_has_input = true;
	JsonGroupMergeMaybeCompact(state);
}

static yyjson_mut_val *JsonGroupMergeApplyPatchInternal(yyjson_mut_doc *doc, yyjson_mut_val *base, yyjson_val *patch,
                                                        idx_t depth, idx_t &replacements_since_compact,
                                                        JsonNullTreatment null_treatment) {
	if (!patch) {
		return base;
	}
	if (depth > MAX_JSON_NESTING_DEPTH) {
		throw InvalidInputException("json_group_merge: nesting depth exceeds maximum limit of " +
		                            std::to_string(MAX_JSON_NESTING_DEPTH));
	}

	if (!duckdb_yyjson::yyjson_is_obj(patch)) {
		auto copy = yyjson_val_mut_copy(doc, patch);
		if (!copy) {
			throw InternalException("json_group_merge: failed to materialize JSON value");
		}
		if (base) {
			replacements_since_compact++;
		}
		return copy;
	}

	yyjson_mut_val *result = nullptr;
	bool base_is_object = base && duckdb_yyjson::yyjson_mut_is_obj(base);
	if (base_is_object) {
		result = base;
	}

	auto EnsureResult = [&]() -> yyjson_mut_val * {
		if (result) {
			return result;
		}
		result = yyjson_mut_obj(doc);
		if (!result) {
			throw InternalException("json_group_merge: failed to allocate JSON object");
		}
		if (base && !base_is_object) {
			replacements_since_compact++;
		}
		return result;
	};

	bool applied_any = false;

	yyjson_val *patch_key = nullptr;
	yyjson_obj_iter patch_iter = yyjson_obj_iter_with(patch);
	while ((patch_key = yyjson_obj_iter_next(&patch_iter))) {
		auto key_str = duckdb_yyjson::yyjson_get_str(patch_key);
		auto key_len = duckdb_yyjson::yyjson_get_len(patch_key);
		auto patch_val = yyjson_obj_iter_get_val(patch_key);

		if (!key_str) {
			throw InvalidInputException("json_group_merge: encountered non-string object key");
		}

		if (duckdb_yyjson::yyjson_is_null(patch_val)) {
			if (null_treatment == JsonNullTreatment::DELETE_NULLS) {
				if (result) {
					auto removed = duckdb_yyjson::yyjson_mut_obj_remove_keyn(result, key_str, key_len);
					if (removed) {
						replacements_since_compact++;
						applied_any = true;
					}
				}
			}
			continue;
		}

		auto existing_child = result ? duckdb_yyjson::yyjson_mut_obj_getn(result, key_str, key_len) : nullptr;
		if (duckdb_yyjson::yyjson_is_obj(patch_val)) {
			auto merged_child = JsonGroupMergeApplyPatchInternal(doc, existing_child, patch_val, depth + 1,
			                                                     replacements_since_compact, null_treatment);
			// Skip if merged_child is null (can happen with IGNORE_NULLS when patch contains only nulls
			// and there's no existing value)
			if (!merged_child) {
				continue;
			}
			if (!existing_child || merged_child != existing_child) {
				auto target_obj = EnsureResult();
				if (existing_child) {
					replacements_since_compact++;
					duckdb_yyjson::yyjson_mut_obj_remove_keyn(target_obj, key_str, key_len);
				}
				auto key_copy = yyjson_mut_strncpy(doc, key_str, key_len);
				if (!key_copy) {
					throw InternalException("json_group_merge: failed to allocate key storage");
				}
				if (!duckdb_yyjson::yyjson_mut_obj_add(target_obj, key_copy, merged_child)) {
					throw InternalException("json_group_merge: failed to append merged object value");
				}
				applied_any = true;
			}
			continue;
		}

		auto new_child = yyjson_val_mut_copy(doc, patch_val);
		if (!new_child) {
			throw InternalException("json_group_merge: failed to copy JSON value");
		}
		if (existing_child) {
			replacements_since_compact++;
			duckdb_yyjson::yyjson_mut_obj_remove_keyn(result, key_str, key_len);
		}
		auto target_obj = EnsureResult();
		auto key_copy = yyjson_mut_strncpy(doc, key_str, key_len);
		if (!key_copy) {
			throw InternalException("json_group_merge: failed to allocate key storage");
		}
		if (!duckdb_yyjson::yyjson_mut_obj_add(target_obj, key_copy, new_child)) {
			throw InternalException("json_group_merge: failed to append merged value");
		}
		applied_any = true;
	}

	// If nothing was applied, return the base unchanged (unless we're at top level with nothing)
	if (!applied_any) {
		// Special case: at top level with no base and empty patch, return empty object
		if (depth == 0 && !base) {
			return EnsureResult();
		}
		return base;
	}

	// Something was applied, ensure we have a result object to return
	return result ? result : EnsureResult();
}

static yyjson_mut_val *JsonGroupMergeComposePatchInternal(yyjson_mut_doc *doc, yyjson_mut_val *base, yyjson_val *patch,
                                                          idx_t depth, idx_t &replacements_since_compact,
                                                          JsonNullTreatment null_treatment) {
	if (!patch) {
		return base;
	}
	if (depth > MAX_JSON_NESTING_DEPTH) {
		throw InvalidInputException("json_group_merge: nesting depth exceeds maximum limit of " +
		                            std::to_string(MAX_JSON_NESTING_DEPTH));
	}
	if (!duckdb_yyjson::yyjson_is_obj(patch)) {
		auto copy = yyjson_val_mut_copy(doc, patch);
		if (!copy) {
			throw InternalException("json_group_merge: failed to materialize JSON value");
		}
		if (base) {
			replacements_since_compact++;
		}
		return copy;
	}

	yyjson_mut_val *result = nullptr;
	bool base_is_object = base && duckdb_yyjson::yyjson_mut_is_obj(base);
	if (base_is_object) {
		result = base;
	}

	auto EnsureResult = [&]() -> yyjson_mut_val * {
		if (result) {
			return result;
		}
		result = yyjson_mut_obj(doc);
		if (!result) {
			throw InternalException("json_group_merge: failed to allocate JSON object");
		}
		if (base && !base_is_object) {
			replacements_since_compact++;
		}
		return result;
	};

	bool applied_any = false;

	yyjson_val *patch_key = nullptr;
	yyjson_obj_iter patch_iter = yyjson_obj_iter_with(patch);
	while ((patch_key = yyjson_obj_iter_next(&patch_iter))) {
		auto key_str = duckdb_yyjson::yyjson_get_str(patch_key);
		auto key_len = duckdb_yyjson::yyjson_get_len(patch_key);
		auto patch_val = yyjson_obj_iter_get_val(patch_key);

		if (!key_str) {
			throw InvalidInputException("json_group_merge: encountered non-string object key");
		}

		if (duckdb_yyjson::yyjson_is_null(patch_val)) {
			if (null_treatment == JsonNullTreatment::IGNORE_NULLS) {
				continue;
			}
			auto target_obj = EnsureResult();
			auto removed = duckdb_yyjson::yyjson_mut_obj_remove_keyn(target_obj, key_str, key_len);
			if (removed) {
				replacements_since_compact++;
			}
			auto key_copy = yyjson_mut_strncpy(doc, key_str, key_len);
			if (!key_copy) {
				throw InternalException("json_group_merge: failed to allocate key storage");
			}
			auto null_value = yyjson_mut_null(doc);
			if (!null_value) {
				throw InternalException("json_group_merge: failed to allocate JSON null value");
			}
			if (!duckdb_yyjson::yyjson_mut_obj_add(target_obj, key_copy, null_value)) {
				throw InternalException("json_group_merge: failed to append merged value");
			}
			applied_any = true;
			continue;
		}

		auto existing_child = result ? duckdb_yyjson::yyjson_mut_obj_getn(result, key_str, key_len) : nullptr;
		if (duckdb_yyjson::yyjson_is_obj(patch_val)) {
			auto merged_child = JsonGroupMergeComposePatchInternal(doc, existing_child, patch_val, depth + 1,
			                                                       replacements_since_compact, null_treatment);
			if (!merged_child) {
				continue;
			}
			if (!existing_child || merged_child != existing_child) {
				auto target_obj = EnsureResult();
				if (existing_child) {
					replacements_since_compact++;
					duckdb_yyjson::yyjson_mut_obj_remove_keyn(target_obj, key_str, key_len);
				}
				auto key_copy = yyjson_mut_strncpy(doc, key_str, key_len);
				if (!key_copy) {
					throw InternalException("json_group_merge: failed to allocate key storage");
				}
				if (!duckdb_yyjson::yyjson_mut_obj_add(target_obj, key_copy, merged_child)) {
					throw InternalException("json_group_merge: failed to append merged object value");
				}
				applied_any = true;
			}
			continue;
		}

		auto new_child = yyjson_val_mut_copy(doc, patch_val);
		if (!new_child) {
			throw InternalException("json_group_merge: failed to copy JSON value");
		}
		auto target_obj = EnsureResult();
		if (existing_child) {
			replacements_since_compact++;
			duckdb_yyjson::yyjson_mut_obj_remove_keyn(target_obj, key_str, key_len);
		}
		auto key_copy = yyjson_mut_strncpy(doc, key_str, key_len);
		if (!key_copy) {
			throw InternalException("json_group_merge: failed to allocate key storage");
		}
		if (!duckdb_yyjson::yyjson_mut_obj_add(target_obj, key_copy, new_child)) {
			throw InternalException("json_group_merge: failed to append merged value");
		}
		applied_any = true;
	}

	if (!applied_any) {
		if (depth == 0 && !base) {
			return EnsureResult();
		}
		return base;
	}

	return result ? result : EnsureResult();
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

	static inline string JsonParseError(const string_t &input, yyjson_read_err &err) {
		return JSONCommon::FormatParseError(input.GetDataUnsafe(), input.GetSize(), err);
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input) {
		static_assert(std::is_same<INPUT_TYPE, string_t>::value, "json_group_merge expects string_t input");
		yyjson_read_err err;
		auto doc = yyjson_read_opts(const_cast<char *>(input.GetDataUnsafe()), input.GetSize(), JSONCommon::READ_FLAG,
		                            nullptr, &err);
		if (!doc) {
			throw InvalidInputException("json_group_merge: %s", JsonParseError(input, err));
		}
		yyjson_doc_ptr patch_doc(doc);
		auto patch_root = yyjson_doc_get_root(patch_doc.get());
		if (!patch_root) {
			throw InvalidInputException("json_group_merge: invalid JSON payload");
		}
		auto null_treatment = GetNullTreatment(unary_input.input.bind_data);
		JsonGroupMergeApplyResultPatch(state, patch_root, null_treatment);
		JsonGroupMergeComposePatch(state, patch_root, null_treatment);
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
		if (!source.patch_has_input || !source.patch_doc || !source.patch_doc->root) {
			return;
		}
		auto source_patch_doc_ptr = yyjson_doc_ptr(yyjson_mut_val_imut_copy(source.patch_doc->root, nullptr));
		if (!source_patch_doc_ptr) {
			throw InternalException("json_group_merge: failed to materialize patch state");
		}
		auto patch_root = yyjson_doc_get_root(source_patch_doc_ptr.get());
		if (!patch_root) {
			throw InternalException("json_group_merge: failed to materialize patch state");
		}
		auto null_treatment = GetNullTreatment(aggr_input_data.bind_data);
		JsonGroupMergeApplyResultPatch(target, patch_root, null_treatment);
		JsonGroupMergeComposePatch(target, patch_root, null_treatment);
	}

	template <class RESULT_TYPE, class STATE>
	static void Finalize(STATE &state, RESULT_TYPE &target, AggregateFinalizeData &finalize_data) {
		if (!state.result_doc || !state.result_doc->root) {
			finalize_data.ReturnNull();
			return;
		}
		size_t output_length = 0;
		auto output_cstr =
		    yyjson_mut_write_opts(state.result_doc, JSONCommon::WRITE_FLAG, nullptr, &output_length, nullptr);
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
	}
	return make_uniq<JsonGroupMergeBindData>(treatment);
}

static constexpr idx_t kAggregatedInputCount = 1;

static void JsonGroupMergeUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
                                 data_ptr_t state, idx_t count) {
	AggregateFunction::UnaryUpdate<JsonGroupMergeState, string_t, JsonGroupMergeFunction>(
	    inputs, aggr_input_data, kAggregatedInputCount, state, count);
}

static void JsonGroupMergeScatterUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
                                        Vector &states, idx_t count) {
	AggregateFunction::UnaryScatterUpdate<JsonGroupMergeState, string_t, JsonGroupMergeFunction>(
	    inputs, aggr_input_data, kAggregatedInputCount, states, count);
}

static AggregateFunction CreateJsonGroupMergeAggregate(const LogicalType &input_type) {
	AggregateFunction function(
	    "json_group_merge", {input_type}, LogicalType::JSON(), AggregateFunction::StateSize<JsonGroupMergeState>,
	    AggregateFunction::StateInitialize<JsonGroupMergeState, JsonGroupMergeFunction>, JsonGroupMergeScatterUpdate,
	    AggregateFunction::StateCombine<JsonGroupMergeState, JsonGroupMergeFunction>,
	    AggregateFunction::StateFinalize<JsonGroupMergeState, string_t, JsonGroupMergeFunction>, JsonGroupMergeUpdate);
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

struct JsonExtractColumnsBindData : public FunctionData {
	JsonExtractColumnsBindData(vector<string> column_names_p, vector<string> patterns_p,
	                           child_list_t<LogicalType> children_p, duckdb_re2::RE2::Options options_p)
	    : column_names(std::move(column_names_p)), patterns(std::move(patterns_p)), children(std::move(children_p)),
	      options(std::move(options_p)) {
		CompilePatterns();
	}

	vector<string> column_names;
	vector<string> patterns;
	child_list_t<LogicalType> children;
	duckdb_re2::RE2::Options options;
	vector<unique_ptr<duckdb_re2::RE2>> compiled_patterns;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<JsonExtractColumnsBindData>(column_names, patterns, children, options);
	}

	bool Equals(const FunctionData &other_p) const override {
		const auto &other = other_p.Cast<JsonExtractColumnsBindData>();
		return column_names == other.column_names && patterns == other.patterns &&
		       options.case_sensitive() == other.options.case_sensitive();
	}

private:
	void CompilePatterns() {
		compiled_patterns.clear();
		compiled_patterns.reserve(patterns.size());
		for (auto &pattern : patterns) {
			auto re = make_uniq<duckdb_re2::RE2>(pattern, options);
			if (!re->ok()) {
				throw BinderException("json_extract_columns: %s", re->error());
			}
			compiled_patterns.push_back(std::move(re));
		}
	}
};

struct JsonExtractColumnsLocalState : public FunctionLocalState {
	JsonExtractColumnsLocalState(Allocator &allocator, idx_t column_count)
	    : json_allocator(std::make_shared<JSONAllocator>(allocator)), buffers(column_count),
	      has_match(column_count, false) {
	}

	shared_ptr<JSONAllocator> json_allocator;
	vector<string> buffers;
	vector<bool> has_match;
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
	if (column_names.empty()) {
		throw BinderException("json_extract_columns: column patterns must not be empty");
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

		yyjson_val *key = nullptr;
		yyjson_obj_iter iter = yyjson_obj_iter_with(root);
		while ((key = yyjson_obj_iter_next(&iter))) {
			auto key_str = duckdb_yyjson::yyjson_get_str(key);
			auto key_len = duckdb_yyjson::yyjson_get_len(key);
			if (!key_str) {
				throw InvalidInputException("json_extract_columns: encountered non-string object key");
			}
			auto value = yyjson_obj_iter_get_val(key);
			duckdb_re2::StringPiece key_piece(key_str, key_len);
			for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
				auto &regex = *bind_data.compiled_patterns[col_idx];
				if (!duckdb_re2::RE2::PartialMatch(key_piece, regex)) {
					continue;
				}
				if (local_state.has_match[col_idx]) {
					local_state.buffers[col_idx].append(separator_data_ptr, separator_len);
				} else {
					local_state.has_match[col_idx] = true;
				}
				AppendJsonValue(local_state.buffers[col_idx], value, alc);
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
			if (depth > 0) {
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
			if (depth > 0) {
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
