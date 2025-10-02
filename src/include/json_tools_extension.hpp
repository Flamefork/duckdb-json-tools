#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension.hpp"

#ifndef JSON_TOOLS_EXTENSION_HAS_LOADER
#if __has_include(<duckdb/main/extension/extension_loader.hpp>)
#define JSON_TOOLS_EXTENSION_HAS_LOADER 1
#else
#define JSON_TOOLS_EXTENSION_HAS_LOADER 0
#endif
#endif

namespace duckdb {

class JsonToolsExtension : public Extension {
public:
#if JSON_TOOLS_EXTENSION_HAS_LOADER
	void Load(ExtensionLoader &db) override;
#else
	void Load(DuckDB &db) override;
#endif
	std::string Name() override;
	std::string Version() const override;
};

} // namespace duckdb
