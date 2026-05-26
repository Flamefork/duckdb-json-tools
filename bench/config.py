from pathlib import Path

BENCH_DIR = Path(__file__).parent
DATA_DIR = BENCH_DIR / "data"
REAL_PROD_DATA_DIR = DATA_DIR / "real_prod"
RESULTS_DIR = BENCH_DIR / "results"
PROFILES_DIR = RESULTS_DIR / "profiles"
PROJECT_ROOT = BENCH_DIR.parent
EXTENSION_PATH = PROJECT_ROOT / "build" / "release" / "extension" / "json_tools" / "json_tools.duckdb_extension"

SIZES = {
    "1k": 1_000,
    "10k": 10_000,
    "100k": 100_000,
}

URL_SIZES = {
    "10k": 10_000,
    "100k": 100_000,
    "1M": 1_000_000,
}

URL_DEFAULT_SIZES = ["10k", "100k", "1M"]

DEFAULT_RUNS = 10
DEFAULT_PROFILE_RUNS = 1
DEFAULT_TOLERANCE_PCT = 5
SCHEMA_VERSION = 5
DEFAULT_MIN_EFFECT_MS = 5.0


# ────────────────────────────────────────────────────────────────────────
# Scenarios — built around the marketing-telemetry shape produced by
# bench/generate_data.py. Patterns target real field groups (utm_*, page_*,
# device_*, geo_*, ec_*, ga_*, custom_event_*, pixel ids).
# ────────────────────────────────────────────────────────────────────────

# Reusable pattern blocks
_CORE_EXACT = {
    "user_id": "^user_id$",
    "event_name": "^event_name$",
    "event_ts": "^event_ts$",
    "session_id": "^session_id$",
    "event_id": "^event_id$",
}

_UTM_EXACT = {
    "utm_source": "^utm_source$",
    "utm_medium": "^utm_medium$",
    "utm_campaign": "^utm_campaign$",
    "utm_content": "^utm_content$",
    "utm_term": "^utm_term$",
}

_PAGE_EXACT = {
    "page_url": "^page_url$",
    "page_path": "^page_path$",
    "page_title": "^page_title$",
    "page_referrer": "^page_referrer$",
}

_DEVICE_EXACT = {
    "device_type": "^device_type$",
    "device_browser": "^device_browser$",
    "device_os": "^device_os$",
    "device_user_agent": "^device_user_agent$",
    "device_language": "^device_language$",
}

_GEO_EXACT = {
    "geo_country": "^geo_country$",
    "geo_region": "^geo_region$",
    "geo_city": "^geo_city$",
}

_EC_REGEX = {
    "ec_revenue": "^ec_revenue$",
    "ec_currency": "^ec_currency$",
    "ec_item_ids": r"^ec_items\[\d+\]\.item_id$",
    "ec_item_prices": r"^ec_items\[\d+\]\.price$",
}


SCENARIOS = [
    # ─── json_flatten ───
    {"function": "json_flatten", "scenario": "basic"},

    # ─── json_add_prefix ───
    {"function": "json_add_prefix", "scenario": "basic"},

    # ─── json_parse_url ───
    {
        "function": "noop_url",
        "scenario": "marketing",
        "data_kind": "urls",
        "sizes": URL_DEFAULT_SIZES,
        "url_column": "url_marketing",
    },
    {
        "function": "json_parse_url",
        "scenario": "short",
        "data_kind": "urls",
        "sizes": URL_DEFAULT_SIZES,
        "url_column": "url_short",
    },
    {
        "function": "json_parse_url",
        "scenario": "marketing",
        "data_kind": "urls",
        "sizes": URL_DEFAULT_SIZES,
        "url_column": "url_marketing",
    },
    {
        "function": "json_parse_url",
        "scenario": "wide_query",
        "data_kind": "urls",
        "sizes": URL_DEFAULT_SIZES,
        "url_column": "url_wide_query",
    },
    {
        "function": "json_parse_url",
        "scenario": "mixed",
        "data_kind": "urls",
        "sizes": URL_DEFAULT_SIZES,
        "url_column": "url_mixed",
    },
    # ─── json_extract_columns ───
    # Smallest realistic ETL: pick 5 core columns
    {
        "function": "json_extract_columns",
        "scenario": "etl_core",
        "patterns": {
            "user_id": "^user_id$",
            "event_name": "^event_name$",
            "utm_source": "^utm_source$",
            "page_url": "^page_url$",
            "geo_country": "^geo_country$",
        },
    },
    # K-crossover sweep: 8 exact patterns (just below current trivial cap K≤12).
    # Used to confirm trivial-mode threshold on realistic shape.
    {
        "function": "json_extract_columns",
        "scenario": "etl_k8",
        "patterns": {
            **_CORE_EXACT,
            "utm_source": "^utm_source$",
            "utm_campaign": "^utm_campaign$",
            "page_url": "^page_url$",
        },
    },
    # K=16 — above current trivial cap (12), confirms general-path baseline.
    {
        "function": "json_extract_columns",
        "scenario": "etl_k16",
        "patterns": {
            **_CORE_EXACT,
            **_UTM_EXACT,
            "page_url": "^page_url$",
            "page_path": "^page_path$",
            "page_title": "^page_title$",
            "device_type": "^device_type$",
            "device_browser": "^device_browser$",
            "device_os": "^device_os$",
        },
    },
    # Full daily-ETL extraction: 25 exact fields covering core + utm + page + device + geo
    {
        "function": "json_extract_columns",
        "scenario": "etl_full",
        "patterns": {**_CORE_EXACT, **_UTM_EXACT, **_PAGE_EXACT, **_DEVICE_EXACT, **_GEO_EXACT},
    },
    # Production-mirror: matches the real event-attribute-rules shape
    # of "few exact + one regex". Lets us measure the realistic mixed case
    # without the spec's regex disqualifying trivial-mode being the headline.
    {
        "function": "json_extract_columns",
        "scenario": "prod_mirror",
        "patterns": {
            "user_id": "^user_id$",
            "event_name": "^event_name$",
            "page_url": "^page_url$",
            "ec_item_ids": r"^ec_items\[\d+\]\.item_id$",
        },
    },
    {
        "function": "json_extract_columns",
        "scenario": "real_prod",
        "size": "245k",
        "data_file": REAL_PROD_DATA_DIR / "events.parquet",
        "json_column": "event_properties::JSON",
        "patterns": {
            "url": "^url$",
            "event_name": "^event_name$",
            "goals": "^goals$",
            "product_category": r"^products.\d+.category$",
        },
    },
    # Prefix bucketing: collapse each field group into a single newline-joined string
    {
        "function": "json_extract_columns",
        "scenario": "prefix_buckets",
        "patterns": {
            "utm_all": "^utm_",
            "page_all": "^page_",
            "device_all": "^device_",
            "geo_all": "^geo_",
            "ga_all": "^ga_",
            "ec_all": "^ec_",
            "custom_all": "^custom_event_",
        },
    },
    # Power-user ETL: ~50 patterns mixing exact + prefix + regex (forces chunked-cache path)
    {
        "function": "json_extract_columns",
        "scenario": "mixed_50",
        "patterns": {
            **_CORE_EXACT,
            **_UTM_EXACT,
            **_PAGE_EXACT,
            **_DEVICE_EXACT,
            **_GEO_EXACT,
            "ga_client_id": "^ga_client_id$",
            "ga_session_id": "^ga_session_id$",
            "ga_user_pseudo_id": "^ga_user_pseudo_id$",
            "fbp": "^fbp$",
            "yandex_uid": "^yandex_uid$",
            "ym_uid": "^ym_uid$",
            "_ga_cookie": "^_ga$",
            # Prefix matches (lit. 7 → 1-many keys each)
            "any_utm": "^utm_",
            "any_page": "^page_",
            "any_device": "^device_",
            "any_geo": "^geo_",
            "any_ec": "^ec_",
            "any_ga": "^ga_",
            "any_custom": "^custom_event_",
            # Regex matches (~5)
            "ec_item_ids": r"^ec_items\[\d+\]\.item_id$",
            "ec_item_prices": r"^ec_items\[\d+\]\.price$",
            "ec_item_qtys": r"^ec_items\[\d+\]\.quantity$",
            "version_fields": r"_version$",
            "id_fields": r"_id$",
        },
    },

    # ─── json_group_merge ───
    {"function": "json_group_merge", "scenario": "few_groups", "group_col": "g1e1", "merge_opts": ""},
    {"function": "json_group_merge", "scenario": "medium_groups", "group_col": "g1e3", "merge_opts": ""},
    {"function": "json_group_merge", "scenario": "many_groups", "group_col": "g1e4", "merge_opts": ""},
    {"function": "json_group_merge", "scenario": "ignore_nulls", "group_col": "g1e3", "merge_opts": ", 'IGNORE NULLS'"},
    {"function": "json_group_merge", "scenario": "delete_nulls", "group_col": "g1e3", "merge_opts": ", 'DELETE NULLS'"},
]
