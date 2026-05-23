#!/usr/bin/env python3
"""Generate realistic marketing-telemetry-shaped JSON events for benchmarks.

Each row models a single web/app tracking event in the style of Segment,
GA4, or Snowplow payloads: a mix of attribution (utm_*), page context,
device fingerprint, geo, pixel ids, ecommerce, and sparse custom params.
"""
import argparse
import json
import random
import uuid
from datetime import datetime, timezone

import duckdb

from config import DATA_DIR, SIZES


# ────────────────────────────────────────────────────────────────────────
# Value pools (realistic but bounded for repeatability)
# ────────────────────────────────────────────────────────────────────────

UTM_SOURCES = ["google", "facebook", "instagram", "yandex", "vk", "tiktok",
               "email", "direct", "bing", "twitter", "youtube", "linkedin"]
UTM_MEDIUMS = ["cpc", "organic", "social", "email", "referral", "display",
               "video", "affiliate", "push"]
UTM_CAMPAIGNS = [f"{theme}_{year}_{i:02d}"
                 for theme in ["spring_sale", "summer_promo", "black_friday",
                               "newyear", "back_to_school", "valentines",
                               "brand_awareness", "retargeting"]
                 for year in ["2023", "2024", "2025"]
                 for i in range(1, 6)]
UTM_CONTENTS = ["banner_top", "banner_side", "card_a", "card_b", "video_pre",
                "email_cta", "popup", "footer_link", "menu_promo", None]
UTM_TERMS = ["buy_shoes", "best_laptop", "cheap_flights", "ai_tools",
             "online_course", "summer_dress", None, None, None]

DOMAINS = ["example.com", "shop.example.com", "blog.example.com",
           "promo.partner.com", "app.startup.io"]
PAGE_PATHS = ["/", "/products", "/products/shoes", "/products/laptops",
              "/cart", "/checkout", "/blog/seo-tips", "/about", "/contact",
              "/account", "/account/orders", "/search?q=running"]
PAGE_TITLES = ["Home – Example Store", "All Products", "Running Shoes – Best Sellers",
               "Laptops Under $1000", "Your Cart", "Secure Checkout",
               "SEO Tips for 2025", "About Us", "Contact Support",
               "My Account", "Order History"]
REFERRERS = [None, None, None,
             "https://www.google.com/search?q=shoes",
             "https://www.google.com/", "https://yandex.ru/",
             "https://m.facebook.com/", "https://t.me/somechannel",
             "https://news.ycombinator.com/", "https://twitter.com/"]

DEVICE_TYPES = ["mobile", "desktop", "tablet"]
BROWSERS = ["Chrome", "Safari", "Firefox", "Edge", "Yandex", "Opera"]
OSES = ["iOS", "Android", "Windows", "macOS", "Linux"]
SCREENS = ["390x844", "414x896", "375x667", "1920x1080", "2560x1440",
           "1440x900", "1366x768", "768x1024"]
LANGUAGES = ["en-US", "ru-RU", "es-ES", "de-DE", "fr-FR", "pt-BR", "ja-JP", "zh-CN"]
UA_TEMPLATES = [
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/121.0",
]

COUNTRIES = ["US", "RU", "DE", "FR", "BR", "JP", "GB", "CA", "AU", "IN", "ES", "IT"]
REGIONS = {"US": ["California", "New York", "Texas", "Florida"],
           "RU": ["Moscow", "St. Petersburg", "Sverdlovsk", "Tatarstan"],
           "DE": ["Berlin", "Bavaria", "Hesse"], "FR": ["Île-de-France", "Provence"],
           "BR": ["São Paulo", "Rio de Janeiro"], "JP": ["Tokyo", "Osaka"],
           "GB": ["England", "Scotland"], "CA": ["Ontario", "Quebec"]}
CITIES = {"California": ["San Francisco", "Los Angeles", "San Jose"],
          "New York": ["New York City", "Buffalo"], "Moscow": ["Moscow"],
          "Berlin": ["Berlin"], "Tokyo": ["Tokyo"]}

EVENT_NAMES = ["page_view", "click", "scroll", "form_submit", "purchase",
               "add_to_cart", "remove_from_cart", "view_item", "sign_up",
               "login", "search", "share", "video_play", "video_complete"]
CURRENCIES = ["USD", "EUR", "RUB", "JPY", "BRL", "GBP"]

CUSTOM_KEYS = ["button_text", "form_id", "experiment_variant", "ab_test",
               "section_id", "video_id", "video_position", "search_query",
               "filter_applied", "sort_order", "page_depth", "scroll_pct"]

PIXEL_IDS = {
    "fbp": lambda r: f"fb.1.{r.randint(1_000_000_000, 9_999_999_999)}.{r.randint(100, 999)}",
    "yandex_uid": lambda r: str(r.randint(1_000_000_000, 9_999_999_999)),
    "ym_uid": lambda r: str(r.randint(1_000_000_000, 9_999_999_999)),
    "_ga": lambda r: f"GA1.2.{r.randint(100_000_000, 999_999_999)}.{r.randint(1_000_000_000, 9_999_999_999)}",
}

ITEM_NAMES = ["Running Shoes", "Wireless Headphones", "Laptop Stand",
              "Coffee Mug", "Notebook", "USB Cable", "T-Shirt", "Jeans",
              "Sunglasses", "Backpack"]


# ────────────────────────────────────────────────────────────────────────
# Field-group probabilities (per event)
# ────────────────────────────────────────────────────────────────────────

P_UTM = 0.70          # has any utm_* fields
P_UTM_CONTENT = 0.40  # within utm-having events, has utm_content
P_UTM_TERM = 0.30
P_PAGE = 0.80         # has page_*
P_REFERRER = 0.55     # within page-having
P_DEVICE = 0.95
P_GEO = 0.90
P_GEO_REGION = 0.75   # geo+region
P_GEO_CITY = 0.60
P_GA = 0.60
P_EC = 0.15           # event is ecommerce
P_EC_ITEMS = 0.80     # within ec events, has line items
P_CUSTOM = 0.30
N_CUSTOM_MIN = 3      # custom field count when present
N_CUSTOM_MAX = 8
P_PIXEL = 0.40        # has any pixel ids
N_PIXEL_MIN = 1
N_PIXEL_MAX = 3


def event_id(seed: int, row_idx: int) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{seed}:event:{row_idx}"))


def user_id_for(seed: int, row_idx: int) -> str:
    # Cluster users: ~100K distinct users across data, repeated visits
    user_bucket = row_idx % max(1, 1 + row_idx // 5)
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{seed}:user:{user_bucket}"))


def session_id_for(seed: int, row_idx: int) -> str:
    session_bucket = row_idx // 5  # ~5 events per session
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{seed}:session:{session_bucket}"))


def make_rng(seed: int, row_idx: int) -> random.Random:
    return random.Random(f"{seed}:{row_idx}")


def make_page_url(rng: random.Random, path: str) -> str:
    """Build a realistic-looking page_url with query string sometimes."""
    domain = rng.choice(DOMAINS)
    qs = ""
    if rng.random() < 0.40:
        # Add some tracking params to query string
        params = []
        if rng.random() < 0.7:
            params.append(f"utm_source={rng.choice(UTM_SOURCES)}")
        if rng.random() < 0.5:
            params.append(f"utm_campaign={rng.choice(UTM_CAMPAIGNS)}")
        if rng.random() < 0.3:
            params.append(f"ref={rng.randint(100, 9999)}")
        if params:
            qs = "?" + "&".join(params)
    return f"https://{domain}{path}{qs}"


def generate_event(seed: int, row_idx: int) -> dict:
    """Generate a single realistic web telemetry event."""
    rng = make_rng(seed, row_idx)
    e: dict = {}

    # Core (always present)
    e["event_id"] = event_id(seed, row_idx)
    e["event_name"] = rng.choice(EVENT_NAMES)
    # Spread timestamps over ~30 days
    e["event_ts"] = 1_716_000_000 + rng.randint(0, 2_592_000)
    e["user_id"] = user_id_for(seed, row_idx)
    e["session_id"] = session_id_for(seed, row_idx)

    # UTM
    if rng.random() < P_UTM:
        e["utm_source"] = rng.choice(UTM_SOURCES)
        e["utm_medium"] = rng.choice(UTM_MEDIUMS)
        e["utm_campaign"] = rng.choice(UTM_CAMPAIGNS)
        if rng.random() < P_UTM_CONTENT:
            v = rng.choice(UTM_CONTENTS)
            if v is not None:
                e["utm_content"] = v
        if rng.random() < P_UTM_TERM:
            v = rng.choice(UTM_TERMS)
            if v is not None:
                e["utm_term"] = v

    # Page
    if rng.random() < P_PAGE:
        path = rng.choice(PAGE_PATHS)
        e["page_url"] = make_page_url(rng, path)
        e["page_path"] = path
        e["page_title"] = rng.choice(PAGE_TITLES)
        if rng.random() < P_REFERRER:
            v = rng.choice(REFERRERS)
            if v is not None:
                e["page_referrer"] = v

    # Device
    if rng.random() < P_DEVICE:
        e["device_type"] = rng.choice(DEVICE_TYPES)
        e["device_browser"] = rng.choice(BROWSERS)
        e["device_browser_version"] = f"{rng.randint(80, 125)}.0"
        e["device_os"] = rng.choice(OSES)
        e["device_os_version"] = f"{rng.randint(10, 17)}.{rng.randint(0, 5)}"
        e["device_user_agent"] = rng.choice(UA_TEMPLATES)
        e["device_screen"] = rng.choice(SCREENS)
        e["device_language"] = rng.choice(LANGUAGES)

    # Geo
    if rng.random() < P_GEO:
        country = rng.choice(COUNTRIES)
        e["geo_country"] = country
        if rng.random() < P_GEO_REGION and country in REGIONS:
            region = rng.choice(REGIONS[country])
            e["geo_region"] = region
            if rng.random() < P_GEO_CITY and region in CITIES:
                e["geo_city"] = rng.choice(CITIES[region])

    # GA pixels
    if rng.random() < P_GA:
        e["ga_client_id"] = f"{rng.randint(1_000_000_000, 9_999_999_999)}.{rng.randint(1_000_000_000, 9_999_999_999)}"
        e["ga_session_id"] = str(rng.randint(1_700_000_000, 1_750_000_000))
        if rng.random() < 0.6:
            e["ga_user_pseudo_id"] = str(rng.randint(1_000_000_000, 9_999_999_999))

    # Ecommerce (sparse)
    if rng.random() < P_EC:
        e["ec_currency"] = rng.choice(CURRENCIES)
        e["ec_revenue"] = round(rng.uniform(5, 500), 2)
        e["ec_transaction_id"] = f"txn_{rng.randint(100000, 999999)}"
        if rng.random() < P_EC_ITEMS:
            n_items = rng.randint(1, 4)
            e["ec_items"] = [
                {
                    "item_id": f"sku_{rng.randint(1000, 9999)}",
                    "item_name": rng.choice(ITEM_NAMES),
                    "price": round(rng.uniform(5, 200), 2),
                    "quantity": rng.randint(1, 3),
                }
                for _ in range(n_items)
            ]

    # Custom event params (sparse, varying field set)
    if rng.random() < P_CUSTOM:
        n_custom = rng.randint(N_CUSTOM_MIN, N_CUSTOM_MAX)
        for k in rng.sample(CUSTOM_KEYS, k=min(n_custom, len(CUSTOM_KEYS))):
            if k in ("video_position", "page_depth", "scroll_pct"):
                e[f"custom_event_{k}"] = rng.randint(0, 100)
            else:
                e[f"custom_event_{k}"] = f"val_{rng.randint(0, 50)}"

    # Pixel ids
    if rng.random() < P_PIXEL:
        n_pix = rng.randint(N_PIXEL_MIN, min(N_PIXEL_MAX, len(PIXEL_IDS)))
        for pix_key in rng.sample(list(PIXEL_IDS.keys()), k=n_pix):
            e[pix_key] = PIXEL_IDS[pix_key](rng)

    return e


def flatten_event(event: dict) -> dict:
    """Flatten one level of nesting (ec_items[i].*, user_properties.*).

    Top-level keys stay as-is; nested objects/arrays get path keys with
    bracket-index for arrays and dot for object members. Mirrors how
    json_extract_columns is fed in production (typically already-flattened
    payloads or one-level-nested ones).
    """
    out: dict = {}
    for k, v in event.items():
        if isinstance(v, dict):
            for sub_k, sub_v in v.items():
                out[f"{k}.{sub_k}"] = sub_v
        elif isinstance(v, list):
            for i, item in enumerate(v):
                if isinstance(item, dict):
                    for sub_k, sub_v in item.items():
                        out[f"{k}[{i}].{sub_k}"] = sub_v
                else:
                    out[f"{k}[{i}]"] = item
        else:
            out[k] = v
    return out


def generate_row(row_idx: int, seed: int) -> dict:
    event = generate_event(seed, row_idx)
    flat = flatten_event(event)
    return {
        "json_nested": json.dumps(event, separators=(",", ":")),
        "json_flat": json.dumps(flat, separators=(",", ":")),
        # Group keys for json_group_merge: vary cardinality from 10 to 10k
        "g1e1": f"g_{row_idx % 10}",
        "g1e3": f"g_{row_idx % 1000}",
        "g1e4": f"g_{row_idx % 10000}",
    }


def generate_data(size_name: str, num_rows: int, seed: int) -> None:
    print(f"Generating {size_name} ({num_rows:,} rows) with seed={seed}...")

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    output_path = DATA_DIR / f"events_{size_name}.parquet"

    conn = duckdb.connect()

    batch_size = 50_000
    rows_generated = 0
    temp_table_created = False

    while rows_generated < num_rows:
        batch_rows = min(batch_size, num_rows - rows_generated)
        batch = [generate_row(rows_generated + i, seed) for i in range(batch_rows)]

        conn.execute(
            """
            CREATE OR REPLACE TEMP TABLE batch (
                json_nested JSON,
                json_flat JSON,
                g1e1 VARCHAR,
                g1e3 VARCHAR,
                g1e4 VARCHAR
            )
            """
        )
        conn.executemany(
            "INSERT INTO batch VALUES (?, ?, ?, ?, ?)",
            [(r["json_nested"], r["json_flat"], r["g1e1"], r["g1e3"], r["g1e4"]) for r in batch],
        )

        if not temp_table_created:
            conn.execute("CREATE OR REPLACE TABLE data AS SELECT * FROM batch")
            temp_table_created = True
        else:
            conn.execute("INSERT INTO data SELECT * FROM batch")

        rows_generated += batch_rows
        print(f"  {rows_generated:,} / {num_rows:,}")

    generated_at = datetime.now(timezone.utc).isoformat()
    conn.execute(
        f"""
        COPY data TO '{output_path}' (
            FORMAT PARQUET,
            KV_METADATA {{
                seed: '{seed}',
                size: '{size_name}',
                rows: '{num_rows}',
                generated_at: '{generated_at}',
                schema_version: 'marketing_telemetry_v1'
            }}
        )
        """
    )
    conn.close()

    print(f"  Saved to {output_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate marketing-telemetry benchmark data")
    parser.add_argument(
        "--size",
        choices=list(SIZES.keys()),
        help="Generate only this size (default: all)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for deterministic generation (default: 42)",
    )
    args = parser.parse_args()

    if args.size:
        generate_data(args.size, SIZES[args.size], args.seed)
    else:
        for size_name, num_rows in SIZES.items():
            generate_data(size_name, num_rows, args.seed)


if __name__ == "__main__":
    main()
