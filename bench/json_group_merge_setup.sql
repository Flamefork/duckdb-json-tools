PRAGMA enable_progress_bar = FALSE;

INSTALL json;
LOAD json;
LOAD 'json_tools';

CREATE OR REPLACE TABLE bench_sessions_shallow AS
WITH base AS (
    SELECT
        session_id,
        event_index,
        session_id * 1000 + event_index AS seq,
        to_timestamp(1700000000 + session_id * 1000 + event_index) AS event_ts,
        (event_index % 20 = 0) AS mark_null
    FROM range(0, 2000) session(session_id)
    CROSS JOIN range(0, 50) events(event_index)
)
SELECT
    session_id AS group_id,
    event_ts,
    seq,
    CASE
        WHEN mark_null THEN NULL
        ELSE json_object(
            'last_page', format('page_%s', seq % 32),
            'cart_value', (seq % 500) * 1.0,
            'site', format('site_%s', session_id % 5),
            'flags', json_array((seq % 2) = 0, (seq % 3) = 0, (seq % 5) = 0)
        )
    END::JSON AS patch
FROM base;

CREATE OR REPLACE TABLE bench_sessions_shallow_long AS
WITH base AS (
    SELECT
        session_id,
        event_index,
        session_id * 1000 + event_index AS seq,
        to_timestamp(1705000000 + session_id * 1000 + event_index) AS event_ts,
        (event_index % 25 = 0) AS mark_null
    FROM range(0, 10000) session(session_id)
    CROSS JOIN range(0, 100) events(event_index)
)
SELECT
    session_id AS group_id,
    event_ts,
    seq,
    CASE
        WHEN mark_null THEN NULL
        ELSE json_object(
            'last_page', format('page_%s', seq % 64),
            'cart_value', (seq % 750) * 1.0,
            'site', format('site_%s', session_id % 11),
            'flags', json_array((seq % 2) = 0, (seq % 3) = 0, (seq % 7) = 0),
            'attributes', json_object(
                'from_campaign', (seq % 6) = 0,
                'ab_bucket', format('bucket_%s', seq % 4)
            )
        )
    END::JSON AS patch
FROM base;

CREATE OR REPLACE TABLE bench_customers_nested AS
WITH base AS (
    SELECT
        customer_id,
        event_index,
        customer_id * 100 + event_index AS seq,
        to_timestamp(1710000000 + customer_id * 100 + event_index) AS event_ts,
        (seq % 137 = 0) AS mark_null
    FROM range(0, 6000) customer(customer_id)
    CROSS JOIN range(0, 40) events(event_index)
)
SELECT
    customer_id AS group_id,
    event_ts,
    seq,
    CASE
        WHEN mark_null THEN NULL
        ELSE json_object(
            'profile', json_object(
                'id', customer_id,
                'segment', format('segment_%s', customer_id % 7),
                'stats', json_object(
                    'lifetime_value', seq % 1000,
                    'tier', CASE
                        WHEN seq % 3 = 0 THEN 'gold'
                        WHEN seq % 3 = 1 THEN 'silver'
                        ELSE 'bronze'
                    END,
                    'nested', json_object(
                        'prefs', json_object(
                            'email_opt_in', (seq % 2) = 0,
                            'sms_opt_in', (seq % 5) = 0
                        ),
                        'tags', json_array(
                            format('tag_%02s', seq % 20),
                            format('tag_%02s', (seq + 7) % 20)
                        )
                    )
                )
            ),
            'event', json_object(
                'type', CASE
                    WHEN event_index % 4 = 0 THEN 'view'
                    WHEN event_index % 4 = 1 THEN 'click'
                    WHEN event_index % 4 = 2 THEN 'purchase'
                    ELSE 'refund'
                END,
                'amount', (seq % 500) * 0.85,
                'metadata', json_object(
                    'region', format('region_%s', customer_id % 10),
                    'device', format('device_%s', event_index % 5)
                )
            )
        )
    END::JSON AS patch
FROM base;

CREATE OR REPLACE TABLE bench_arrays_replace AS
WITH base AS (
    SELECT
        stream_id,
        event_index,
        stream_id * 200 + event_index AS seq,
        to_timestamp(1715000000 + stream_id * 200 + event_index) AS event_ts,
        (seq % 193 = 0) AS mark_null
    FROM range(0, 4000) stream(stream_id)
    CROSS JOIN range(0, 40) events(event_index)
)
SELECT
    stream_id AS group_id,
    event_ts,
    seq,
    CASE
        WHEN mark_null THEN NULL
        ELSE json_object(
            'items', json_array(
                json_array(seq % 10, (seq + 1) % 10, (seq + 2) % 10),
                json_array(
                    format('item_%s', seq % 15),
                    format('item_%s', (seq + 5) % 15),
                    format('item_%s', (seq + 9) % 15)
                ),
                json_object(
                    'metrics', json_array(
                        json_object('name', 'latency', 'value', (seq % 200) * 0.1),
                        json_object('name', 'errors', 'value', seq % 7)
                    )
                )
            ),
            'metadata', json_object(
                'shard', stream_id % 16,
                'tick', event_index,
                'flags', json_array((seq % 2) = 0, (seq % 3) = 0)
            )
        )
    END::JSON AS patch
FROM base;

CREATE OR REPLACE TABLE bench_scalars_mix AS
WITH base AS (
    SELECT
        account_id,
        change_index,
        account_id * 200 + change_index AS seq,
        to_timestamp(1720000000 + account_id * 200 + change_index) AS event_ts
    FROM range(0, 5000) account(account_id)
    CROSS JOIN range(0, 30) changes(change_index)
)
SELECT
    account_id AS group_id,
    event_ts,
    seq,
    json_object(
        'status', CASE
            WHEN seq % 4 = 0 THEN 'active'
            WHEN seq % 4 = 1 THEN 'suspended'
            WHEN seq % 4 = 2 THEN 'paused'
            ELSE 'closed'
        END,
        'score', seq % 1000,
        'limits', json_object(
            'daily', (seq % 500) * 1.0,
            'monthly', (seq % 1000) * 2.5
        ),
        'quota_remaining', greatest(0, 100000 - seq),
        'note', format('audit_%s', seq % 97)
    )::JSON AS patch
FROM base;

CREATE OR REPLACE TABLE bench_large_docs AS
WITH base AS (
    SELECT
        project_id,
        revision,
        project_id * 100 + revision AS seq,
        to_timestamp(1725000000 + project_id * 100 + revision) AS event_ts
    FROM range(0, 800) project(project_id)
    CROSS JOIN range(0, 40) revisions(revision)
)
SELECT
    project_id AS group_id,
    event_ts,
    seq,
    json_object(
        'doc', json_object(
            'chunk', format('chunk_%03d', revision),
            'payload', repeat(format('payload_%s;', seq % 17), 50),
            'version', revision,
            'author', format('user_%s', project_id % 20)
        ),
        'audit', json_object(
            'timestamp', event_ts,
            'checksum', format('hash_%s', seq)
        ),
        'labels', json_array(
            format('label_%s', seq % 13),
            format('label_%s', (seq + 5) % 13),
            format('label_%s', (seq + 9) % 13)
        )
    )::JSON AS patch
FROM base;

CREATE OR REPLACE TABLE bench_sessions_heavy_docs AS
-- Heavy scenario knobs: adjust the constants below to resize key count, depth, or repeat factors.
WITH params AS (
    SELECT
        160 AS top_level_keys,
        4 AS complexity_tiers,
        4 AS nested_repeat_count
),
base AS (
    SELECT
        session_id,
        event_index,
        session_id * 2000 + event_index AS seq,
        to_timestamp(1730000000 + session_id * 2000 + event_index) AS event_ts,
        (event_index % 48 = 0) AS mark_null
    FROM range(0, 512) session(session_id)
    CROSS JOIN range(0, 80) events(event_index)
),
heavy_payloads AS (
    SELECT
        b.session_id,
        b.event_ts,
        b.seq,
        b.event_index,
        b.mark_null,
        json_group_object(
            printf('field_%03d', attr_idx),
            CASE
                WHEN attr_idx % 5 = 0 THEN json_object(
                    'counters', json_object(
                        'seq_mod', b.seq % 4096,
                        'attr_idx', attr_idx,
                        'rolling', (b.seq / (attr_idx + 1))::BIGINT
                    ),
                    'flags', json_array(
                        (b.seq + attr_idx) % 2 = 0,
                        (b.session_id + attr_idx) % 3 = 0,
                        (b.event_index + attr_idx) % 5 = 0
                    ),
                    'labels', json_array(
                        printf('session_%03d', b.session_id % 128),
                        printf('event_%03d', b.event_index % 256),
                        printf('attr_%03d', attr_idx)
                    )
                )
                WHEN attr_idx % 5 = 1 THEN json_array(
                    json_object(
                        'type', 'metric',
                        'name', printf('m_%03d', attr_idx),
                        'value', (b.seq % 100000) * 0.0001
                    ),
                    json_object(
                        'type', 'window',
                        'start', (b.event_ts::TIMESTAMP) - (attr_idx % 6) * INTERVAL '15 minutes',
                        'end', b.event_ts::TIMESTAMP
                    ),
                    json_object(
                        'type', 'breakdown',
                        'bins', to_json(
                            list_transform(range(0, params.nested_repeat_count), i ->
                                struct_pack(
                                    bucket := printf('bin_%02d', (attr_idx + i) % 64),
                                    hits := (b.seq + i * (attr_idx + 1)) % 8192,
                                    active := ((b.seq + attr_idx + i) % 3) = 0
                                )
                            )
                        )
                    )
                )
                WHEN attr_idx % 5 = 2 THEN json_object(
                    'state', json_object(
                        'level1', json_object(
                            'level2', json_object(
                                'level3', json_object(
                                    'level4', json_object(
                                        'checksum', printf('chk_%08x', b.seq * 131 + attr_idx * 17),
                                        'updated_at', b.event_ts::TIMESTAMP,
                                        'tier', CASE
                                            WHEN attr_idx % params.complexity_tiers = 0 THEN 'platinum'
                                            WHEN attr_idx % params.complexity_tiers = 1 THEN 'gold'
                                            WHEN attr_idx % params.complexity_tiers = 2 THEN 'silver'
                                            ELSE 'bronze'
                                        END
                                    ),
                                    'guards', json_array(
                                        (attr_idx + 1) % 2 = 0,
                                        (attr_idx + b.session_id) % 3 = 0,
                                        (attr_idx + b.event_index) % 5 = 0
                                    )
                                )
                            )
                        )
                    )
                )
                WHEN attr_idx % 5 = 3 THEN to_json(
                    list_transform(range(0, params.complexity_tiers * 2), lvl ->
                        struct_pack(
                            layer := lvl,
                            time_window := struct_pack(
                                start := (b.event_ts::TIMESTAMP) - lvl * INTERVAL '10 minutes',
                                finish := b.event_ts::TIMESTAMP
                            ),
                            samples := list_transform(range(0, params.nested_repeat_count), s ->
                                struct_pack(
                                    idx := s,
                                    score := ((b.seq + attr_idx + lvl * 7 + s) % 1000) * 0.01,
                                    tag := printf('s_%02d', (attr_idx + s) % 80)
                                )
                            )
                        )
                    )
                )
                ELSE json_object(
                    'meta', json_object(
                        'session_key', printf('sess_%05d', b.session_id),
                        'event_key', printf('evt_%05d', b.event_index),
                        'carrier', printf('carrier_%02d', (b.session_id + attr_idx) % 17),
                        'complexity', params.complexity_tiers
                    ),
                    'overrides', json_array(
                        json_object(
                            'path', printf('state.field_%03d.threshold', attr_idx),
                            'value', (b.seq + attr_idx) % 1024
                        ),
                        json_object(
                            'path', 'metrics.primary',
                            'value', (b.seq * (attr_idx + 1)) % 2048
                        )
                    ),
                    'notes', printf('note_%03d_%03d', b.seq % 1000, attr_idx)
                )
            END
        ) AS payload
    FROM base b
    CROSS JOIN params
    CROSS JOIN range(0, params.top_level_keys) attr(attr_idx)
    GROUP BY b.session_id, b.event_ts, b.seq, b.event_index, b.mark_null
)
SELECT
    session_id AS group_id,
    event_ts,
    seq,
    CASE
        WHEN mark_null THEN NULL
        ELSE payload
    END::JSON AS patch
FROM heavy_payloads;

ANALYZE;
