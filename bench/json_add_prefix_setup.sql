PRAGMA enable_progress_bar = FALSE;

INSTALL json;
LOAD json;
LOAD 'json_tools';

CREATE OR REPLACE TEMP MACRO add_prefix_json(v, prefix) AS (
    json_object(
        *
        SELECT prefix || key AS k, value
        FROM json_each(v)
    )
);

CREATE OR REPLACE TABLE bench_events_shallow AS
SELECT json_object(
           'event_id', i,
           'category', format('cat_%s', i % 10),
           'value', i * 0.1,
           'flag', (i % 2) = 0,
           'score', (i % 100) / 100.0
       )::JSON AS payload
FROM range(0, 100000) tbl(i);

CREATE OR REPLACE TABLE bench_events_nested AS
SELECT json_object(
           'event', json_object(
               'id', i,
               'tags', json_array(format('tag_%02s', i % 20), format('tag_%02s', (i + 5) % 20)),
               'metrics', json_object(
                   'count', i % 100,
                   'ratio', (i % 1000) / 1000.0,
                   'nested', json_object(
                       'inner', json_object(
                           'leaf', json_array(i, i + 1, i + 2)
                       )
                   )
               )
           ),
           'payload', json_object(
               'status', CASE WHEN i % 3 = 0 THEN 'ok' WHEN i % 3 = 1 THEN 'warn' ELSE 'error' END,
               'toggle', (i % 2) = 0,
               'variants', json_array(
                   json_object('name', 'alpha', 'value', (i % 7) * 1.0),
                   json_object('name', 'beta', 'value', ((i + 3) % 11) * 1.0)
               ),
               'meta', json_object(
                   'source', format('src_%s', i % 4),
                   'timestamp', to_timestamp(1700000000 + i)
               )
           )
       )::JSON AS payload
FROM range(0, 50000) tbl(i);

CREATE OR REPLACE TABLE bench_arrays_heavy AS
SELECT json_object(
           'id', i,
           'items', json_array(
               json_object('idx', 0, 'value', i),
               json_object('idx', 1, 'value', i + 1,
                                 'extras', json_array(i % 5, (i + 2) % 5, (i + 4) % 5)),
               json_array(i, i + 1, i + 2, i + 3, i + 4, i + 5, i + 6, i + 7, i + 8, i + 9),
               json_object('nested', json_array(
                   json_object('inner', i % 11),
                   json_object('inner', (i + 1) % 11),
                   json_object('inner', (i + 2) % 11)
               )),
               json_object('flags', json_array((i % 2) = 0, (i % 3) = 0, (i % 5) = 0)),
               json_object('summary', json_object('min', i, 'max', i + 50,
                                                            'avg', (i + 25) * 0.5)),
               json_array(format('item_%s', i % 10), format('item_%s', (i + 3) % 10), format('item_%s', (i + 6) % 10))
           ),
           'meta', json_object(
               'source', format('source_%s', i % 6),
               'partition', i % 32,
               'timestamp', to_timestamp(1705000000 + i)
           )
       )::JSON AS payload
FROM range(0, 20000) tbl(i);

CREATE OR REPLACE TABLE bench_worst_case AS
SELECT json_object(
           'lvl0', json_object(
               'lvl1', json_object(
                   'lvl2', json_object(
                       'lvl3', json_object(
                           'lvl4', json_object(
                               'lvl5', json_object(
                                   'lvl6', json_object(
                                       'lvl7', json_object(
                                           'lvl8', json_object(
                                               'lvl9', json_object(
                                                   'lvl10', json_object(
                                                       'leaf', json_array(i, i + 1, i + 2, i + 3),
                                                       'extra', json_object(
                                                           'ids', json_array(i % 7, (i + 1) % 7, (i + 2) % 7),
                                                           'text', format('node_%s', i % 17)
                                                       )
                                                   )
                                               )
                                           )
                                       )
                                   )
                               )
                           )
                       )
                   )
               )
           )
       )::JSON AS payload
FROM range(0, 2000) tbl(i);

CREATE OR REPLACE TABLE bench_events_shallow_long AS
WITH base AS (
    SELECT i, rep_idx, i * 10 + rep_idx AS seq
    FROM range(0, 100000) base(i)
    CROSS JOIN range(0, 10) rep(rep_idx)
)
SELECT json_object(
           'event_id', seq,
           'category', format('cat_%s', seq % 10),
           'value', seq * 0.1,
           'flag', (seq % 2) = 0,
           'score', (seq % 100) / 100.0,
           'repeat_index', rep_idx
       )::JSON AS payload
FROM base;

CREATE OR REPLACE TABLE bench_events_nested_long AS
WITH base AS (
    SELECT i, rep_idx, i * 10 + rep_idx AS seq
    FROM range(0, 50000) base(i)
    CROSS JOIN range(0, 10) rep(rep_idx)
)
SELECT json_object(
           'event', json_object(
               'id', seq,
               'tags', json_array(format('tag_%02s', seq % 20), format('tag_%02s', (seq + 5) % 20)),
               'metrics', json_object(
                   'count', seq % 100,
                   'ratio', (seq % 1000) / 1000.0,
                   'nested', json_object(
                       'inner', json_object(
                           'leaf', json_array(seq, seq + 1, seq + 2),
                           'repeat_index', rep_idx
                       )
                   )
               )
           ),
           'payload', json_object(
               'status', CASE WHEN seq % 3 = 0 THEN 'ok' WHEN seq % 3 = 1 THEN 'warn' ELSE 'error' END,
               'toggle', (seq % 2) = 0,
               'variants', json_array(
                   json_object('name', 'alpha', 'value', (seq % 7) * 1.0),
                   json_object('name', 'beta', 'value', ((seq + 3) % 11) * 1.0)
               ),
               'meta', json_object(
                   'source', format('src_%s', seq % 4),
                   'timestamp', to_timestamp(1700000000 + seq),
                   'repeat_index', rep_idx
               )
           )
       )::JSON AS payload
FROM base;

CREATE OR REPLACE TABLE bench_arrays_heavy_long AS
WITH base AS (
    SELECT i, rep_idx, i * 25 + rep_idx AS seq
    FROM range(0, 20000) base(i)
    CROSS JOIN range(0, 25) rep(rep_idx)
)
SELECT json_object(
           'id', seq,
           'items', json_array(
               json_object('idx', 0, 'value', seq),
               json_object('idx', 1, 'value', seq + 1,
                                 'extras', json_array(seq % 5, (seq + 2) % 5, (seq + 4) % 5)),
               json_array(seq, seq + 1, seq + 2, seq + 3, seq + 4, seq + 5, seq + 6, seq + 7, seq + 8, seq + 9),
               json_object('nested', json_array(
                   json_object('inner', seq % 11),
                   json_object('inner', (seq + 1) % 11),
                   json_object('inner', (seq + 2) % 11)
               )),
               json_object('flags', json_array((seq % 2) = 0, (seq % 3) = 0, (seq % 5) = 0)),
               json_object('summary', json_object('min', seq, 'max', seq + 50,
                                                            'avg', (seq + 25) * 0.5,
                                                            'repeat_index', rep_idx)),
               json_array(format('item_%s', seq % 10), format('item_%s', (seq + 3) % 10), format('item_%s', (seq + 6) % 10))
           ),
           'meta', json_object(
               'source', format('source_%s', seq % 6),
               'partition', seq % 32,
               'timestamp', to_timestamp(1705000000 + seq),
               'repeat_index', rep_idx
           )
       )::JSON AS payload
FROM base;
