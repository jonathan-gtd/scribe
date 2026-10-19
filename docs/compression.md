# Compression, measured

[← README](../README.md) · read on 2026-10-19

Not a benchmark. One Home Assistant instance, the database behind this README,
on Scribe's default settings: 7-day chunks, compressed once they are seven days
old, nothing tuned.

| | |
| :--- | :--- |
| History | 53 147 521 states over 1 948 entities, since 2024-08-19 |
| Written per day | 48 902 states, 0.57 per second |
| Chunks | 114, of which 112 compressed |
| Before compression | 18 GB |
| After compression | 608 MB |
| **Ratio** | **29.7×** |
| Hypertable on disk | **733 MB**, the two uncompressed chunks included |

TimescaleDB measures it itself, so the figure is reproducible on any Scribe
database rather than quoted:

```sql
SELECT pg_size_pretty(before_compression_total_bytes) AS before,
       pg_size_pretty(after_compression_total_bytes)  AS after,
       round(before_compression_total_bytes::numeric
             / after_compression_total_bytes, 1)      AS ratio
FROM hypertable_compression_stats('states_raw');
```

```
 before | after  | ratio
--------+--------+-------
 18 GB  | 608 MB |  29.7
```

## Where the space goes

The total hides three different stories:

| | Before | After |
| :--- | ---: | ---: |
| Table | 11 GB | 64 MB |
| Indexes | 6867 MB | 6760 kB |
| TOAST | 960 kB | 538 MB |
| **Total** | **18 GB** | **608 MB** |

- **The table** holds the states themselves: 11 GB becomes 64 MB of chunk
  skeleton — the data moves into TOAST, compressed.
- **The indexes** mostly stop existing: 6867 MB against 6760 kB, about 1000×.
  A compressed chunk does not need a B-tree on `(metadata_id, time)` — it keeps
  min/max per batch and reads the batch. That collapse is a third of the 18 GB,
  and it is why the overall ratio beats what any single chunk shows.
- **TOAST** is where the compressed batches live, 538 MB of the 608 MB.

## Per state

| | Uncompressed | Compressed |
| :--- | ---: | ---: |
| Bytes per state, indexes included | 260 B | 10.6 B |
| One day, at 48 902 states | 12.7 MB | 0.5 MB |
| One year, at the same rate | 4.6 GB | **189 MB** |

Measured on real chunks of this database: 84 MB for 336 801 states in the live
one, 4200 kB for 404 737 states in the last compressed one.

## Chunk by chunk

```sql
SELECT chunk_name,
       pg_size_pretty(before_compression_total_bytes) AS before,
       pg_size_pretty(after_compression_total_bytes)  AS after,
       round(before_compression_total_bytes::numeric
             / after_compression_total_bytes, 1)      AS ratio
FROM chunk_compression_stats('states_raw')
WHERE after_compression_total_bytes > 0
ORDER BY chunk_name DESC LIMIT 5;
```

```
     chunk_name     | before |  after  | ratio
--------------------+--------+---------+-------
 _hyper_9_497_chunk | 101 MB | 4448 kB |  23.3
 _hyper_9_493_chunk | 88 MB  | 4056 kB |  22.1
 _hyper_9_489_chunk | 86 MB  | 3960 kB |  22.1
 _hyper_9_485_chunk | 89 MB  | 4056 kB |  22.6
 _hyper_9_481_chunk | 93 MB  | 4368 kB |  21.8
```

Across the 112 compressed chunks: **average 28.8×, median 25.9×, from 13.3× to
94.7×**. The spread is the history itself, not the compression — a week where a
handful of entities repeated the same value compresses into almost nothing,
a week of a dozen chatty sensors reporting genuinely different values does not.
The recent chunks sit around 22×, below the overall figure, because the index
collapse above is counted once over the whole hypertable.

## Why it compresses this well

Home Assistant history is the ideal case for columnar compression, and the
database says so. Over 50 000 consecutive states:

```sql
SELECT count(*)                    AS rows,
       count(DISTINCT state)       AS distinct_states,
       count(DISTINCT attributes)  AS distinct_attributes,
       round(avg(pg_column_size(attributes))) AS avg_attr_bytes,
       round(avg(pg_column_size(state)))      AS avg_state_bytes
FROM (SELECT * FROM states_raw
      WHERE time >= now() - interval '2 days' LIMIT 50000) s;
```

```
 rows  | distinct_states | distinct_attributes | avg_attr_bytes | avg_state_bytes
-------+-----------------+---------------------+----------------+-----------------
 50000 |             502 |                2739 |            120 |               8
```

Five hundred distinct values in fifty thousand rows. The `attributes` blob, 120
bytes on average against 8 for the state itself, repeats nearly as much — the
same unit, the same device class, the same friendly name, on every single row.
Stored in columns rather than in rows, that is a dictionary of a few hundred
entries plus an index into it; the timestamps, strictly increasing, become
deltas.

That is also the honest limit of the figure: an instance recording mostly
numeric sensors that genuinely change, with attributes stripped by
`exclude_attributes`, will compress less. One recording a thousand binary
sensors will compress more.

## Compressed does not mean slower to read

The opposite, for the queries a dashboard makes. The same hourly average, over
a compressed week and over the week being written:

```sql
-- the same query, over a compressed week and over the live one
SELECT time_bucket('1 hour', time) AS bucket, avg(state::float)
FROM states_raw
WHERE metadata_id = 31
  AND time >= '2026-10-01' AND time < '2026-10-08'   -- compressed
  AND state ~ '^[0-9.]+$'
GROUP BY 1 ORDER BY 1;
```

| Week | Chunk | Rows read | Time |
| :--- | :--- | ---: | ---: |
| 10-01 → 10-08 | compressed | 12 991 | **0.97 ms** |
| 10-15 → 10-22 | uncompressed | 8 084 | 6.93 ms |

Seven times faster on 1.6× more rows — about 11× per row. A compressed chunk
reads a handful of batches instead of walking 13 000 row versions, and the
columns the query never mentions are never read at all.

What a compressed chunk *is* slower at is being written into. That is the whole
point of `compress_after`: it keeps compression away from the chunk still
receiving states. Rows that arrive late — a backfill, a migration script, a
replay after the database was unreachable — land in an already compressed chunk
and are written to its uncompressed part, then recompressed later.

## Reading it from Home Assistant

With `enable_stats_size: true`, the same three numbers are sensors — *States
Original Size*, *States Compressed Size* and *States Compression Ratio* —
refreshed every `stats_size_interval` minutes, with the chunk counts beside them
under `enable_stats_chunk`. No SQL, and no second connection to the database.
