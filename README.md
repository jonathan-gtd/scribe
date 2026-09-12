<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="brands_assets/dark_logo.png">
  <img src="brands_assets/logo.png" alt="Scribe" width="300">
</picture>

### Home Assistant history in TimescaleDB

Every state and every event, through `asyncpg` — without blocking the event loop.

[![Release](https://img.shields.io/github/v/release/jonathan-gtd/scribe?color=41BDF5)](https://github.com/jonathan-gtd/scribe/releases/latest) [![Downloads](https://img.shields.io/github/downloads/jonathan-gtd/scribe/total?color=41BDF5)](https://github.com/jonathan-gtd/scribe/releases) [![Tests](https://img.shields.io/github/actions/workflow/status/jonathan-gtd/scribe/tests.yaml?branch=master&label=tests)](https://github.com/jonathan-gtd/scribe/actions/workflows/tests.yaml) [![License](https://img.shields.io/github/license/jonathan-gtd/scribe?color=lightgrey)](LICENSE)

[![lang en](https://img.shields.io/badge/lang-en-41BDF5)](README.md) [![lang fr](https://img.shields.io/badge/lang-fr-lightgrey)](README.fr.md) [![lang es](https://img.shields.io/badge/lang-es-lightgrey)](README.es.md) [![lang de](https://img.shields.io/badge/lang-de-lightgrey)](README.de.md) [![lang nl](https://img.shields.io/badge/lang-nl-lightgrey)](README.nl.md)

</div>

---

Home Assistant's recorder keeps a few weeks of history in SQLite and slows down as it grows. Scribe writes the same states and events to **TimescaleDB**, where years stay fast and take a fraction of the space.

- 🚀 **Asynchronous end to end** — `asyncpg` and batched `COPY`: recording never blocks Home Assistant.
- 🗜️ **Compressed automatically** — old history is chunked and compressed, typically 10× smaller.
- 🛟 **Nothing is lost** — a database that is down is buffered, then written when it returns.
- 🧩 **Context included** — entities, devices, areas, users and integrations, not only values.
- 🩺 **It says when something is wrong**, in Repairs rather than in a log nobody reads.

---

## Install

**1. A TimescaleDB database.** The extension is required — see *Setting up TimescaleDB* below.

**2. Scribe, through HACS:**

[![Open your Home Assistant instance and open a repository inside the Home Assistant Community Store.](https://my.home-assistant.io/badges/supervisor_add_addon_repository.svg)](https://my.home-assistant.io/redirect/hacs_repository/?owner=jonathan-gtd&repository=scribe&category=integration)

*Or by hand:* copy `custom_components/scribe` into your `custom_components` folder. Either way, restart Home Assistant.

**3. The database URL.** Go to **Settings → Devices & services → Add integration**, search for **Scribe**, and paste it:

[![Open your Home Assistant instance and start setting up a new integration.](https://my.home-assistant.io/badges/config_flow_start.svg)](https://my.home-assistant.io/redirect/config_flow_start/?domain=scribe)

```
postgresql://scribe:password@192.168.1.10:5432/scribe
```

*Or in `configuration.yaml`*, if you would rather keep your configuration in files:

```yaml
scribe:
  db_url: "postgresql://scribe:password@192.168.1.10:5432/scribe"
```

Done — states recorded, chunked and compressed, with their entity, device and area context. Everything below is optional.

---

<details>
<summary><b>🧩 Scribe Card — charts on your dashboard</b></summary>
<br>

**[Scribe Card](https://github.com/jonathan-gtd/scribe-card)** puts any query from your history on a dashboard. Drawn with Apache ECharts — what Home Assistant's own history charts use — and configured in a form, with the chart type, the unit and the axes picked from the columns your query returns.

It talks to Scribe through the `scribe.query` service, so there is **no second database connection to configure and no password in your dashboard**. Install it through HACS as a custom repository, category *Dashboard*.

</details>

<details>
<summary><b>🗄️ Setting up TimescaleDB</b></summary>
<br>

You need a running TimescaleDB instance. I recommend PostgreSQL 17 or 18.

> **❗ Important** — **The TimescaleDB extension is required.** Chunking, compression, retention
> and the size sensors are the whole point of Scribe, and none of them exist on
> plain PostgreSQL. A new installation is refused if the extension is missing —
> although Scribe enables it for you when the server has it available and your
> database user holds `CREATE` on the database, which the setup below grants.
> Installations that already run without it keep recording and are told what
> they are missing through a Repairs issue.

#### Option A: Home Assistant OS (Add-on)

If you are running Home Assistant OS, I recommend using the [TimescaleDB Add-on](https://github.com/expaso/hassos-addon-timescaledb).

[![Open your Home Assistant instance and show the add add-on repository dialog with a specific repository URL pre-filled.](https://my.home-assistant.io/badges/supervisor_add_addon_repository.svg)](https://my.home-assistant.io/redirect/supervisor_add_addon_repository/?repository_url=https%3A%2F%2Fgithub.com%2Fexpaso%2Fhassos-addon-timescaledb)

#### Option B: Docker (Manual)

```bash
# High Availability (Recommended)
docker run -d --name timescaledb -p 5432:5432 -e POSTGRES_PASSWORD=password timescale/timescaledb-ha:pg18

# Standard
docker run -d --name timescaledb -p 5432:5432 -e POSTGRES_PASSWORD=password timescale/timescaledb:pg18
```

Create the database and user:

```sql
CREATE DATABASE scribe;
CREATE USER scribe WITH PASSWORD 'password';
GRANT ALL PRIVILEGES ON DATABASE scribe TO scribe;

\c scribe
CREATE EXTENSION IF NOT EXISTS timescaledb;
GRANT ALL ON SCHEMA public TO scribe;
```

</details>

<details>
<summary><b>⚙️ Every option, with its defaults</b></summary>
<br>

### Full Configuration (Default Values)

```yaml
scribe:
  # The only required option.
  db_url: postgresql://scribe:password@192.168.1.10:5432/scribe

  # Everything below is optional. These are the defaults.

  # Where it records
  db_schema: ""                 # empty = the connection's own schema, normally public
  db_ssl: false                 # TLS to the database
  ssl_root_cert: ""             # CA certificate; only read when db_ssl is true
  ssl_cert_file: ""             # client certificate, for mutual TLS
  ssl_key_file: ""              # its private key

  # What it records
  record_states: true           # state changes
  record_events: false          # Home Assistant events (automations, scripts…)
  include_domains: []           # empty = every domain
  include_entities: []          # empty = every entity
  include_entity_globs: []      # e.g. sensor.weather_*
  exclude_domains: []           # applied after the include lists
  exclude_entities: []
  exclude_entity_globs: []
  exclude_attributes: []        # attribute names stripped from the attributes column
  include_events: []            # empty = every event type
  exclude_events: []            # applied after include_events

  # How long it keeps it
  chunk_time_interval: "7 days" # how much time one chunk covers
  compress_after: "7 days"      # chunks older than this get compressed
  retention_states: ""          # empty = forever; otherwise DELETES older states
  retention_events: ""          # empty = forever; otherwise DELETES older events
  enable_rollups: false         # hourly and daily summaries of numeric states

  # How it writes
  batch_size: 500               # rows buffered before a write
  flush_interval: 5             # seconds before an incomplete batch is written anyway
  max_queue_size: 10000         # rows held in memory before new ones are dropped
  buffer_on_failure: true       # keep buffering while the database is unreachable

  # What scribe.query may cost
  query_timeout: 60             # seconds a query may run
  query_max_rows: 20000         # rows it may return before it is refused

  # Sensors about Scribe itself
  enable_stats_io: false        # writer counters, read from memory
  enable_stats_chunk: false     # chunk counts, one query per refresh
  enable_stats_size: false      # disk sizes, one query per refresh
  stats_io_interval: 60         # seconds between two I/O values
  stats_chunk_interval: 60      # minutes between two chunk queries
  stats_size_interval: 60       # minutes between two size queries

  # Context tables, kept in sync with Home Assistant's registries
  enable_table_areas: true
  enable_table_devices: true
  enable_table_integrations: true
  enable_table_users: true
```

</details>

<details>
<summary><b>📋 Parameter reference</b></summary>
<br>

| Parameter | Description |
| :--- | :--- |
| `db_url` | **Required.** The connection string for your TimescaleDB database. |
| `db_ssl` | Enable SSL/TLS for the database connection. |
| `ssl_root_cert` | Path to the CA certificate file (e.g. `/ssl/ca.crt`). A relative path resolves from the Home Assistant config directory. |
| `ssl_cert_file` | Path to the client certificate file, for mutual TLS. |
| `ssl_key_file` | Path to the client private key file, for mutual TLS. |
| `db_schema` | PostgreSQL schema to record into. Empty (default) uses the connection's own schema, normally `public`. |
| `chunk_time_interval` | How much time each chunk of the table covers. See *Storage tuning* below. |
| `compress_after` | Chunks older than this are compressed. See *Storage tuning* below. |
| `retention_states` | **Deletes** state history older than this interval (e.g. `"365 days"`). Empty (default) keeps everything. See *Retention* below. |
| `retention_events` | **Deletes** event history older than this interval. Empty (default) keeps everything. See *Retention* below. |
| `record_states` | Whether to record state changes. |
| `record_events` | Whether to record events. |
| `batch_size` | Number of items to buffer before writing to the database. |
| `flush_interval` | Maximum time (in seconds) to wait before flushing the buffer. |
| `max_queue_size` | Maximum number of items to hold in memory before dropping new ones. |
| `query_timeout` | Seconds a `scribe.query` call may run before the database stops it (default `60`). |
| `query_max_rows` | Rows a `scribe.query` call may return before it is refused (default `20000`). |
| `buffer_on_failure` | If true, keeps data in memory if the DB is unreachable (up to `max_queue_size`). |
| `enable_stats_io` | Enable real-time writer performance sensors (no DB queries). |
| `enable_stats_chunk` | Enable chunk count statistics sensors (queries DB). |
| `enable_stats_size` | Enable storage size statistics sensors (queries DB). |
| `stats_io_interval` | Seconds between two values of the I/O sensors (default `60`). Every change is a row Scribe records about itself. |
| `stats_chunk_interval` | Interval (in minutes) to update chunk statistics. |
| `stats_size_interval` | Interval (in minutes) to update size statistics. |
| `include_domains` | List of domains to include. |
| `include_entities` | List of specific entities to include. |
| `include_entity_globs` | List of entity patterns to include (e.g. `sensor.weather_*`). |
| `exclude_domains` | List of domains to exclude. |
| `exclude_entities` | List of specific entities to exclude. |
| `exclude_entity_globs` | List of entity patterns to exclude (e.g. `switch.kitchen_*`). |
| `exclude_attributes` | List of attributes to exclude from the `attributes` column. |
| `include_events` | List of event types to record. Leave empty to record all events. |
| `exclude_events` | List of event types to never record (applied after `include_events`). |
| `enable_table_areas` | Enable creation and sync of the `areas` table. |
| `enable_table_devices` | Enable creation and sync of the `devices` table. |
| `enable_table_integrations` | Enable creation and sync of the `integrations` table. |
| `enable_table_users` | Enable creation and sync of the `users` table. |
| `enable_rollups` | Keep pre-computed hourly and daily summaries of states (`states_hourly`, `states_daily`). Off by default. |

</details>

<details>
<summary><b>🗜️ Storage tuning — chunks and compression</b></summary>
<br>

Scribe stores history in TimescaleDB **hypertables**: a table that looks and
queries like any other, but is physically split into **chunks**, each covering a
slice of time. Almost everything about Scribe's disk usage and query speed comes
down to that split — a query for last week reads only the chunks that overlap
last week, compression works one chunk at a time, and *Retention* below
deletes whole chunks rather than individual rows.

Two settings control it, both in YAML and in the UI under **Configure →
Advanced (TimescaleDB & SSL)**:

### `chunk_time_interval` (default `7 days`)

How much time one chunk covers.

- **Smaller chunks** (e.g. `1 day`) mean more, smaller files: finer-grained
  retention, and queries over short recent windows touch less data. Past a
  point, a query spanning months has to open hundreds of chunks.
- **Larger chunks** (e.g. `30 days`) mean fewer, bigger files: better for long
  historical queries, worse for memory — TimescaleDB's own guidance is that the
  chunks you write to should comfortably fit in memory alongside their indexes,
  so an oversized chunk on a small machine hurts write performance.

The default suits a typical Home Assistant instance. Consider `1 day` if you
record thousands of entities, and only then.

> **Changing it affects new chunks only.** Chunks already written keep the span
> they were created with, and nothing is rewritten or moved — you will simply
> have a mix of old and new spans, which TimescaleDB handles natively.

### `compress_after` (default `7 days`)

How old a chunk must be before TimescaleDB compresses it. Compression is
typically a large reduction in size for this kind of data (many repeated
`entity_id`s and slowly-changing values), which is why it is on by default.

Compressed chunks are still fully queryable — the `states` view does not care.
Writing *into* one is slower, which is why compression only kicks in once a
chunk is old enough to be effectively finished. Keep `compress_after` comfortably
above the age of the data you still write to; states arriving out of order (a
backfill, a migration script) land in old chunks.

> **Changing it takes effect on the next restart**, and chunks already
> compressed stay compressed — the setting only decides when the *next* ones
> are.

### How the three settings fit together

| Setting | What it does | Reversible |
| :--- | :--- | :--- |
| `chunk_time_interval` | How much time one chunk covers | Yes — future chunks only |
| `compress_after` | When a chunk gets compressed | Yes |
| `retention_states` / `retention_events` | When a chunk gets **deleted** | **No** |

They apply in that order to the same chunk over its life: written → compressed →
dropped. Two consequences worth knowing:

- If `compress_after` is greater than your retention, chunks are deleted before
  they are ever compressed, and compression does nothing.
- Retention deletes whole chunks, so your real retention window is the interval
  you set *plus* up to one `chunk_time_interval`. Smaller chunks make it tighter.

If the size and chunk statistics sensors are enabled (`enable_stats_size`,
`enable_stats_chunk`), they report exactly what these settings produce: chunk
counts, compressed and uncompressed sizes, and the compression ratio.

</details>

<details>
<summary><b>🧹 Retention — dropping old history on a schedule</b></summary>
<br>

By default Scribe keeps everything, forever. If you only want to store a bounded
window — because you aggregate the raw history elsewhere, or simply to cap disk
usage — set a retention interval and TimescaleDB will drop chunks older than it:

```yaml
scribe:
  db_url: postgresql://scribe:password@192.168.1.10:5432/scribe
  retention_states: "365 days"
  retention_events: "30 days"
```

Both are also in the UI under **Configure → Advanced (TimescaleDB & SSL)**.

> **⚠️ Warning** — Retention **deletes data permanently**. There is no undo and no trip to a bin:
> once a chunk falls outside the window it is dropped, and only a backup brings
> it back. States and events are configured separately so you can expire noisy
> events while keeping state history.

Details worth knowing:

- **No setting means "keep forever", always.** Clearing the field in the UI and
  deleting the line from `configuration.yaml` both remove the policy — a value
  Scribe once imported from YAML is never allowed to outlive the line that set
  it.
- **Scribe owns the retention policy on its own tables.** Emptying the field
  removes the policy — including one you created by hand with
  `add_retention_policy()`, which is the only way clearing the setting in the UI
  can actually stop the deletions.
- **It starts immediately.** TimescaleDB runs the policy within seconds of it
  being created, not at the next daily interval — everything outside the window
  is gone on the first run, right after the restart that enabled it.
- **Deletion happens by chunk, not by row.** A chunk is dropped only once *all*
  of its rows are older than the interval, so with the default `chunk_time_interval`
  of 7 days you keep up to a week more than you asked for. That is what makes
  retention nearly free: it drops files rather than deleting rows.
- **Only the history is deleted.** The `entities` table and the other metadata
  tables are not touched, so an entity whose history has fully expired still
  resolves.
- **TimescaleDB is required** — it is the extension that runs the policy. On
  plain PostgreSQL, setting a retention interval raises a Repairs issue instead
  of silently doing nothing.
- Accepted values are plain intervals: `30 days`, `6 months`, `1 year`.
  Anything else is refused with an error rather than sent to the database.

</details>

<details>
<summary><b>📈 Summaries — hourly and daily rollups</b></summary>
<br>

A year of a sensor that reports every 30 seconds is about a million rows. A chart of that year reads every one of them, every time it is drawn.

With `enable_rollups: true`, TimescaleDB keeps two summaries of your states up to date as they are written — hourly and daily — and a chart over years reads thousands of rows instead of millions.

```yaml
scribe:
  enable_rollups: true
```

It is also in the UI under **Configure → Metadata Tables**. That adds two views:

| View | One row per | Columns |
| --- | --- | --- |
| `states_hourly` | entity and hour | `entity_id`, `bucket`, `value_avg`, `value_min`, `value_max`, `samples` |
| `states_daily` | entity and day | the same |

```sql
SELECT bucket, value_avg, value_min, value_max
FROM states_daily
WHERE entity_id = 'sensor.outside_temperature'
  AND bucket > now() - interval '2 years'
ORDER BY bucket;
```

Only numeric states are summarised — the average of `on` and `off` means nothing — so `value_avg`, `value_min` and `value_max` are empty for the rest, while `samples` counts every state in the bucket.

**They are derived data.** Nothing is duplicated that you would miss: turning the option off deletes both views, turning it back on rebuilds them from the history, and your states are never touched either way. TimescaleDB refreshes them itself — the hourly one every 30 minutes, the daily one every hour — and each run looks back far enough (3 days, 30 days) that a batch written late still lands in them. Scribe only creates them.

</details>

<details>
<summary><b>🗃️ Recording into a specific PostgreSQL schema</b></summary>
<br>

By default Scribe records into whatever schema your connection already points at — normally `public`. Set `db_schema` and it creates that schema and puts everything in it instead: its tables, its views, its hypertables and its policies.

```yaml
scribe:
  db_url: postgresql://scribe:password@192.168.1.10:5432/scribe
  db_schema: scribe
```

It is also in the UI under **Configure → Advanced (TimescaleDB & SSL)**.

This is what you want when Scribe shares a database with something else: another integration's tables, your own copies of the history, or a second Home Assistant recording into the same server. Schemas are independent — separate tables, hypertables, retention and compression — and nothing Scribe does in one reaches the other.

- **Only new data goes there.** Setting `db_schema` does not move history already recorded. Move it yourself before restarting (`ALTER TABLE public.states_raw SET SCHEMA scribe;`), or query the old schema directly.
- **Scribe creates the schema if it can**, which needs `CREATE` on the database. One you created by hand works too, given `USAGE` and `CREATE` on it.
- **An unreachable schema stops recording.** PostgreSQL falls through to the next entry on the search path instead of failing, so a typo would otherwise fill `public` while the UI showed something else. Scribe checks where it landed and records nothing rather than the wrong thing, with a Repairs issue saying what to grant.
- **Your queries do not change.** Scribe puts the schema first on the connection's `search_path`, so `SELECT * FROM states` keeps working through `scribe.query`. From Grafana or psql, qualify the name (`scribe.states`) or set your own `search_path`. `public` stays on the path — that is where the TimescaleDB functions live.
- Accepted values are plain identifiers: letters, digits and underscores, not starting with a digit. Empty keeps the connection's own schema, including one you set yourself with `?options=-csearch_path%3Dmyschema` in the URL.

**The tables themselves** — every column, how they relate, and query recipes for Grafana and `scribe.query` — are documented in [`docs/data-structure.md`](docs/data-structure.md).

</details>

<details>
<summary><b>🛠️ Services — flush, query, purge</b></summary>
<br>

### `scribe.flush`
Force an immediate flush of buffered data to the database.

```yaml
service: scribe.flush
```

### `scribe.query`
Execute a read-only SQL query against the TimescaleDB database.

**Parameters:**
- `sql` (Required): The SQL query to execute. Must be a `SELECT` statement.

**Returns:**
A list of rows, where each row is a dictionary of column names and values.

**Example:**
```yaml
service: scribe.query
data:
  sql: "SELECT * FROM states ORDER BY time DESC LIMIT 5"
response_variable: query_result
```

### `scribe.purge`
Delete recorded history. **This cannot be undone.**

**Parameters** (at least one of the first two is required):
- `entity_id`: entities to purge. Without `keep_days`, their whole history *and* their row in the `entities` table are deleted — recording them again starts from nothing.
- `keep_days`: delete everything older than this many days.
- `events` (default `false`): also delete events older than `keep_days`. Ignored without it.

**Returns:** how many states, events and entity rows were deleted.

**Examples:**
```yaml
# Remove one entity from the database entirely
action: scribe.purge
data:
  entity_id: sensor.sensor_i_no_longer_want
```

```yaml
# Trim everything older than two years, events included
action: scribe.purge
data:
  keep_days: 730
  events: true
response_variable: purged
```

Compressed history is purged as well; TimescaleDB handles it, and the chunks stay compressed. For a rolling window you want to keep applying, use the *Retention* settings below instead: a purge is a one-off.

</details>

<details>
<summary><b>📊 Statistics sensors</b></summary>
<br>

Enable sensors by setting their flags in your configuration.

### IO Statistics (`enable_stats_io: true`)

Real-time metrics from the writer (no DB queries).

| Sensor | Description |
| :--- | :--- |
| <img src="https://api.iconify.design/mdi:database-plus.svg?color=%232196F3" width="15" /> `sensor.scribe_states_written` | Total number of state changes written to the DB. |
| <img src="https://api.iconify.design/mdi:database-plus.svg?color=%232196F3" width="15" /> `sensor.scribe_events_written` | Total number of events written to the DB. |
| <img src="https://api.iconify.design/mdi:buffer.svg?color=%232196F3" width="15" /> `sensor.scribe_buffer_size` | Current number of items waiting in the memory buffer. |
| <img src="https://api.iconify.design/mdi:timer-sand.svg?color=%232196F3" width="15" /> `sensor.scribe_last_write_duration` | Time taken (in ms) for the last database write operation. |
| <img src="https://api.iconify.design/mdi:speedometer.svg?color=%232196F3" width="15" /> `sensor.scribe_states_rate` | Rate of states written to DB (per minute). |
| <img src="https://api.iconify.design/mdi:speedometer.svg?color=%232196F3" width="15" /> `sensor.scribe_events_rate` | Rate of events written to DB (per minute). |

### Chunk Statistics (`enable_stats_chunk: true`)

Chunk counts (updated every `stats_chunk_interval` minutes).

| Sensor | Description |
| :--- | :--- |
| <img src="https://api.iconify.design/mdi:cube-outline.svg?color=%232196F3" width="15" /> `sensor.scribe_states_total_chunks` | Total number of chunks for the states table. |
| <img src="https://api.iconify.design/mdi:package-down.svg?color=%232196F3" width="15" /> `sensor.scribe_states_compressed_chunks` | Number of chunks that have been compressed. |
| <img src="https://api.iconify.design/mdi:package-up.svg?color=%232196F3" width="15" /> `sensor.scribe_states_uncompressed_chunks` | Number of chunks waiting to be compressed. |
| <img src="https://api.iconify.design/mdi:cube-outline.svg?color=%232196F3" width="15" /> `sensor.scribe_events_total_chunks` | Total number of chunks for the events table. |
| <img src="https://api.iconify.design/mdi:package-down.svg?color=%232196F3" width="15" /> `sensor.scribe_events_compressed_chunks` | Number of compressed event chunks. |
| <img src="https://api.iconify.design/mdi:package-up.svg?color=%232196F3" width="15" /> `sensor.scribe_events_uncompressed_chunks` | Number of uncompressed event chunks. |

### Size Statistics (`enable_stats_size: true`)

Storage usage in bytes (updated every `stats_size_interval` minutes).

| Sensor | Description |
| :--- | :--- |
| <img src="https://api.iconify.design/mdi:database.svg?color=%232196F3" width="15" /> `sensor.scribe_states_total_size` | Total disk size (includes compressed data + recent chunks + indices). |
| <img src="https://api.iconify.design/mdi:database-search.svg?color=%232196F3" width="15" /> `sensor.scribe_states_original_size` | **Theoretical size** if data was not compressed (e.g. 11 GB). |
| <img src="https://api.iconify.design/mdi:package-variant.svg?color=%232196F3" width="15" /> `sensor.scribe_states_compressed_size` | Physical size of the compressed data chunks. |
| <img src="https://api.iconify.design/mdi:package-variant-closed.svg?color=%232196F3" width="15" /> `sensor.scribe_states_uncompressed_size` | Size of recent data not yet compressed (or pending indices). |
| <img src="https://api.iconify.design/mdi:percent.svg?color=%232196F3" width="15" /> `sensor.scribe_states_compression_ratio` | Compression ratio for states (%). |
| <img src="https://api.iconify.design/mdi:database.svg?color=%232196F3" width="15" /> `sensor.scribe_events_total_size` | Total disk size of the events table. |
| <img src="https://api.iconify.design/mdi:database-search.svg?color=%232196F3" width="15" /> `sensor.scribe_events_original_size` | Theoretical size of events before compression. |
| <img src="https://api.iconify.design/mdi:package-variant.svg?color=%232196F3" width="15" /> `sensor.scribe_events_compressed_size` | Size of compressed event data. |
| <img src="https://api.iconify.design/mdi:package-variant-closed.svg?color=%232196F3" width="15" /> `sensor.scribe_events_uncompressed_size` | Size of uncompressed event data. |
| <img src="https://api.iconify.design/mdi:percent.svg?color=%232196F3" width="15" /> `sensor.scribe_events_compression_ratio` | Compression ratio for events (%). |

</details>

<details>
<summary><b>🖼️ Dashboard</b></summary>
<br>

A pre-configured Lovelace layout containing all useful Scribe sensors (Database Statistics, Compression Ratios, I/O Performance) is available in this repository, in two flavours:

| File | What it is | Where to paste it |
| --- | --- | --- |
| [`lovelace_scribe_card.yaml`](lovelace_scribe_card.yaml) | A **single card** (`type: vertical-stack`) | The card YAML editor ("Add card" → "Manual") |
| [`lovelace_scribe_view.yaml`](lovelace_scribe_view.yaml) | A **whole view** (`title` / `icon` / `cards`) | The view YAML editor |

> ⚠️ These two are not interchangeable. Pasting the *view* file into a *card* editor fails with **"No card type configured"**, because a card config must start with a `type:` key.

**Option A — add it as a card (easiest, works in every view type):**

1.  Open your dashboard and click "Edit Dashboard" (pencil icon).
2.  Click **+ Add card** and scroll to the bottom of the card picker to select **Manual**.
3.  Copy the content of [`lovelace_scribe_card.yaml`](lovelace_scribe_card.yaml), replace everything in the editor with it, and click **Save**.

**Option B — add it as a dedicated view:**

1.  Open your dashboard and click "Edit Dashboard" (pencil icon).
2.  Click the **+** button *in the top tab bar* (next to your existing view names) to add a new View — not the "Add card" button.
3.  In the view dialog, open the ⋮ menu (or the "Show code editor" button) and choose **Edit in YAML**.
4.  Copy the content of [`lovelace_scribe_view.yaml`](lovelace_scribe_view.yaml), replace everything in the editor with it, and click **Save**.

</details>

<details>
<summary><b>📦 Migrating from InfluxDB, LTSS, the recorder or Scribe 2.x</b></summary>
<br>

### Upgrading from Scribe 2.x

Scribe 3.0 replaced the `states` table with `states_raw` plus a compatibility
view, and gave `entities` a numeric primary key. The conversion of an old
database was carried by 3.x and **removed in 3.9**.

If your database still has a `states` *table* (rather than a view), a
`states_legacy` table, or an `entities` table without an `id` column, Scribe
stops at startup, records nothing, and raises a Repairs issue — without
renaming, creating or deleting anything. Install **Scribe 3.8**, let Home
Assistant run until the logs report the migration finished (around fifteen
minutes on a large database), then update again.

Fresh installs and any database created by 3.x are unaffected.

### Backfilling from other sources

`migration/` holds three scripts that copy history into Scribe from somewhere else. They are run by hand, once, from a machine that reaches both databases — and Scribe must have started at least once, so its tables exist.

```bash
cd migration
pip install psycopg2-binary python-dotenv   # plus influxdb-client for InfluxDB
cp .env.example .env && nano .env
python3 <script>.py
```

| Source | Script | What to fill in |
| --- | --- | --- |
| InfluxDB | `influx2scribe.py` | `INFLUX_*` |
| LTSS | `ltss2scribe.py` | `LTSS_*` |
| Home Assistant recorder | `recorder2scribe.py` | `RECORDER_*`, with `RECORDER_TYPE` set to `postgres` or `sqlite` (SQLite only needs `RECORDER_DB_PATH`) |

Every run also needs `SCRIBE_*` — the destination — and the migration settings: `MIGRATION_START_TIME`, `MIGRATION_END_TIME`, `CHUNK_SIZE` (hours per batch), and `PURGE_DESTINATION`, which **deletes the destination's history before importing**. Leave it `False` unless you mean it.

Each script checks the destination schema before writing anything, and stops with an explanation rather than a wall of per-row errors if Scribe never initialised it.

</details>

<details>
<summary><b>🩺 Troubleshooting</b></summary>
<br>

### Before anything else

Two places answer "why is nothing being recorded?" without reading a single log line:

- **Settings → Devices & Services → Scribe → ⋮ → Download diagnostics** reports what
  the writer is actually doing: connected or not, whether TimescaleDB was found, how
  many items are waiting in the buffer and how many were dropped, consecutive write
  failures, the storage and retention settings in force. The database URL is never
  included, and driver errors have any connection string stripped out.
- **Settings → System → Repairs** lists the conditions below, and
  **Settings → System → System health** shows the database Scribe is pointed at and
  whether it is connected right now.

### Repairs

Scribe reports problems it cannot fix on its own in **Settings → System → Repairs**, so you do not have to watch the logs. Each one disappears by itself once the condition is resolved.

| Repair | What it means |
| --- | --- |
| Cannot reach its database | The connection failed. Scribe keeps buffering and retries in the background, so history recorded during the outage is written once the database returns. Check that the server is up and that the URL and credentials are right. |
| Cannot write to its database | Several consecutive writes failed. Data is held in memory and written on recovery — unless Home Assistant restarts first. |
| Buffer is full | Writes failed long enough to saturate the buffer; the oldest records are now being discarded. Fix the database, or raise `max_queue_size`. |
| Discarding records | A write failed while buffering is disabled, so records were dropped immediately. Enable buffering to survive short outages. |
| Could not create its tables | Scribe reached the database but failed to build its schema, usually a privileges problem. On a new database nothing is recorded at all. |
| Cannot reach the schema it was given | The schema in `db_schema` does not exist and could not be created, or the database user has no rights on it. Nothing is recorded — rather than silently filling `public` instead. |
| Could not create the `states` view | History is recorded, but the view every query goes through is missing — the history looks empty even though nothing is lost. |
| `states_raw` / `events` is not a hypertable | TimescaleDB is installed but the table was never converted (a common outcome when the extension is added *after* the tables filled up). Chunking, compression and retention all do nothing. |
| `states_raw` / `events` is never compressed | The table is a hypertable but has no compression policy, so it keeps its full uncompressed size. |
| TLS is not fully in force | Scribe connects over TLS, but a certificate you configured could not be applied — most often a client certificate, so it authenticates as an ordinary client instead of the one you provisioned. |
| TimescaleDB is not installed | History is recorded, but chunking and compression are unavailable, so the database grows much faster and the size sensors stay empty. |
| Database predates version 3.0 | The database still uses the pre-3.0 layout, which this version cannot convert. Nothing is recorded and nothing was modified — install Scribe 3.8 to convert it, then update again. |
| Could not apply the retention policy | You asked Scribe to delete data older than an interval and the policy could not be created. Nothing was deleted, and nothing is being deleted — the table keeps growing. |
| Entity rename was not applied | A rename collided with an existing row in the database. The entity's history is split across the two IDs. |

### High memory usage
- Reduce `max_queue_size`
- Reduce `flush_interval` for faster writes
- Check `sensor.scribe_buffer_size`

### Performance tuning

If the `states` view is slow (several seconds per query), it is likely due to the PostgreSQL query planner choosing a **Hash Join** instead of a **Nested Loop**, which prevents TimescaleDB from pruning chunks effectively.

The most common cause is a high `random_page_cost` (the default is `4.0`, optimized for HDDs). If you are using modern storage (SSD, NVMe) or have a well-cached database, you should reduce this value:

```sql
-- Check current value
SHOW random_page_cost;

-- Set to a lower value (usually 1.1)
ALTER SYSTEM SET random_page_cost = 1.1;
SELECT pg_reload_conf();
```

Reducing this value encourages the planner to use index-based joins (Nested Loops), which are essential for Scribe's performance with large datasets.

### Still having issues?
Please [open an issue](https://github.com/jonathan-gtd/scribe/issues) on GitHub with your logs and configuration. I would be happy to help!

</details>

---

## License

MIT License - See LICENSE file for details
