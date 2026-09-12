# Developing Scribe

Everything needed to work on Scribe's code: how it is built, the rules the code relies on, how to test it, how changes get merged, and how a release is cut.
For how to *use* Scribe, see the [README](../README.md). For the database layout from a user's point of view, see [data-structure.md](data-structure.md).

- [1. Quick start](#1-quick-start)
- [2. Repository layout](#2-repository-layout)
- [3. How Scribe works](#3-how-scribe-works)
- [4. Rules the code relies on](#4-rules-the-code-relies-on)
- [5. Development environment](#5-development-environment)
- [6. Tests](#6-tests)
- [7. Branches, commits and pull requests](#7-branches-commits-and-pull-requests)
- [8. Changelog](#8-changelog)
- [9. Releasing](#9-releasing)
- [10. Repairs issues](#10-repairs-issues)
- [11. Checklist: adding a configuration option](#11-checklist-adding-a-configuration-option)
- [12. Translations and READMEs](#12-translations-and-readmes)
- [13. Home Assistant compatibility](#13-home-assistant-compatibility)
- [14. Static analysis](#14-static-analysis)
- [15. Migration scripts and helper scripts](#15-migration-scripts-and-helper-scripts)

---

## 1. Quick start

From a fresh clone to a green test suite:

```
uv venv --python 3.14 venv
uv pip install --python venv/bin/python -r requirements_test.txt pytest-cov

docker run -d --name scribe-test-db -e POSTGRES_PASSWORD=scribe \
    -e POSTGRES_DB=scribe -p 55432:5432 timescale/timescaledb:latest-pg17 \
    -c timescaledb.max_background_workers=0

venv/bin/ruff check . && venv/bin/ruff format --check .
venv/bin/python -m pytest tests
```

`uv` is only a convenient way to get Python 3.14. Any Python 3.14 works: `python3.14 -m venv venv`, then `venv/bin/pip install -r requirements_test.txt pytest-cov`.

---

## 2. Repository layout

HACS installs **only `custom_components/scribe/`**. Everything else in the repository never reaches a user.

| Path | What it is |
|---|---|
| `custom_components/scribe/__init__.py` | Setup and unload, the YAML schema (`CONFIG_SCHEMA`), settings resolution, the event listeners, the registry sync, the services. |
| `custom_components/scribe/writer.py` | `ScribeWriter`: connection pool, queue, flushes, schema creation, TimescaleDB policies, entity renames, Repairs issues, `scribe.query`, statistics. Most of the logic lives here. |
| `custom_components/scribe/config_flow.py` | The UI setup (database URL, TimescaleDB check), the YAML import, and the 5-step options flow. |
| `custom_components/scribe/const.py` | Every `CONF_*` key and `DEFAULT_*` value. |
| `custom_components/scribe/sensor.py` | The statistics sensors (I/O, chunks, sizes). All are off by default. |
| `custom_components/scribe/binary_sensor.py` | The "Database Connection" sensor. Also holds `_redact_dsn`. |
| `custom_components/scribe/coordinator.py` | `DataUpdateCoordinator` that polls `writer.get_db_stats()` for the chunk and size sensors. |
| `custom_components/scribe/diagnostics.py` | The diagnostics download (config entry + writer state, credentials redacted). |
| `custom_components/scribe/system_health.py` | The Scribe block on Home Assistant's System information page (database, schema, connected, TimescaleDB, buffered items). |
| `custom_components/scribe/strings.json`, `translations/*.json` | UI and Repairs texts. See [section 12](#12-translations-and-readmes). |
| `custom_components/scribe/services.yaml`, `icons.json`, `manifest.json` | Service descriptions, icons, integration manifest (holds the **version**). |
| `tests/` | Unit tests, run against mocks. `tests/conftest.py` mocks `asyncpg.create_pool` for every test. |
| `tests/integration/` | End-to-end tests against a real TimescaleDB. See [section 6](#6-tests). |
| `tests/upgrade/` | Upgrade tests: a database filled by an older release, opened by the current code. See [6.6](#66-upgrade-tests). |
| `migration/` | Stand-alone scripts that import history from InfluxDB, LTSS or the HA recorder. Not part of the integration. |
| `scripts/` | Local helper shell scripts. See [section 15](#15-migration-scripts-and-helper-scripts). |
| `.github/workflows/` | CI. See [section 6](#6-tests) and [section 9](#9-releasing). |
| `hacs.json` | HACS metadata, including the **minimum Home Assistant version**. |
| `README.md`, `README.fr.md`, `README.es.md`, `README.de.md` | User documentation, in four languages, kept in sync by tests. |
| `CHANGELOG.md` | One entry per user-visible change. See [section 8](#8-changelog). |
| `docs/DEVELOPMENT.md` | This guide. |
| `docs/data-structure.md` | User guide to the tables and to querying them. |
| `lovelace_*.yaml` | Example dashboard and card. |

Ignored by git and safe to delete: `venv/`, `.pytest_cache/`, `.ruff_cache/`, `__pycache__/`, `.coverage`, `coverage.xml`, `tuto/` (local copy of Home Assistant's developer docs), `migration/.env`.

---

## 3. How Scribe works

### 3.1 Setup sequence

1. **`async_setup`** (`__init__.py`) runs when Home Assistant starts. If `configuration.yaml` has a `scribe:` block, it stores it in `hass.data["scribe"]["yaml_config"]` and starts the **import** config flow. That creates the config entry the first time and updates it afterwards.
2. **`async_setup_entry`** runs for the (single) config entry:
   1. `_resolve_settings` builds a `_Settings` object (see [3.2](#32-where-each-setting-comes-from)). If no database URL can be found, setup fails.
   2. A `ScribeWriter` is built and `writer.start()` is awaited. `start()` **never fails because the database is down**. It starts the writer loop either way, and the loop reconnects in the background (see [3.4](#34-connection-and-reconnection)).
   3. The statistics coordinators are created, only for the sensor groups that are enabled.
   4. `_async_late_setup` is started as a background task. It pushes every registry into the database (`_sync_metadata`) and primes the coordinators. It runs in the background so a large registry cannot hit Home Assistant's bootstrap timeout.
   5. The listeners are registered: state changes (if `record_states`), events (if `record_events`), and the entity, device, area and user registries. **Before the platforms**, so a state set while they load is recorded as it changes rather than only in its final form.
   6. With `record_states`, `_record_current_states` queues the state of everything Home Assistant has already set ([3.7](#37-the-states-already-set-at-startup)), then the `sensor` and `binary_sensor` platforms are set up.
   7. The writer is set to stop on `homeassistant_final_write`, **not** `homeassistant_stop`. Home Assistant runs the specific listeners of an event before the `MATCH_ALL` ones, so stopping on `homeassistant_stop` flushed before Scribe had recorded the stop event and everything emitted during shutdown.
   8. The `scribe.flush` and `scribe.query` services are registered.
   9. An update listener is added: **any change in the options flow unloads and sets up the entry again**. So every check done at startup is also done after each options change.
3. **`async_unload_entry`** unloads the platforms, then awaits `writer.stop()`. `stop()` cancels the loop, does a final flush and closes the pool.

`hass.data["scribe"]` holds `"yaml_config"` and, per entry id, a dict with `"writer"`, `"chunk_coordinator"`, `"size_coordinator"` and `"enable_stats_io"`.

### 3.2 Where each setting comes from

`_resolve_settings` reads every setting from four sources, in this order. **The first one that has the key wins:**

1. `configuration.yaml` (`scribe:` block)
2. the config entry's **options** (written by the options flow)
3. the config entry's **data** (written by the initial setup or by the YAML import)
4. the default in `const.py`

Two exceptions:

- **`db_url`** is read from YAML, then from the entry data. If neither has it, it is rebuilt from the pre-3.x keys `db_user`, `db_password`, `db_host`, `db_port`, `db_name`.
- **`retention_states` and `retention_events` are never read from the entry data.** A YAML import copies its keys into the data, and nothing removes them when the line is deleted from `configuration.yaml`. Reading them back would keep deleting history the user stopped asking to delete. The UI stores retention in the options.

### 3.3 Recording path

```
state_changed / other event
  → listener (@callback, synchronous, on the event loop)
  → writer.enqueue(dict)                     appended to a deque(maxlen=max_queue_size)
  → flush, when either
       · the queue reaches batch_size        (enqueue starts a flush task), or
       · flush_interval seconds pass         (the writer loop in _run)
  → _flush()
       1. drain the queue into `batch`
       2. _split_batch in an executor        sanitize every value, split states / events
       3. under _metadata_lock:
            resolve entity_id → metadata_id  (inserts missing rows into `entities`)
            COPY states into states_raw and events into `events`, in one transaction
       4. success → counters, clear write-failure issues
          failure → _handle_flush_failure
```

- **The queue drops its oldest items silently when it is full** (`deque(maxlen=...)`). `buffer_full` is raised when that happens during an outage.
- On failure, with `buffer_on_failure` on (the default), the batch is put back at the front of the queue and retried at the next flush. With it off, the batch is dropped and `data_dropped` is raised.
- Two states for the same entity at the same timestamp in one batch are collapsed into the last one. `states_raw`'s primary key is `(metadata_id, time)`, and COPY has no `ON CONFLICT`: a single duplicate would fail the batch at every retry.
- If the COPY into `states_raw` hits a row already in the table, the states are retried row by row with `ON CONFLICT (metadata_id, time) DO NOTHING`. `events` has no key, so it cannot conflict.
- Numeric states go to `value` (`DOUBLE PRECISION`) with `state` NULL. Any other state goes to `state` (`TEXT`) with `value` NULL.
- `enqueue` does nothing while the writer is stopped, or blocked on a pre-3.0 database or an unusable schema.

### 3.4 Connection and reconnection

- The pool is created by `_connect()`: `asyncpg.create_pool(min_size=1, max_size=10)`, with a custom `jsonb` codec and, when `db_schema` is set, `search_path` passed as a **startup parameter**. A `SET` would be undone by asyncpg's `RESET ALL` when a connection returns to the pool.
- If the database is unreachable, `db_unreachable` is raised and the loop retries on a backoff: 5 s, then doubling, up to 300 s (`RECONNECT_MIN_DELAY`, `RECONNECT_MAX_DELAY`). Meanwhile states keep being queued, and they are written once the database answers.

### 3.5 Database initialisation (`init_db`)

Runs after every successful connection, in this order:

1. **Schema** (`_ensure_schema`). If `db_schema` is set: `CREATE SCHEMA IF NOT EXISTS`, then check that `current_schema()` really is that schema. PostgreSQL silently skips a `search_path` entry that does not exist, so without this check the history would go to `public`. If the check fails, recording is refused and `schema_unavailable` is raised.
2. **Pre-3.0 database check** (`_detect_legacy_schema`). A `states` *table*, a `states_legacy` table, or an `entities` table without an `id` column means a database from before 3.0. Scribe then records nothing, changes nothing and raises `legacy_schema`, which points at 3.8, the last version able to convert it.
3. **Tables** (`_create_tables`), in one transaction: `entities` first, then `users`, `areas`, `devices`, `integrations` (each only if enabled), then `states_raw` + the `states` view (if `record_states`) and `events` (if `record_events`). All use `CREATE ... IF NOT EXISTS`.
4. **TimescaleDB** (`ensure_timescaledb`). If the extension is missing but available, Scribe runs `CREATE EXTENSION`. If it still is not there, `no_timescaledb` is raised and the hypertable steps are skipped.
5. **Hypertables**, separately for `states_raw` (segmented by `metadata_id`) and `events` (segmented by `event_type`):
   `create_hypertable` → enable compression → `_apply_chunk_interval` → `_apply_compression_policy` → `_verify_storage_features` → `_apply_retention_policy`.
   The chunk interval, compression policy and retention policy are **brought in line with the settings at every start**, not only when the table is created. A new chunk interval only applies to chunks created afterwards.

The tables:

| Table | Key | Content |
|---|---|---|
| `states_raw` | `(metadata_id, time)` | `time`, `metadata_id` → `entities.id`, `state`, `value`, `attributes` (jsonb). Hypertable. |
| `states` (view) | — | `states_raw` joined to `entities`: `time`, `entity_id`, `state`, `value`, `attributes`. What users query. |
| `events` | none | `time`, `event_type`, `event_data` (jsonb), `origin`, `context_id`, `context_user_id`, `context_parent_id`. Hypertable. |
| `entities` | `id` (SERIAL), `entity_id` UNIQUE | `unique_id`, `platform`, `domain`, `name`, `device_id`, `area_id`, `capabilities`. |
| `devices` | `device_id` | `name`, `name_by_user`, `model`, `manufacturer`, `sw_version`, `area_id`, `primary_config_entry`. |
| `areas` | `area_id` | `name`, `picture`. |
| `integrations` | `entry_id` | `domain`, `title`, `state`, `source`. |
| `users` | `user_id` | `name`, `is_owner`, `is_active`, `system_generated`, `group_ids`. |

The names `states` and `events` come from `DEFAULT_TABLE_NAME_STATES` / `DEFAULT_TABLE_NAME_EVENTS`. They are not user settings.

### 3.6 Metadata sync and entity renames

- **At startup**, `_sync_metadata` writes each enabled registry in full. Each table is synced in its own `try`, so a failure on one does not stop the others. `entities` is always synced: every state write depends on it.
- **Afterwards**, listeners on `entity_registry_updated`, `device_registry_updated`, `area_registry_updated` and `user_added` / `user_updated` / `user_removed` keep the tables current.
- `write_entities` inserts new rows and updates changed ones, and skips identical ones. `INSERT ... ON CONFLICT DO UPDATE` would use up a SERIAL id on every row at each resync.
- **A rename** (`rename_entity`) is a single `UPDATE entities SET entity_id = ...`. The history in `states_raw` points at `metadata_id` and does not move. If the new name is already taken by another row:
  - that row belongs to the **same** entity (same `unique_id`) → it is merged;
  - that row belongs to an entity that is **provably gone** (its `unique_id`/`domain`/`platform` resolve to nothing in Home Assistant's registry) → its history is merged into the renamed entity and the row is deleted;
  - otherwise → the rename is **refused**, nothing is modified, and `rename_refused_live` or `rename_refused_unprovable` is raised.

### 3.7 The states already set at startup

The listener only ever sees what changes *after* it is registered, and Scribe is set up well into a Home Assistant start. An entity that changed while Home Assistant was down and does not change again would never be recorded: the history shows the previous value carrying on across the gap.

So `_record_current_states` walks `hass.states.async_all()` at setup and queues each state that passes the filter, through the same `_state_row` the listener uses — **same `last_updated`**. What that writes depends on why Scribe is starting:

- after a **Home Assistant restart**, Home Assistant has just given every entity a fresh `last_updated`, so these are new rows: one per live entity (1361 rows for 795 entities on the maintainer's installation, against ~58 000 recorded in a day). They are the point — they are the states nothing else would have recorded;
- after a **reload** (any options change reloads the entry), the states still carry the `last_updated` they already had, so each row is the one already in `states_raw` and the key refuses it.

Either way a state is never stored twice, which is what the key is for.

That key is therefore the condition. A database created by Scribe 3.1 to 3.5 has none, and no release ever added it ([6.6](#66-upgrade-tests)), so writing a state twice would store it twice: `writer.deduplicates_states` is false there, the snapshot is skipped, and a warning says why. `_states_have_primary_key` settles it once per start, in `init_db`.

### 3.8 Services, sensors, diagnostics

- `scribe.flush` runs a flush immediately.
- `scribe.query` runs one SQL statement in a `READ ONLY` transaction with `statement_timeout = 120000` ms (`QUERY_TIMEOUT_MS`). The rows go through the same sanitizer as the write path, so `Decimal` and `timedelta` come back as numbers.
- The sensors are opt-in: `enable_stats_io` (read from the writer's counters), `enable_stats_chunk` and `enable_stats_size` (each polled by its own coordinator, every 60 minutes by default).
- Diagnostics and system health read the writer's internal state. They never show the database URL, only `_safe_target()`, which is host, port and database name.

---

## 4. Rules the code relies on

Breaking one of these has caused a real bug before. Each is explained in a comment at the place it applies.

1. **Never block the event loop.** The state and event listeners are synchronous `@callback`s that only call `enqueue`. CPU work on a batch (`_split_batch`) and file reads (the TLS certificates) run in an executor.
2. **Never let an exception escape into Home Assistant.** A recorder must not take Home Assistant down. `except Exception` is used broadly on purpose; ruff's `BLE001` is deliberately not enabled. Every caught error is logged, with `exc_info=True` when it is unexpected.
3. **SQL: values are parameters, names are validated.**
   - Values go through `$1`, `$2`… Free-form text from the user (for instance `chunk_time_interval`) is passed as a parameter and cast in SQL (`$2::text::interval`). It is never interpolated.
   - Table and schema names cannot be parameters, so they are checked against `[A-Za-z_][A-Za-z0-9_]*` (`_validate_table_name`, `_validate_schema_name`) before reaching an f-string.
   - A retention interval is interpolated into a TimescaleDB call, so `_validate_interval` first restricts it to `<number> <unit>` pairs.
4. **Anything that drops something names its schema.** `DROP` resolves through the whole `search_path`, and could remove another installation's view in `public`. Use `_qualified(...)`. Catalog lookups filter on `self.active_schema`.
5. **Take `_metadata_lock` before touching `entities` or a `metadata_id`.** Renames, `write_entities` and the resolve + COPY part of a flush run as concurrent tasks. Without the lock, a flush can write rows to a `metadata_id` a rename just merged away.
6. **Never modify a queued item.** A failed batch is put back into the queue as the same dicts. An earlier version removed `entity_id` from them while resolving, and every retry was then dropped.
7. **Sanitize everything that reaches jsonb.** Null bytes in strings *and in keys*, keys that are not strings, `inf` and `nan`, dataclasses, unknown objects: see `_sanitize_obj`. One bad value fails the whole batch, at every retry.
8. **Never log or display credentials.** Use `_safe_target(db_url)` for anything shown to the user and `_redact_dsn()` for driver error messages. CodeQL flags `_safe_target(self.db_url)` as clear-text logging. That is a false positive: see [section 14](#14-static-analysis).
9. **Tell the user about any lasting failure** through a Repairs issue that clears itself. See [section 10](#10-repairs-issues).
10. **Log format:** `_LOGGER.<level>("[module.function] What happened: %s (%s)", e, type(e).__name__)`. The bracketed prefix makes a user's log greppable.

---

## 5. Development environment

- **Python 3.14**, the same as CI. Every `pytest-homeassistant-custom-component` release after 0.13.316 requires it. On 3.13, pip stays on an old release, and the tests run against a much older Home Assistant than CI does.
- **The Home Assistant version under test is set by `pytest-homeassistant-custom-component`** in `requirements_test.txt`. Each release of that package pins exactly one Home Assistant version.
- **Pin a release that maps to a stable Home Assistant, never a beta.** The test job gates releases, so it must not depend on a version that can still change. To see which Home Assistant a release pins:

  ```
  curl -s https://pypi.org/pypi/pytest-homeassistant-custom-component/<version>/json \
    | python3 -c "import json,sys; print([r for r in json.load(sys.stdin)['info']['requires_dist'] if r.startswith('homeassistant')])"
  ```

- `ruff` is pinned too (`requirements_test.txt`), with its rules listed in `ruff.toml`. A new ruff version therefore cannot fail the build on code nobody touched.

---

## 6. Tests

### 6.1 Running them

| Command | What it runs |
|---|---|
| `venv/bin/python -m pytest tests --ignore=tests/integration` | Unit tests only. No database needed. |
| `venv/bin/python -m pytest tests/integration` | Integration tests. Need the TimescaleDB container ([section 1](#1-quick-start)). |
| `venv/bin/python -m pytest tests` | Everything. Integration and upgrade tests skip themselves if no database answers. |
| `venv/bin/python -m pytest tests/test_collect_devices.py -k child` | One file, filtered by test name. |
| `venv/bin/python -m pytest tests/upgrade` | Upgrade tests from older releases, one per release ([6.6](#66-upgrade-tests)). Need the TimescaleDB container. |
| `venv/bin/ruff check . && venv/bin/ruff format --check .` | Lint and formatting, as in CI. `venv/bin/ruff format .` fixes the formatting. |

**To reproduce CI exactly:**

```
venv/bin/ruff check . && venv/bin/ruff format --check . \
  && venv/bin/python -m pytest tests --cov=custom_components/scribe --cov-report=term-missing --cov-fail-under=83
```

### 6.2 Unit tests vs integration tests

- **Unit tests** (`tests/*.py`) mock asyncpg: `tests/conftest.py` patches `asyncpg.create_pool` for every test. They check which SQL is sent and how failures are handled.
- **Integration tests** (`tests/integration/`) run the real writer against a real TimescaleDB and a real Home Assistant, and check what ends up in the tables. They have found bugs the mocked tests could not reach. **Prefer adding an integration test** whenever the behaviour depends on what PostgreSQL does.
  - Reuse the helpers in `tests/integration/conftest.py`: `writer`, `db`, `scribe_entry`, `clean_db`, `make_writer(hass, **overrides)`, `write_states`, `write_event`, `register_entity`, `sync_metadata`, `reconnect`, `entity_rows`, `table_exists`.
  - The database address is `SCRIBE_TEST_DSN`, by default `postgresql://postgres:scribe@127.0.0.1:55432/scribe`.
  - Gotchas:
    - The host must be the literal `127.0.0.1`, not `localhost`: pytest-socket only allows that address. For the same reason, fixtures that connect depend on `socket_enabled`.
    - The integration `conftest.py` replaces the global asyncpg mock with the real `create_pool`, with `max_inactive_connection_lifetime=0`. Otherwise asyncpg leaves a timer running after the test, and Home Assistant's lingering-timer check fails the test.
    - Query through `writer._pool`, not a new pool: only the writer's pool has the `jsonb` codec, and without it dict attributes fail.
    - `pg_class.relkind` comes back from asyncpg as **bytes**, so `== "r"` never matches. Cast it to text in SQL.
    - The statistics coordinators are off by default. A test that needs them must enable them.
    - **The test server runs with TimescaleDB's background jobs switched off** (`timescaledb.max_background_workers=0`, in the `docker run` of [section 1](#1-quick-start) and in every CI job). Otherwise the scheduler runs retention and compression jobs on the tables the tests create, and cancelling one because a test dropped its table has crashed the whole server: a segfault in `policy_retention` on `timescaledb:latest-pg17`. The policies still exist and can be checked. A test that needs a job to run calls it itself (`CALL run_job(...)`, `compress_chunk(...)`). For a container created without the flag: `docker exec scribe-test-db psql -U postgres -c "ALTER SYSTEM SET timescaledb.max_background_workers = 0"`, then `docker restart scribe-test-db`.

### 6.3 What CI checks

| Workflow | When | What |
|---|---|---|
| `tests.yaml` | push to `master`, every pull request, and before each release | `ruff check`, `ruff format --check`, the whole suite against a TimescaleDB service container, **coverage ≥ 83 %**, and a second run of `tests/integration` that **fails if any integration test was skipped**. A second job, `Minimum Home Assistant`, runs the same suite against the oldest Home Assistant `hacs.json` claims to support ([6.7](#67-testing-the-oldest-supported-home-assistant)). A third, `Upgrade from older releases`, runs `tests/upgrade` ([6.6](#66-upgrade-tests)) and fails if any of them was skipped; the first job leaves that folder out. |
| `validate.yaml` | push to `master`, pull requests, daily | HACS validation and hassfest (Home Assistant's manifest and translation checks). |
| `codeql.yaml` | push to `master`, pull requests, weekly | GitHub CodeQL security analysis. Results go to the Security tab. It does not block a merge. |
| `upstream-watch.yaml` | Mondays, or by hand | The suite against the **latest Home Assistant pre-release**, ignoring the pin. It only runs on a schedule, so a failure sends an email and never blocks anything. |
| `release.yaml` | a `v*` tag is pushed | See [section 9](#9-releasing). |
| `stale.yml` | daily | Marks issues stale after 21 days and closes them 7 days later (pull requests: 45 + 7). |

Some tests check consistency rather than behaviour. They fail when documentation or translations drift from the code:

- every key accepted by `CONFIG_SCHEMA` appears in the full YAML example **and** in the parameter table of all four READMEs, and those list nothing else (`tests/test_config_flow.py`);
- `en`, `fr`, `es` and `de` have every key of `strings.json`, no language file has a key that `strings.json` lacks, placeholders are the same in every language, and no `fr`/`es`/`de` text is still identical to English (`tests/test_config_flow.py`);
- every translation key passed to `_report_issue` / `_report_rename_issue` has a title and a description in `strings.json` and in the four documented languages (`tests/integration/test_repairs.py`).

### 6.4 Testing against another Home Assistant version

Use a separate virtual environment so `venv/` stays on the pinned version:

```
uv venv --python 3.14 /tmp/venv-ha
grep -v '^pytest-homeassistant-custom-component' requirements_test.txt > /tmp/req-ha.txt
uv pip install --python /tmp/venv-ha/bin/python -r /tmp/req-ha.txt \
    pytest-homeassistant-custom-component==<version>
/tmp/venv-ha/bin/python -m pytest tests --ignore=tests/integration -q
```

The `grep` is needed: pip refuses to install the pinned version and another one at the same time.

### 6.5 Finding Home Assistant deprecation warnings

Home Assistant reports most deprecations **through its logger**, as `Detected that custom integration 'scribe' ...`, and not as Python warnings. pytest does not print the log of a passing test, and a test that uses a mock instead of the real registry never triggers the warning at all. That is how #56 went through upstream-watch unnoticed. To list them:

```
venv/bin/python -m pytest tests --ignore=tests/integration -q \
    -o log_cli=true -o log_cli_level=WARNING 2>&1 \
  | grep -i "detected that custom integration" | sort -u
```

Run it with the environment from 6.4 to check an upcoming Home Assistant version. To keep a deprecation from coming back, write a test that uses the real helper (not a mock) and asserts `"deprecated" not in caplog.text`, like `tests/test_collect_devices.py`.

### 6.6 Upgrade tests

Every other test starts from an empty database. Existing users do not: they update with a database written by an older release. `tests/upgrade/` tests that path, with **one test per release**:

```
venv/bin/python -m pytest tests/upgrade             # every release
venv/bin/python -m pytest tests/upgrade -k v3.8.0   # one release
```

For each release, the `older_database` fixture (`tests/upgrade/conftest.py`):

1. creates a fresh database on the test server, with TimescaleDB enabled, as on a real installation;
2. checks out the release's tag in a temporary git worktree, and runs `tests/upgrade/seed.py` there **in a separate process**, so that **the release's own code** fills the database through its normal setup: a config entry, state changes, an event, the `flush` service. It has to be another process: two versions of `custom_components.scribe` cannot be imported by the same interpreter;
3. hands the database to `test_an_older_database_keeps_working` (`tests/upgrade/test_upgrade.py`), which runs with the **current code** and checks that Scribe:
   - starts without being blocked and without raising an error-level Repairs issue;
   - finds the old history through the `states` view;
   - keeps writing to the entities the old release registered;
   - carries the whole history through a rename;
   - has its hypertables and compression policies;
   - survives a restart;
4. removes the worktree and the database afterwards.

- **Releases tested**: the last patch of every stable minor since 3.2, read from the git tags by `releases()` in `conftest.py`. A new release is included automatically once it is tagged.
- **Needs** the test TimescaleDB from [section 1](#1-quick-start), with a role that can create databases (the default `postgres` can), **and the git tags**. Without a database the tests are skipped; in a shallow clone (no tags) there is nothing to test and they are skipped too. The CI job fetches the whole history and fails on any skip.
- `seed.py` is not named `test_*`, so pytest never collects it directly.
- **Databases created by 3.1 to 3.5 have no primary key on `states_raw`**, and no release ever added it. On them, the test skips the check that relies on it (duplicate rows ignored with `ON CONFLICT`).
- The old code runs on the pinned Home Assistant. If a future Home Assistant can no longer run an old release, its test fails with "`vX` could not fill the database with its own code". That is the old release failing, not a regression: raise `OLDEST` in `tests/upgrade/conftest.py` to stop testing it.

### 6.7 Testing the oldest supported Home Assistant

`hacs.json` names the oldest Home Assistant Scribe claims to support. The pinned release ([section 5](#5-development-environment)) is a recent one, and upstream-watch tests the newest, so nothing here sees the floor: a helper that only exists in a recent Home Assistant passes every check and reaches the users on the floor as a crash.

The `Minimum Home Assistant` CI job runs the suite (without `tests/upgrade`, a different question) against it. Two values in the job say which Home Assistant that is:

- `FLOOR`, the version in `hacs.json`. The job **fails if the two stop matching**, so raising the floor cannot silently leave this job testing a Home Assistant nobody supports any more.
- `PLUGIN`, the `pytest-homeassistant-custom-component` release pinning the oldest Home Assistant at or above the floor — today `0.13.317`, for Home Assistant 2026.3.1. Find it as in [section 5](#5-development-environment).

To run it locally, build a virtual environment on that plugin release as in [6.4](#64-testing-against-another-home-assistant-version), and run `pytest tests --ignore=tests/upgrade`.

**When raising the floor in `hacs.json`**: set `FLOOR` to the new version, set `PLUGIN` to the matching release, and go through [section 13](#13-home-assistant-compatibility) — that is the moment the compatibility shims listed there can go.

---

## 7. Branches, commits and pull requests

### 7.1 Branches

- **`master` is the only long-lived branch, and it must always be releasable.** Only merge what could ship as it is. Anything unfinished stays on its branch.
- **Every change is made on a short-lived branch**, merged into `master` through a pull request, then deleted. This includes documentation changes and release commits.
- Branch name: `<type>/<issue>-<short-description>`, with the same `<type>` as the commit ([7.2](#72-commit-messages)). Leave the issue number out when there is none.
  Examples: `fix/56-device-registry`, `feat/53-db-schema`, `docs/development-guide`, `release/4.1.0`.

### 7.2 Commit messages

```
<type>(<scope>): <summary>

<body>
```

- `<type>`: `fix`, `feat`, `perf`, `refactor`, `test`, `docs`, `i18n`, `ci`, `chore`. Add `!` after the scope for a breaking change: `feat(scribe)!: ...`.
- `<scope>`: the part of the code touched, usually the module: `writer`, `init`, `config_flow`, `query`, `stats`, `schema`, `view`, `diagnostics`, `system_health`, `migration`, `readme`, `retention`…
- `<summary>`: lowercase, no final period. It says what was wrong or what changes, as a user would notice it. Example: `fix(writer): chunk_time_interval could run SQL of its own`.
- `<body>`: what was wrong, what it caused for the user, and why this fix. Wrap lines at about 72 characters.
- Reference the issue in the pull request (`Closes #56`) rather than in each commit.

### 7.3 Everyday workflow

```
git switch master && git pull --ff-only          # start from an up-to-date master
git switch -c fix/56-device-registry             # one branch per change

# ... edit, then run the checks from section 6.1 ...
git add -p && git commit                         # as many commits as needed

git push -u origin fix/56-device-registry
gh pr create --base master --fill                # add "Closes #56" to the description
gh pr checks --watch                             # wait for CI
gh pr merge --merge --delete-branch              # merge, delete the branch (local and remote)
git switch master && git pull --ff-only
```

- **Merge method: "Create a merge commit"** (`--merge`). The branch's commits land on `master` unchanged, with the same SHAs CI tested, and a merge commit named after the pull request groups them. `git revert -m 1 <merge commit>` undoes the whole pull request.
- Use **"Squash and merge"** (`--squash`) instead when the branch has "wip" or "fix typo" commits, and write a proper message for the squashed commit. Do not use "Rebase and merge": it rewrites every commit.
- **Do not keep working on a merged branch**: start a new one from `master`.
- **To bring a branch up to date with `master`:** `git fetch origin && git rebase origin/master`, then `git push --force-with-lease`. Only force-push your own branches, never `master`.
- The repository deletes a branch on GitHub automatically once its pull request is merged.

### 7.4 Pull requests from contributors and from Dependabot

- **Contributors** open pull requests from their fork, and CI runs on them. For a first-time contributor, GitHub waits for you to click **Approve and run** before running CI. Review, ask for changes if needed, then usually **squash and merge**, rewriting the message to follow [7.2](#72-commit-messages). GitHub keeps the contributor as the author.
- **Dependabot** opens `ci:` pull requests once a month for GitHub Actions and for `requirements_test.txt`.
  - GitHub Actions and `ruff`: merge once CI is green. A new `ruff` can report new findings. Fix them in the same pull request or in a separate one.
  - `pytest-homeassistant-custom-component`: **check which Home Assistant it pins first** ([section 5](#5-development-environment)). If it pins a beta, close the pull request with a comment saying so. When a release that pins a stable version exists, bump to it on a branch of your own.

### 7.5 `master` is protected

`master` has a branch protection rule (GitHub → Settings → Branches), which applies to administrators too:

- **A pull request is required to change it**, with 0 required approvals: a single maintainer cannot approve their own pull request. A direct `git push` to `master` is refused.
- **These checks must pass before merging**: `Run Unit Tests`, `Upgrade from older releases`, `HACS`, `Hassfest`. CodeQL stays advisory.
- **Force pushes and deleting the branch are refused.**
- Branches do not have to be up to date with `master` before merging.

Tags are not affected: a release is still published by pushing a tag ([section 9](#9-releasing)).

A check listed as required must exist in every pull request's workflows, or that pull request waits for it forever. When adding or renaming a CI job that should be required, merge it first, then add it to the rule.

**In an emergency** (CI broken by something outside the repository, and a fix must land): lift the rule temporarily in Settings → Branches, or with `gh api -X DELETE repos/jonathan-gtd/scribe/branches/master/protection`, and put it back afterwards with the settings above.

### 7.6 Fixing a released version

If a stable release needs a fix and `master` already holds changes that are not ready, something unready was merged (see [7.1](#71-branches)). If it happens anyway:

```
git switch -c release/4.1 v4.1.0      # branch from the released tag
# fix, commit, bump the manifest to 4.1.1, add the changelog entry
git tag -a v4.1.1 -m "Scribe 4.1.1" && git push origin release/4.1 v4.1.1
```

Then bring the fix into `master` through a normal pull request (`git cherry-pick <sha>` onto a new branch from `master`).

---

## 8. Changelog

- **Every pull request that changes `custom_components/scribe/` in a way a user can notice adds its entry to `CHANGELOG.md`**, under `## [Unreleased]` at the top. Create the heading if it is not there.
- Sections, in this order and only those needed: `### Fixed`, `### Added`, `### Changed`. A breaking change also gets a `> **Breaking changes.**` block right under the version heading.
- One bullet per change. It starts with a **bold sentence saying what the user saw or gets**, followed by the explanation: what happened, in which situation, what the fix does. Name the issue and thank whoever reported it when there is one.
- Changes that reach no user (tests, CI, internal refactoring with no visible effect) get no entry.
- When releasing, `## [Unreleased]` becomes `## [X.Y.Z] - YYYY-MM-DD` ([section 9](#9-releasing)).

---

## 9. Releasing

### 9.1 What is released, and to whom

- **HACS installs only `custom_components/scribe/`.** A release that changes nothing there only delivers a new version number and an update notification, which is noise. **Only release when that folder changed**:

  ```
  git diff $(git describe --tags --abbrev=0) HEAD --stat -- custom_components/
  ```

  Empty output: there is nothing to release.
- Scribe is in the **HACS default store**. Every stable tag becomes an update notification for real installations.
- **Pre-release**: a tag ending in `aN`, `bN` or `rcN` (`v4.1.0rc1`) is published as a GitHub pre-release. HACS only offers it to users who enabled beta versions for Scribe. Use one to put code in the hands of testers without notifying everyone.
- **Version numbers**: `X.Y.Z` in `manifest.json`, with an optional `aN`/`bN`/`rcN` suffix and no separator. Major (`X`) for a breaking change, minor (`Y`) for new features, patch (`Z`) for fixes only. The tag is `v` followed by exactly the manifest version.

### 9.2 Steps

1. Start from an up-to-date `master`, and run the check from 9.1.
2. `git switch -c release/X.Y.Z`
3. In `custom_components/scribe/manifest.json`, set `"version": "X.Y.Z"`.
4. In `CHANGELOG.md`, rename `## [Unreleased]` to `## [X.Y.Z] - YYYY-MM-DD` (today's date). For a stable release that follows an `rc`, update the date of the existing `[X.Y.Z]` entry and add whatever was merged since the `rc`.
5. Commit with the message `chore: release X.Y.Z`, push, open the pull request, merge it once CI is green.
6. Tag the merged commit on `master` and push the tag. **Pushing the tag is the release**:

   ```
   git switch master && git pull --ff-only
   git tag -a vX.Y.Z -m "Scribe X.Y.Z" -m "<two or three lines: what this release brings>"
   git push origin vX.Y.Z
   ```

7. Follow the **Release** workflow in the Actions tab (or `gh run watch`). It:
   1. runs the whole `tests.yaml` workflow on the tagged commit: the test suite and the upgrade tests;
   2. fails if the tag is not `v` + the version in `manifest.json`;
   3. zips `custom_components/scribe` into `scribe.zip`;
   4. creates the GitHub release with generated notes and the zip, marked as a pre-release if the tag ends in `aN`/`bN`/`rcN`.
8. Check the release on GitHub (`gh release view vX.Y.Z`). HACS picks it up from there.

**If the workflow fails before the release is created** (wrong manifest version, failing tests), delete the tag, fix the problem through a pull request, then tag again:

```
git push --delete origin vX.Y.Z && git tag -d vX.Y.Z
```

**Do not delete a stable release once it is published.** Users may already have installed it. Publish a new patch version instead.

---

## 10. Repairs issues

Scribe reports conditions it cannot fix itself in Settings → System → Repairs, so the user does not have to read the log. All of them are raised and cleared in `writer.py`.

| Issue id | Translation key | Severity | Raised when | Cleared when |
|---|---|---|---|---|
| `db_unreachable` | `db_unreachable` | error | the pool cannot be created | a connection succeeds, or a flush succeeds after failures |
| `write_failing` | `write_failing` | error | 3 flushes in a row failed (`WRITE_FAILURE_ISSUE_THRESHOLD`) | the next successful flush |
| `buffer_full` | `buffer_full` | error | the queue is full during an outage, or after a failed batch is put back | a successful flush that leaves the queue below its maximum |
| `data_dropped` | `data_dropped` | error | a batch is dropped because `buffer_on_failure` is off | the next successful flush |
| `schema_failed` | `schema_failed` | error | `init_db` raised, usually missing privileges | the next successful `init_db` |
| `schema_unavailable` | `schema_unavailable` | error | `db_schema` is not the schema actually in use | the schema check passes |
| `legacy_schema` | `legacy_schema` | error | a pre-3.0 database is detected | the next successful `init_db` |
| `view_failed` | `view_failed` | error | the `states` view cannot be created | the view is created, or states are no longer recorded |
| `no_timescaledb` | `no_timescaledb` | warning | the extension is missing and cannot be enabled | the extension is found |
| `ssl_degraded` | `ssl_degraded` | warning | a configured certificate could not be loaded | the TLS context builds without problems, or TLS is turned off |
| `no_hypertable_<table>` | `no_hypertable` | warning | TimescaleDB is installed but the table is not a hypertable | the table is a hypertable, or TimescaleDB is absent, or the table is no longer recorded |
| `no_compression_<table>` | `no_compression` | warning | the hypertable has no compression policy | a compression policy exists, or the table is no longer recorded |
| `retention_failed_<table>` | `retention_failed` | error | the retention interval is invalid, or the policy could not be applied | the policy is applied, or retention is emptied, or the table is no longer recorded |
| `rename_collision_<entity_id>` | `rename_refused_live`, `rename_refused_unprovable` (warning), `rename_failed` (error) | see key | a rename is refused or fails | a later rename to the same `entity_id` succeeds |

The checks done by `init_db` (schema, pre-3.0 database, view, TimescaleDB, hypertables, compression, retention) only run at startup, so **the issues they raise are re-checked at the next start**: a Home Assistant restart or any change in the options flow. The flush and connection issues clear themselves while Scribe is running.

**A reload must leave the panel as a restart would.** Every issue is non-persistent, so a restart turns them all inactive and setup raises again whatever is still true. A reload does not: an issue stays up until something clears it. So a check that a new configuration *skips* must clear what it said last time — a table no longer recorded retires its hypertable, compression and retention issues, and turning states off retires `view_failed`. Removing Scribe retires every issue (`async_remove_entry`), since the writer that would have cleared them is gone.

**Adding an issue:**

1. Call `self._report_issue(issue_id, translation_key, placeholders, severity=...)` where the condition is detected, and `self._clear_issue(issue_id)` where it stops being true. Never call `ir.async_create_issue` directly: these two helpers catch their own errors, so a Repairs problem can never break a write.
2. **The issue must clear itself.** An issue that stays up after the problem is gone teaches users to ignore the panel. Test both directions in `tests/integration/test_repairs.py`.
3. For a failure that can be temporary, wait for a threshold before raising, as `write_failing` does. A database restart must not raise anything.
4. Use a fixed `issue_id` so a repeated condition updates one issue. Add a suffix (`_<table>`, `_<entity_id>`) only when several instances can be true at the same time.
5. The description says **what it means for the user and how to fix it**, not only the error. Add `issues.<translation_key>.title` and `.description` to `strings.json`, `translations/en.json`, `fr.json`, `es.json` and `de.json`, with the same `{placeholders}` everywhere.
6. Add a row to the **Repairs** table under Troubleshooting in all four READMEs. The issue's "Learn more" link points there (`ISSUE_LEARN_MORE_URL`).

---

## 11. Checklist: adding a configuration option

1. `const.py`: add `CONF_<NAME> = "<name>"` and `DEFAULT_<NAME> = ...`.
2. `__init__.py`: add the key to `CONFIG_SCHEMA` (for YAML) and read it in `_resolve_settings` with `get_config(CONF_<NAME>, DEFAULT_<NAME>)`.
3. If the writer uses it: add a field with its default to `WriterConfig` in `writer.py`, and read it in `ScribeWriter.__init__`.
4. `config_flow.py`: add it to the right step of `ScribeOptionsFlowHandler`:

   | Step | Title | Holds |
   |---|---|---|
   | `init` | Recording & Filtering | `record_*`, include/exclude filters, `exclude_attributes` |
   | `performance` | Performance | `batch_size`, `flush_interval`, `max_queue_size`, `buffer_on_failure` |
   | `stats` | Statistics Sensors | `enable_stats_*`, `stats_*_interval` |
   | `metadata` | Metadata Tables | `enable_table_*` |
   | `advanced` | Advanced (TimescaleDB & SSL) | storage intervals, retention, `db_schema`, TLS |

   A number or list field also goes into `_coerce_options` (the UI returns numbers as floats and single values as strings). Validation errors go in `errors[...]` with a key under `options.error` in `strings.json`.
5. `strings.json` + `translations/en.json`, `fr.json`, `es.json`, `de.json`: add the label under `options.step.<step>.data` and the help text under `data_description`.
6. The four READMEs: add the key to the **Full Configuration** YAML example and to the **Configuration Parameters** table. The tests fail otherwise.
7. If it helps with a bug report, add it to `diagnostics.py`.
8. Tests: that the setting is resolved (`tests/test_config_options.py`, `tests/integration/test_yaml_options_precedence.py`) and, if it acts on the database, an integration test.
9. A `CHANGELOG.md` entry under `### Added`.

---

## 12. Translations and READMEs

- `strings.json` is the reference, in English. `translations/en.json` is a copy of it that Home Assistant loads.
- **Documented languages: `en`, `fr`, `es`, `de`**, the four READMEs. They must be complete and really translated: the tests in [6.3](#63-what-ci-checks) enforce it. A string may stay identical to English only if it is listed in `ALLOWED_IDENTICAL_TO_ENGLISH` in `tests/test_config_flow.py`.
- **Other languages** (`da`, `it`, `ja`, `nl`, `pl`, `pt`, `ru`, `sk`, `sv`) are partial and come from contributors. Home Assistant falls back to English for any missing key. They may not contain keys that `strings.json` lacks.
- **The four READMEs have the same structure.** A change to one (a new option, a Repairs row, a new section) is made in all four in the same pull request.

---

## 13. Home Assistant compatibility

The minimum Home Assistant version is `homeassistant` in `hacs.json`. Code that works around a difference between Home Assistant versions is listed here, with the condition that makes it removable. Raising the minimum is the moment to go through this table. Each entry is also marked in the code with `DEVELOPMENT.md`, so `grep -rn DEVELOPMENT.md custom_components` finds them.

| What | Where | Why | Remove when |
|---|---|---|---|
| `isinstance(devices, Mapping)` before iterating `device_registry.devices` | `_collect_devices` in `__init__.py` | Up to 2026.8, `devices` is a mapping of id to entry. From 2026.9 it is a collection of the entries: iterating it yields them, and every mapping method (`.values()`, `[id]`, `.get()`) logs a deprecation and stops working in **2027.9** (#56). | The minimum is **2026.9** or later: `devices` is then never a mapping, so iterate it directly. |
| `getattr(registry, "child_devices", ())` | `_collect_devices` in `__init__.py` | Child devices appeared in 2026.9. They are kept outside `devices`, and entities can belong to them. | The minimum is **2026.9** or later: read `registry.child_devices` directly. |
| `_CHILD_DEVICE_ENTRY = getattr(dr, "ChildDeviceEntry", ())` | `_device_row` in `__init__.py` | A child device has no `model`, `manufacturer` or `sw_version`. Reading one returns `None` but logs a deprecation that stops working in 2027.9. The class does not exist before 2026.9. | The minimum is **2026.9** or later: use `dr.ChildDeviceEntry` directly. Keep the `isinstance` check itself. |

Nothing has to change *by* 2027.9: once these workarounds are in place, the code keeps working on every version. Removing them is only cleanup, and it drops support for anything older than 2026.9.

**When Home Assistant deprecates something Scribe uses:**

1. Find what triggers it with [6.5](#65-finding-home-assistant-deprecation-warnings), against the Home Assistant version that introduced it ([6.4](#64-testing-against-another-home-assistant-version)).
2. Read the new API in Home Assistant's source for **that** release (`https://raw.githubusercontent.com/home-assistant/core/<version>/homeassistant/...`). The `dev` branch may already be further ahead.
3. Write code that works on both sides of the change, from the minimum in `hacs.json` to the new version. Add a row to the table above and mark the code.
4. Run the whole suite on the pinned version **and** on the new version.

---

## 14. Static analysis

- **ruff**: the rules are in `ruff.toml`, together with the rules deliberately left out and why. Adding a rule is a change of its own, with its own diff.
- **CodeQL**: three `py/clear-text-logging-sensitive-data` alerts on `_safe_target(self.db_url)` are **dismissed as false positives**. `_safe_target()` builds its output from the host, port and database name only, so the credentials cannot appear in it. When those lines move, CodeQL reopens the alerts under new numbers. Dismiss them again with the same reason. Do not change the code to silence them.
- **SonarCloud**: `sonar-project.properties` lists the rules that are silenced, each with the reason it does not apply.

---

## 15. Migration scripts and helper scripts

**`migration/`**: stand-alone scripts that import history into a Scribe database. They are run by hand and are not part of the integration:

- `influx2scribe.py`, `ltss2scribe.py`, `recorder2scribe.py` (the recorder from PostgreSQL or SQLite; `states` only).
- Configuration comes from `migration/.env`. Copy `migration/.env.example`; the `.env` file is ignored by git.
- `preflight.py` checks, before a script writes anything, that the target database has been set up by Scribe (constraints on `entities` and `states_raw`). Start Scribe once against the database before running a script.
- Usage for users: the **Migration** section of the README.

**`scripts/`**: local helper shell scripts:

| Script | What it does |
|---|---|
| `run_tests.sh` | `pytest tests -v` with `venv/` if present. Includes the integration tests. |
| `deploy.sh <custom_components dir> [container]` | Copies `custom_components/scribe` into a Home Assistant install, **deleting what was there**, then `docker restart`s the container (default `homeassistant`). |
| `setup_scribe_db.sh [admin_user] [host] [db] [db_user] [db_pass]` | Creates the database user and the database, grants privileges and enables TimescaleDB. |
| `drop_db.sh [user] [host] [db]` | **Deletes a database** after a confirmation prompt. |
