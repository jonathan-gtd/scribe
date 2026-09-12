"""The backfill scripts in `migration/`, against a real database.

`influx2scribe`, `ltss2scribe` and `recorder2scribe` write straight into
`entities` and `states_raw` with psycopg2, bypassing everything the writer
does. Nothing tested them: they were free to drift from the schema Scribe
creates, and the failure mode is a wall of per-row errors — or worse, a
successful migration whose rows do not look like the ones Scribe writes.

Each script reads its whole configuration at import time, so every test here
sets the environment first and imports the module fresh.
"""

import importlib
import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import psycopg2
import pytest

from tests.integration.conftest import DSN

MIGRATION_DIR = Path(__file__).resolve().parents[2] / "migration"

START = datetime(2026, 8, 1, 0, 0, tzinfo=timezone.utc)
END = datetime(2026, 8, 1, 6, 0, tzinfo=timezone.utc)


def _dsn_parts():
    """The test DSN, as the host/port/db/user/password the scripts expect."""
    from urllib.parse import urlparse

    parsed = urlparse(DSN)
    return {
        "host": parsed.hostname,
        "port": str(parsed.port),
        "database": parsed.path.lstrip("/"),
        "user": parsed.username,
        "password": parsed.password,
    }


@pytest.fixture
def migration_env(monkeypatch, tmp_path):
    """Point a script at the test database, from a directory it may write to.

    The scripts call `logging.basicConfig` with a `FileHandler` and
    `load_dotenv()` at import, so the working directory decides where their log
    lands and whether a stray `.env` on this machine leaks into the test.
    """
    parts = _dsn_parts()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("SCRIBE_HOST", parts["host"])
    monkeypatch.setenv("SCRIBE_PORT", parts["port"])
    monkeypatch.setenv("SCRIBE_DB", parts["database"])
    monkeypatch.setenv("SCRIBE_USER", parts["user"])
    monkeypatch.setenv("SCRIBE_PASS", parts["password"])
    monkeypatch.setenv("MIGRATION_START_TIME", START.isoformat())
    monkeypatch.setenv("MIGRATION_END_TIME", END.isoformat())
    monkeypatch.setenv("CHUNK_SIZE", "4")
    monkeypatch.setenv("PURGE_DESTINATION", "False")
    return parts


def load_script(name):
    """Import one migration script fresh, with its configuration already set."""
    sys.path.insert(0, str(MIGRATION_DIR))
    try:
        for module in (name, "preflight"):
            sys.modules.pop(module, None)
        return importlib.import_module(name)
    finally:
        sys.path.remove(str(MIGRATION_DIR))


def scribe_rows():
    """Every migrated state, resolved through `entities`, oldest first."""
    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT e.entity_id, s.time, s.state, s.value, s.attributes "
            "FROM states_raw s JOIN entities e ON e.id = s.metadata_id "
            "ORDER BY s.time, e.entity_id"
        )
        return cur.fetchall()


# --------------------------------------------------------------------------
# preflight
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_preflight_accepts_the_schema_scribe_creates(db):
    """The constraints the scripts' ON CONFLICT clauses rely on."""
    preflight = load_script("preflight")
    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        preflight.preflight_scribe_schema(cur)  # must not raise or exit


@pytest.mark.asyncio
async def test_preflight_refuses_a_database_scribe_never_built(db):
    """Better one clear message than a per-chunk error for every row."""
    preflight = load_script("preflight")
    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS states_raw CASCADE")
        conn.commit()
        with pytest.raises(SystemExit):
            preflight.preflight_scribe_schema(cur)


# --------------------------------------------------------------------------
# recorder2scribe
# --------------------------------------------------------------------------


def build_recorder_sqlite(path, rows, schema_version=53):
    """A Home Assistant recorder database, reduced to what the script reads."""
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE schema_changes (change_id INTEGER PRIMARY KEY, schema_version INTEGER);
        CREATE TABLE states_meta (metadata_id INTEGER PRIMARY KEY, entity_id TEXT);
        CREATE TABLE state_attributes (attributes_id INTEGER PRIMARY KEY, shared_attrs TEXT);
        CREATE TABLE states (
            state_id INTEGER PRIMARY KEY,
            metadata_id INTEGER,
            state TEXT,
            last_updated_ts REAL,
            attributes_id INTEGER
        );
        """
    )
    conn.execute("INSERT INTO schema_changes VALUES (1, ?)", (schema_version,))

    metadata = {}
    for index, (entity_id, state, when, attributes) in enumerate(rows, start=1):
        if entity_id not in metadata:
            metadata[entity_id] = len(metadata) + 1
            conn.execute(
                "INSERT INTO states_meta VALUES (?, ?)",
                (metadata[entity_id], entity_id),
            )
        conn.execute(
            "INSERT INTO state_attributes VALUES (?, ?)",
            (index, json.dumps(attributes)),
        )
        conn.execute(
            "INSERT INTO states VALUES (?, ?, ?, ?, ?)",
            (index, metadata[entity_id], state, when.timestamp(), index),
        )
    conn.commit()
    conn.close()


@pytest.mark.asyncio
async def test_recorder_sqlite_backfills_states_and_entities(
    db, migration_env, monkeypatch, tmp_path
):
    """The SQLite path the README never documented, end to end."""
    recorder = tmp_path / "home-assistant_v2.db"
    build_recorder_sqlite(
        recorder,
        [
            ("sensor.temperature", "21.5", START + timedelta(hours=1), {"unit": "°C"}),
            ("sensor.temperature", "22.0", START + timedelta(hours=5), {"unit": "°C"}),
            ("binary_sensor.door", "on", START + timedelta(hours=2), {}),
        ],
    )
    monkeypatch.setenv("RECORDER_TYPE", "sqlite")
    monkeypatch.setenv("RECORDER_DB_PATH", str(recorder))

    load_script("recorder2scribe").migrate()

    rows = scribe_rows()
    assert len(rows) == 3
    assert {row[0] for row in rows} == {"sensor.temperature", "binary_sensor.door"}

    by_entity = {(row[0], row[1]): row for row in rows}
    numeric = by_entity[("sensor.temperature", START + timedelta(hours=1))]
    assert numeric[3] == 21.5
    assert numeric[2] is None, (
        "a numeric state belongs in `value`, like Scribe writes it"
    )

    text = by_entity[("binary_sensor.door", START + timedelta(hours=2))]
    assert text[2] == "on"
    assert text[3] is None


@pytest.mark.asyncio
async def test_recorder_migration_can_be_run_twice(
    db, migration_env, monkeypatch, tmp_path
):
    """Re-running after an interruption must not duplicate history.

    The whole point of `ON CONFLICT DO NOTHING` on `(metadata_id, time)`: the
    documented workflow is to migrate while Scribe is already recording.
    """
    recorder = tmp_path / "recorder.db"
    build_recorder_sqlite(
        recorder,
        [("sensor.temperature", "21.5", START + timedelta(hours=1), {})],
    )
    monkeypatch.setenv("RECORDER_TYPE", "sqlite")
    monkeypatch.setenv("RECORDER_DB_PATH", str(recorder))

    load_script("recorder2scribe").migrate()
    load_script("recorder2scribe").migrate()

    assert len(scribe_rows()) == 1


@pytest.mark.asyncio
async def test_recorder_refuses_a_schema_still_migrating(
    db, migration_env, monkeypatch, tmp_path
):
    """A recorder mid-migration would be read half-converted."""
    recorder = tmp_path / "old.db"
    build_recorder_sqlite(
        recorder,
        [("sensor.temperature", "21.5", START + timedelta(hours=1), {})],
        schema_version=42,
    )
    monkeypatch.setenv("RECORDER_TYPE", "sqlite")
    monkeypatch.setenv("RECORDER_DB_PATH", str(recorder))

    with pytest.raises(SystemExit):
        load_script("recorder2scribe").migrate()

    assert scribe_rows() == []


@pytest.mark.asyncio
async def test_recorder_only_migrates_the_configured_window(
    db, migration_env, monkeypatch, tmp_path
):
    """MIGRATION_START_TIME and MIGRATION_END_TIME are a filter, not a hint."""
    recorder = tmp_path / "recorder.db"
    build_recorder_sqlite(
        recorder,
        [
            ("sensor.temperature", "1", START - timedelta(hours=1), {}),
            ("sensor.temperature", "2", START + timedelta(hours=1), {}),
            ("sensor.temperature", "3", END + timedelta(hours=1), {}),
        ],
    )
    monkeypatch.setenv("RECORDER_TYPE", "sqlite")
    monkeypatch.setenv("RECORDER_DB_PATH", str(recorder))

    load_script("recorder2scribe").migrate()

    rows = scribe_rows()
    assert [row[3] for row in rows] == [2.0]


# --------------------------------------------------------------------------
# ltss2scribe
# --------------------------------------------------------------------------


def build_ltss_table(rows):
    """The single table LTSS records into, in the test database."""
    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS ltss")
        cur.execute(
            "CREATE TABLE ltss (time TIMESTAMPTZ, entity_id TEXT, "
            "state TEXT, attributes JSONB)"
        )
        for entity_id, state, when, attributes in rows:
            cur.execute(
                "INSERT INTO ltss VALUES (%s, %s, %s, %s)",
                (when, entity_id, state, json.dumps(attributes)),
            )
        conn.commit()


@pytest.mark.asyncio
async def test_ltss_backfills_states_and_entities(db, migration_env, monkeypatch):
    """Source and destination are the same server here; the script uses two connections."""
    build_ltss_table(
        [
            ("sensor.humidity", "48.2", START + timedelta(hours=1), {"unit": "%"}),
            ("binary_sensor.window", "off", START + timedelta(hours=3), {}),
        ]
    )
    parts = migration_env
    monkeypatch.setenv("LTSS_HOST", parts["host"])
    monkeypatch.setenv("LTSS_PORT", parts["port"])
    monkeypatch.setenv("LTSS_DB", parts["database"])
    monkeypatch.setenv("LTSS_USER", parts["user"])
    monkeypatch.setenv("LTSS_PASS", parts["password"])

    load_script("ltss2scribe").migrate()

    rows = scribe_rows()
    assert {row[0] for row in rows} == {"sensor.humidity", "binary_sensor.window"}

    by_entity = {row[0]: row for row in rows}
    assert by_entity["sensor.humidity"][3] == 48.2
    assert by_entity["binary_sensor.window"][2] == "off"
    assert by_entity["binary_sensor.window"][3] is None
    assert by_entity["sensor.humidity"][4] == {"unit": "%"}


@pytest.mark.asyncio
async def test_ltss_migration_can_be_run_twice(db, migration_env, monkeypatch):
    """Same guarantee as the recorder script, through a different INSERT."""
    build_ltss_table([("sensor.humidity", "48.2", START + timedelta(hours=1), {})])
    parts = migration_env
    for key in ("HOST", "PORT", "DB", "USER", "PASS"):
        monkeypatch.setenv(
            f"LTSS_{key}",
            parts[{"DB": "database", "PASS": "password"}.get(key, key.lower())],
        )

    load_script("ltss2scribe").migrate()
    load_script("ltss2scribe").migrate()

    assert len(scribe_rows()) == 1


# --------------------------------------------------------------------------
# influx2scribe
# --------------------------------------------------------------------------


def influx_record(when, entity_id, domain, value=None, state=None, unit="state"):
    """One row as `influxdb_client` hands it back: values, plus get_time()."""
    values = {
        "_time": when,
        "entity_id": entity_id,
        "domain": domain,
        "_measurement": unit,
    }
    if value is not None:
        values["value"] = value
    if state is not None:
        values["state"] = state
    return SimpleNamespace(values=values, get_time=lambda: when)


@pytest.fixture
def fake_influx(monkeypatch):
    """Stand in for `influxdb_client`, so the script can be imported and run.

    InfluxDB is the one source with no server to point at here. What matters is
    the same as for the others: that what it writes matches Scribe's schema.
    """
    records = []

    class _QueryApi:
        def query(self, query, org=None):
            start, stop = (
                datetime.fromisoformat(part)
                for part in query.split("start: ")[1].split(")")[0].split(", stop: ")
            )
            kept = [r for r in records if start <= r.get_time() < stop]
            return [SimpleNamespace(records=kept)] if kept else []

    class _Client:
        def __init__(self, **kwargs):
            pass

        def query_api(self):
            return _QueryApi()

        def close(self):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    monkeypatch.setitem(
        sys.modules, "influxdb_client", SimpleNamespace(InfluxDBClient=_Client)
    )
    return records


@pytest.mark.asyncio
async def test_influx_backfills_states_and_entities(
    db, migration_env, monkeypatch, fake_influx
):
    """The record shapes InfluxDB hands back, turned into states_raw rows."""
    monkeypatch.setenv("INFLUX_URL", "http://127.0.0.1:8086")
    monkeypatch.setenv("INFLUX_TOKEN", "token")
    monkeypatch.setenv("INFLUX_ORG", "org")
    monkeypatch.setenv("INFLUX_BUCKET", "homeassistant")

    fake_influx.append(
        influx_record(
            START + timedelta(hours=1), "temperature", "sensor", value=21.5, unit="°C"
        )
    )
    fake_influx.append(
        influx_record(START + timedelta(hours=2), "door", "binary_sensor", state="on")
    )

    load_script("influx2scribe").migrate()

    rows = scribe_rows()
    assert {row[0] for row in rows} == {"sensor.temperature", "binary_sensor.door"}

    by_entity = {row[0]: row for row in rows}
    assert by_entity["sensor.temperature"][3] == 21.5
    assert by_entity["sensor.temperature"][4]["unit_of_measurement"] == "°C"
    assert by_entity["binary_sensor.door"][2] == "on"
    assert by_entity["binary_sensor.door"][3] is None


def test_influx_prefixes_the_entity_id_with_its_domain(fake_influx):
    """Influx stores `entity_id` without the domain; states_raw needs it whole."""
    influx = load_script("influx2scribe")

    assert (
        influx._record_entity_id(
            SimpleNamespace(values={"entity_id": "temperature", "domain": "sensor"})
        )
        == "sensor.temperature"
    )
    assert (
        influx._record_entity_id(
            SimpleNamespace(
                values={"entity_id": "sensor.temperature", "domain": "sensor"}
            )
        )
        == "sensor.temperature"
    )
    assert influx._record_entity_id(SimpleNamespace(values={})) is None


@pytest.mark.asyncio
async def test_ltss_fills_state_as_well_as_value_for_a_number(
    db, migration_env, monkeypatch
):
    """Pinned, not endorsed: this is where the scripts diverge from Scribe.

    `_state_row` in the integration leaves `state` NULL for a numeric state and
    puts the number in `value`. `recorder2scribe` does the same, but
    `ltss2scribe` and `influx2scribe` also write the text into `state`, so a
    migrated history and a recorded one do not look alike in the same table:
    `SELECT state FROM states` returns the number for migrated rows and NULL
    for everything Scribe wrote afterwards. Changing it is a data decision, so
    the behaviour is nailed down here rather than quietly altered.
    """
    build_ltss_table([("sensor.humidity", "48.2", START + timedelta(hours=1), {})])
    parts = migration_env
    monkeypatch.setenv("LTSS_HOST", parts["host"])
    monkeypatch.setenv("LTSS_PORT", parts["port"])
    monkeypatch.setenv("LTSS_DB", parts["database"])
    monkeypatch.setenv("LTSS_USER", parts["user"])
    monkeypatch.setenv("LTSS_PASS", parts["password"])

    load_script("ltss2scribe").migrate()

    entity_id, _, state, value, _ = scribe_rows()[0]
    assert (state, value) == ("48.2", 48.2)


def test_influx_fills_state_as_well_as_value_for_a_number(fake_influx):
    """The same divergence, in the other script. See the LTSS test above."""
    influx = load_script("influx2scribe")

    assert influx._record_state(SimpleNamespace(values={"value": 21.5})) == (
        "21.5",
        21.5,
    )
    assert influx._record_state(SimpleNamespace(values={"state": "on"})) == ("on", None)


@pytest.fixture(autouse=True)
def _drop_ltss():
    """The LTSS source table is not Scribe's, so the shared teardown ignores it."""
    yield
    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS ltss")
        conn.commit()
