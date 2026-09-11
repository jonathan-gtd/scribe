"""Open a database filled by an older release with the current code.

One test per release, from `releases()` in conftest.py: the last patch of every
stable minor since 3.2. The `older_database` fixture has that release fill a
fresh database with its own code; this test then checks that the current code
picks it up where the release left it. Run them with `pytest tests/upgrade`.
"""

import asyncpg
import pytest
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers import issue_registry as ir
from pytest_homeassistant_custom_component.common import MockConfigEntry

from .conftest import releases

RELEASES = releases() or [
    pytest.param(
        None,
        marks=pytest.mark.skip(reason="no release tags: a shallow clone has none"),
    )
]


async def _fetchval(dsn, sql):
    conn = await asyncpg.connect(dsn)
    try:
        return await conn.fetchval(sql)
    finally:
        await conn.close()


async def _count_states(dsn, entity_id):
    return await _fetchval(
        dsn, f"SELECT count(*) FROM states WHERE entity_id = '{entity_id}'"
    )


async def _set_up(hass, dsn):
    entry = MockConfigEntry(
        domain="scribe",
        data={"db_url": dsn, "record_states": True, "record_events": True},
        entry_id="upgrade_entry",
    )
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    return entry, hass.data["scribe"][entry.entry_id]["writer"]


async def _flush(hass):
    await hass.async_block_till_done()
    await hass.services.async_call("scribe", "flush", blocking=True)
    await hass.async_block_till_done()


def _active_scribe_issues(hass):
    return {
        issue.issue_id: issue.severity
        for (domain, _), issue in ir.async_get(hass).issues.items()
        if domain == "scribe" and issue.active
    }


@pytest.mark.parametrize("release", RELEASES)
async def test_an_older_database_keeps_working(hass, older_database):
    dsn = older_database
    registry = er.async_get(hass)
    registry.async_get_or_create(
        "sensor", "demo", "upgrade-temp", suggested_object_id="upgrade_temp"
    )
    registry.async_get_or_create(
        "sensor", "demo", "upgrade-old", suggested_object_id="upgrade_old"
    )

    # Starts, records, and is not blocked — neither mistaken for a pre-3.0
    # database nor stuck on a schema.
    entry, writer = await _set_up(hass, dsn)
    assert writer._pool is not None
    assert writer._connected, writer._last_error
    assert not writer._legacy_blocked, "mistaken for a pre-3.0 database"
    assert not writer._schema_blocked
    issues = _active_scribe_issues(hass)
    assert not [i for i, s in issues.items() if s == ir.IssueSeverity.ERROR], issues

    # The history written by the older release is there, through the view.
    assert await _count_states(dsn, "sensor.upgrade_temp") == 20
    assert await _count_states(dsn, "sensor.upgrade_old") == 20
    assert (
        await _fetchval(
            dsn, "SELECT count(*) FROM events WHERE event_type = 'upgrade_event'"
        )
        == 1
    )

    # New states land on the entity the older release registered.
    old_id = await _fetchval(
        dsn, "SELECT id FROM entities WHERE entity_id = 'sensor.upgrade_temp'"
    )
    for i in range(5):
        hass.states.async_set("sensor.upgrade_temp", str(100 + i))
    hass.bus.async_fire("upgrade_event", {"n": 2})
    await _flush(hass)
    assert await _count_states(dsn, "sensor.upgrade_temp") == 25
    assert (
        await _fetchval(
            dsn, "SELECT count(*) FROM events WHERE event_type = 'upgrade_event'"
        )
        == 2
    )
    assert (
        await _fetchval(
            dsn, "SELECT id FROM entities WHERE entity_id = 'sensor.upgrade_temp'"
        )
        == old_id
    )

    # A rename carries the whole history, the older release's rows included.
    registry.async_update_entity(
        "sensor.upgrade_temp", new_entity_id="sensor.upgrade_renamed"
    )
    await hass.async_block_till_done()
    assert await _count_states(dsn, "sensor.upgrade_renamed") == 25
    assert await _count_states(dsn, "sensor.upgrade_temp") == 0

    # A batch overlapping rows already written is retried with ON CONFLICT,
    # which needs the (metadata_id, time) primary key. Databases created by
    # 3.1 to 3.5 do not have it — no release ever added it afterwards — so
    # there a duplicate is simply written, as it always was on them.
    if await _fetchval(
        dsn,
        "SELECT count(*) FROM pg_constraint "
        "WHERE conrelid = 'states_raw'::regclass AND contype = 'p'",
    ):
        conn = await asyncpg.connect(dsn)
        try:
            row = await conn.fetchrow(
                "SELECT time, metadata_id FROM states_raw ORDER BY time LIMIT 1"
            )
        finally:
            await conn.close()
        async with writer._metadata_lock:
            await writer._copy_batch(
                [(row["time"], row["metadata_id"], "duplicate", None, None)], []
            )

    # The storage features are in place on the older tables.
    assert (
        await _fetchval(
            dsn,
            "SELECT count(*) FROM timescaledb_information.hypertables "
            "WHERE hypertable_name IN ('states_raw', 'events')",
        )
        == 2
    )
    assert (
        await _fetchval(
            dsn,
            "SELECT count(*) FROM timescaledb_information.jobs "
            "WHERE proc_name = 'policy_compression' "
            "AND hypertable_name IN ('states_raw', 'events')",
        )
        == 2
    )
    assert await _fetchval(
        dsn, "SELECT to_regclass('states_raw_meta_time_idx') IS NULL"
    )

    # And it survives a restart on the upgraded database.
    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
    entry, writer = await _set_up(hass, dsn)
    assert writer._connected and not writer._legacy_blocked
    hass.states.async_set("sensor.upgrade_renamed", "7")
    await _flush(hass)
    assert await _count_states(dsn, "sensor.upgrade_renamed") == 26
    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
