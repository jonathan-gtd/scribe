"""Hourly and daily summaries, against a real TimescaleDB.

They are continuous aggregates: the database maintains them from what is
written. What matters here is that they exist, that they hold the right
numbers, that Scribe takes them away when the option goes off, and that a
database without TimescaleDB is not broken by asking for them.
"""

from datetime import timedelta

import asyncpg
import pytest

from custom_components.scribe.writer import ISSUE_ROLLUPS_FAILED

from .conftest import BASE_TIME, make_writer


async def _relation_exists(pool, name):
    async with pool.acquire() as conn:
        return await conn.fetchval("SELECT to_regclass($1) IS NOT NULL", name)


async def _refresh(pool, aggregate):
    """Fill the aggregate now, instead of waiting for its policy to run."""
    async with pool.acquire() as conn:
        await conn.execute(
            f"CALL refresh_continuous_aggregate('{aggregate}', NULL, NULL)"
        )


async def _write_hourly_states(writer, entity_id, values):
    """One state per hour from BASE_TIME, so the buckets are predictable."""
    for index, value in enumerate(values):
        writer._queue.append(
            {
                "type": "state",
                "time": BASE_TIME + timedelta(hours=index),
                "entity_id": entity_id,
                "state": None,
                "value": float(value),
                "attributes": {},
            }
        )
    await writer._flush()


@pytest.fixture
async def rollup_writer(hass, clean_db):
    writer = make_writer(hass, enable_rollups=True)
    await writer.start()
    assert writer._pool is not None
    yield writer
    await writer.stop()


@pytest.mark.asyncio
async def test_the_summaries_are_created_with_their_policies(rollup_writer):
    for name in (
        "states_hourly_raw",
        "states_hourly",
        "states_daily_raw",
        "states_daily",
    ):
        assert await _relation_exists(rollup_writer._pool, name), f"{name} is missing"

    async with rollup_writer._pool.acquire() as conn:
        policies = await conn.fetchval(
            "SELECT count(*) FROM timescaledb_information.jobs "
            "WHERE proc_name = 'policy_refresh_continuous_aggregate'"
        )
    assert policies == 2, "each summary keeps itself up to date"


@pytest.mark.asyncio
async def test_a_summary_holds_the_numbers_of_the_states_under_it(rollup_writer):
    """Four states in one hour: one row, with their average and their range."""
    await _write_hourly_states(rollup_writer, "sensor.rollup", [10, 20, 30])
    await _refresh(rollup_writer._pool, "states_hourly_raw")

    async with rollup_writer._pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT entity_id, bucket, value_avg, value_min, value_max, samples "
            "FROM states_hourly WHERE entity_id = 'sensor.rollup' ORDER BY bucket"
        )

    assert [row["value_avg"] for row in rows] == [10, 20, 30], "one state per hour"
    assert [row["samples"] for row in rows] == [1, 1, 1]

    # A second state inside the first hour: same bucket, averaged.
    rollup_writer._queue.append(
        {
            "type": "state",
            "time": BASE_TIME + timedelta(minutes=30),
            "entity_id": "sensor.rollup",
            "state": None,
            "value": 20.0,
            "attributes": {},
        }
    )
    await rollup_writer._flush()
    await _refresh(rollup_writer._pool, "states_hourly_raw")

    async with rollup_writer._pool.acquire() as conn:
        first = await conn.fetchrow(
            "SELECT value_avg, value_min, value_max, samples FROM states_hourly "
            "WHERE entity_id = 'sensor.rollup' ORDER BY bucket LIMIT 1"
        )
    assert first["samples"] == 2
    assert first["value_avg"] == 15
    assert (first["value_min"], first["value_max"]) == (10, 20)


@pytest.mark.asyncio
async def test_the_daily_summary_buckets_by_day(rollup_writer):
    await _write_hourly_states(rollup_writer, "sensor.daily", list(range(1, 25)))
    await _refresh(rollup_writer._pool, "states_daily_raw")

    async with rollup_writer._pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT bucket, samples FROM states_daily "
            "WHERE entity_id = 'sensor.daily' ORDER BY bucket"
        )

    assert sum(row["samples"] for row in rows) == 24
    assert len(rows) <= 2, "24 hourly states span one day, two at a boundary"


@pytest.mark.asyncio
async def test_turning_them_off_takes_them_away(hass, clean_db):
    """Scribe owns them, as it owns its retention policy."""
    writer = make_writer(hass, enable_rollups=True)
    await writer.start()
    assert await _relation_exists(writer._pool, "states_hourly")
    await writer.stop()

    off = make_writer(hass, enable_rollups=False)
    await off.start()
    try:
        for name in (
            "states_hourly",
            "states_hourly_raw",
            "states_daily",
            "states_daily_raw",
        ):
            assert not await _relation_exists(off._pool, name), f"{name} is still there"
        async with off._pool.acquire() as conn:
            policies = await conn.fetchval(
                "SELECT count(*) FROM timescaledb_information.jobs "
                "WHERE proc_name = 'policy_refresh_continuous_aggregate'"
            )
        assert policies == 0, "dropping the aggregate takes its policy with it"
    finally:
        await off.stop()


@pytest.mark.asyncio
async def test_the_history_survives_turning_them_off_and_on(hass, clean_db):
    """They are derived: dropping them must cost nothing but the rebuild."""
    writer = make_writer(hass, enable_rollups=True)
    await writer.start()
    await _write_hourly_states(writer, "sensor.kept", [1, 2, 3])
    await writer.stop()

    off = make_writer(hass, enable_rollups=False)
    await off.start()
    await off.stop()

    again = make_writer(hass, enable_rollups=True)
    await again.start()
    try:
        await _refresh(again._pool, "states_hourly_raw")
        async with again._pool.acquire() as conn:
            samples = await conn.fetchval(
                "SELECT sum(samples) FROM states_hourly WHERE entity_id = 'sensor.kept'"
            )
            states = await conn.fetchval(
                "SELECT count(*) FROM states WHERE entity_id = 'sensor.kept'"
            )
        assert states == 3, "the history itself was never touched"
        assert samples == 3, "the summary is rebuilt from it"
    finally:
        await again.stop()


@pytest.mark.asyncio
async def test_nothing_is_raised_when_they_are_off(hass, clean_db):
    from homeassistant.helpers import issue_registry as ir

    writer = make_writer(hass, enable_rollups=False)
    await writer.start()
    try:
        assert (
            ir.async_get(hass).async_get_issue("scribe", ISSUE_ROLLUPS_FAILED) is None
        )
    finally:
        await writer.stop()


@pytest.mark.asyncio
async def test_a_database_that_refuses_them_says_so(hass, clean_db, monkeypatch):
    """A user who turned them on is waiting for views that are not there."""
    from homeassistant.helpers import issue_registry as ir

    writer = make_writer(hass, enable_rollups=True)

    async def refuse(self, rollup):
        raise asyncpg.InsufficientPrivilegeError("permission denied for schema public")

    monkeypatch.setattr(type(writer), "_create_rollup", refuse)
    await writer.start()
    try:
        issue = ir.async_get(hass).async_get_issue("scribe", ISSUE_ROLLUPS_FAILED)
        assert issue is not None
        assert issue.severity == ir.IssueSeverity.WARNING
        assert "permission denied" in issue.translation_placeholders["error"]
        # Recording is a separate matter, and must not have stopped.
        assert writer._connected
        await _write_hourly_states(writer, "sensor.still_recording", [1])
        async with writer._pool.acquire() as conn:
            assert (
                await conn.fetchval(
                    "SELECT count(*) FROM states WHERE entity_id = 'sensor.still_recording'"
                )
                == 1
            )
    finally:
        await writer.stop()
