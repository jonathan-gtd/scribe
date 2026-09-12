"""Deleting history, against a real TimescaleDB.

A purge is the one thing Scribe does that cannot be undone, so what matters
here is as much what it leaves alone as what it removes.
"""

import pytest

from custom_components.scribe.writer import ScribeWriter

from .conftest import BASE_TIME, write_states


async def _entity_rows(pool, entity_id):
    async with pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT count(*) FROM states WHERE entity_id = $1", entity_id
        )


async def _entities(pool):
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT entity_id FROM entities ORDER BY entity_id")
    return [row["entity_id"] for row in rows]


@pytest.mark.asyncio
async def test_purging_an_entity_removes_it_from_the_database(
    hass, writer: ScribeWriter
):
    """Its history and its row: the entity leaves, not just its states."""
    await write_states(writer, "sensor.gone", 5)
    await write_states(writer, "sensor.kept", 5)

    purged = await writer.purge(entity_ids=["sensor.gone"])

    assert purged == {"states": 5, "events": 0, "entities": 1}
    assert await _entity_rows(writer._pool, "sensor.gone") == 0
    assert await _entity_rows(writer._pool, "sensor.kept") == 5
    assert await _entities(writer._pool) == ["sensor.kept"]


@pytest.mark.asyncio
async def test_purging_an_entity_leaves_the_cache_consistent(
    hass, writer: ScribeWriter
):
    """Recording it again must not write states against a deleted id."""
    await write_states(writer, "sensor.again", 3)
    old_id = writer._entity_id_map["sensor.again"]

    await writer.purge(entity_ids=["sensor.again"])
    assert "sensor.again" not in writer._entity_id_map

    await write_states(writer, "sensor.again", 2)

    assert writer._entity_id_map["sensor.again"] != old_id
    assert await _entity_rows(writer._pool, "sensor.again") == 2


@pytest.mark.asyncio
async def test_an_age_trims_the_old_and_keeps_the_rest(hass, writer: ScribeWriter):
    """`keep_days` is a horizon, not a list: everything older goes."""
    from datetime import timedelta

    from .conftest import DSN
    import asyncpg

    await write_states(writer, "sensor.long_lived", 3)
    # The helper writes around BASE_TIME, which is itself weeks in the past, so
    # the horizon below has to clear it: 60 days keeps those three and drops the
    # three written 40 days before them.
    conn = await asyncpg.connect(DSN)
    try:
        metadata_id = writer._entity_id_map["sensor.long_lived"]
        await conn.executemany(
            "INSERT INTO states_raw (time, metadata_id, state) VALUES ($1, $2, 'old')",
            [
                (
                    BASE_TIME - timedelta(days=40 + i),
                    metadata_id,
                )
                for i in range(3)
            ],
        )
    finally:
        await conn.close()
    assert await _entity_rows(writer._pool, "sensor.long_lived") == 6

    purged = await writer.purge(keep_days=60)

    assert purged["states"] == 3
    assert await _entity_rows(writer._pool, "sensor.long_lived") == 3
    assert await _entities(writer._pool) == ["sensor.long_lived"], "the entity stays"


@pytest.mark.asyncio
async def test_events_are_only_purged_when_asked(hass, writer: ScribeWriter):
    from datetime import timedelta

    import asyncpg

    from .conftest import DSN

    conn = await asyncpg.connect(DSN)
    try:
        await conn.execute(
            "INSERT INTO events (time, event_type) VALUES ($1, 'old_event')",
            BASE_TIME - timedelta(days=90),
        )
    finally:
        await conn.close()

    kept = await writer.purge(keep_days=30)
    assert kept["events"] == 0, "events are not touched unless asked for"

    purged = await writer.purge(keep_days=30, include_events=True)
    assert purged["events"] == 1


@pytest.mark.asyncio
async def test_purging_compressed_history_works(hass, writer: ScribeWriter):
    """Most of a real history is compressed; a purge that stopped there is useless."""
    from datetime import timedelta

    import asyncpg

    from .conftest import DSN

    await write_states(writer, "sensor.compressed", 1)
    metadata_id = writer._entity_id_map["sensor.compressed"]

    conn = await asyncpg.connect(DSN)
    try:
        await conn.executemany(
            "INSERT INTO states_raw (time, metadata_id, value) VALUES ($1, $2, 1)",
            [
                (BASE_TIME - timedelta(days=100, minutes=i), metadata_id)
                for i in range(20)
            ],
        )
        compressed = await conn.fetch(
            "SELECT compress_chunk(c) FROM show_chunks('states_raw', "
            "older_than => $1::timestamptz) c",
            BASE_TIME - timedelta(days=90),
        )
        assert compressed, "nothing was compressed, so this proves nothing"
    finally:
        await conn.close()

    purged = await writer.purge(entity_ids=["sensor.compressed"])

    assert purged["states"] == 21
    assert await _entity_rows(writer._pool, "sensor.compressed") == 0


@pytest.mark.asyncio
async def test_a_purge_that_names_nothing_is_refused(hass, writer: ScribeWriter):
    """An empty call must not mean "everything"."""
    await write_states(writer, "sensor.safe", 3)

    with pytest.raises(ValueError):
        await writer.purge()

    assert await _entity_rows(writer._pool, "sensor.safe") == 3


@pytest.mark.asyncio
async def test_purging_an_entity_nobody_recorded_changes_nothing(
    hass, writer: ScribeWriter
):
    await write_states(writer, "sensor.real", 4)

    purged = await writer.purge(entity_ids=["sensor.never_seen"])

    assert purged == {"states": 0, "events": 0, "entities": 0}
    assert await _entity_rows(writer._pool, "sensor.real") == 4
