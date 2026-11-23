"""Behaviour against *compressed* TimescaleDB chunks.

Compression is the part of Scribe's storage that only shows its teeth on a
real installation: compressed chunks are read-mostly, and operations that are
trivial on a fresh database (UPDATE, DELETE) either fail or force a
decompression. Every test here compresses a chunk for real before acting on it.
"""

from datetime import timedelta

import pytest

from .conftest import (
    BASE_TIME,
    entity_rows,
    register_entity,
    sync_metadata,
    write_states,
)


async def _compress_all_states_chunks(pool):
    """Compress every states_raw chunk and return how many were compressed."""
    async with pool.acquire() as conn:
        chunks = await conn.fetch(
            "SELECT chunk_schema, chunk_name FROM timescaledb_information.chunks "
            "WHERE hypertable_name = 'states_raw' AND NOT is_compressed"
        )
        for c in chunks:
            await conn.execute(
                f"SELECT compress_chunk('{c['chunk_schema']}.{c['chunk_name']}')"
            )
        return len(chunks)


async def _compressed_chunk_count(pool):
    async with pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT count(*) FROM timescaledb_information.chunks "
            "WHERE hypertable_name = 'states_raw' AND is_compressed"
        )


@pytest.mark.asyncio
async def test_chunks_can_be_compressed_and_still_read(writer, db):
    """Compression is configured correctly enough that a chunk actually compresses."""
    await write_states(writer, "sensor.compressed", 20)

    assert await _compress_all_states_chunks(db) >= 1
    assert await _compressed_chunk_count(db) >= 1

    # Reading through the view still returns every row.
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM states WHERE entity_id = 'sensor.compressed'"
            )
            == 20
        )


@pytest.mark.asyncio
async def test_stats_report_compressed_chunks(writer, db):
    """The compression sensors reflect reality once chunks are compressed."""
    await write_states(writer, "sensor.compressed", 20)
    await _compress_all_states_chunks(db)

    stats = await writer.get_db_stats("all")

    assert stats["states_compressed_chunks"] >= 1
    assert stats["states_compressed_size"] > 0
    # The before/after pair the ratio sensor divides must both be populated;
    # they come from hypertable_compression_stats('states_raw') — the
    # hypertable, not the `states` view, which reports nothing.
    assert stats["states_before_compression_total_bytes"] > 0
    assert stats["states_after_compression_total_bytes"] > 0
    assert (
        stats["states_before_compression_total_bytes"]
        > stats["states_after_compression_total_bytes"]
    ), "compression saved nothing"


@pytest.mark.asyncio
async def test_new_states_still_write_after_compression(writer, db):
    """Recent data must keep flowing into the uncompressed head chunk."""
    await write_states(writer, "sensor.ongoing", 5, start=0)
    await _compress_all_states_chunks(db)

    # Far enough ahead to land in a new chunk (chunk_interval is 7 days).
    await write_states(writer, "sensor.ongoing", 5, start=60 * 60 * 24 * 30)

    _, count = await entity_rows(db, "sensor.ongoing")
    assert count == 10


@pytest.mark.asyncio
async def test_rename_merges_history_across_compressed_chunks(writer, hass, db):
    """The rename merge must survive compressed chunks, or lose the rename.

    This is the scenario a long-running installation actually hits: the dead
    orphan's history is old, therefore compressed, and the merge has to
    rewrite its metadata_id. TimescaleDB refuses plain UPDATEs on compressed
    chunks on older versions — if that happens the whole rename transaction
    rolls back and the user silently keeps a split history.
    """
    await register_entity(hass, "sensor.victim", "uid-victim")
    await sync_metadata(writer, hass, "sensor.victim")
    await write_states(writer, "sensor.victim", 10, start=0)
    victim_id, victim_rows = await entity_rows(db, "sensor.victim")
    assert victim_rows == 10

    compressed = await _compress_all_states_chunks(db)
    assert compressed >= 1, "test would be vacuous without a compressed chunk"

    from homeassistant.helpers import entity_registry as er

    er.async_get(hass).async_remove("sensor.victim")

    await register_entity(hass, "sensor.phoenix", "uid-phoenix")
    await sync_metadata(writer, hass, "sensor.phoenix")
    await write_states(writer, "sensor.phoenix", 3, start=60 * 60 * 24 * 30)

    await writer.rename_entity("sensor.phoenix", "sensor.victim")

    final_id, final_rows = await entity_rows(db, "sensor.victim")
    assert final_id is not None
    assert final_rows == 13, "compressed history was not merged"
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM entities WHERE id = $1", victim_id
            )
            == 0
        )


@pytest.mark.asyncio
async def test_duplicate_timestamp_cleanup_works_on_compressed_chunks(writer, hass, db):
    """The pre-merge DELETE also has to reach into compressed chunks."""
    await register_entity(hass, "sensor.victim", "uid-victim")
    await sync_metadata(writer, hass, "sensor.victim")
    await write_states(writer, "sensor.victim", 6, start=0)
    from homeassistant.helpers import entity_registry as er

    er.async_get(hass).async_remove("sensor.victim")

    await register_entity(hass, "sensor.phoenix", "uid-phoenix")
    await sync_metadata(writer, hass, "sensor.phoenix")
    # Full timestamp overlap with the victim.
    await write_states(writer, "sensor.phoenix", 4, start=0)

    await _compress_all_states_chunks(db)

    await writer.rename_entity("sensor.phoenix", "sensor.victim")

    _, final_rows = await entity_rows(db, "sensor.victim")
    # 4 survivor rows + the victim's 2 non-overlapping ones.
    assert final_rows == 6


@pytest.mark.asyncio
async def test_compression_policy_is_registered(writer, db):
    """A policy must exist, otherwise nothing ever compresses on its own."""
    async with db.acquire() as conn:
        jobs = await conn.fetch(
            "SELECT hypertable_name, config FROM timescaledb_information.jobs "
            "WHERE proc_name = 'policy_compression'"
        )
    tables = {j["hypertable_name"] for j in jobs}
    assert "states_raw" in tables
    assert "events" in tables


@pytest.mark.asyncio
async def test_states_view_reads_mixed_compressed_and_live_chunks(writer, db):
    """A query spanning both compressed and uncompressed chunks is complete."""
    await write_states(writer, "sensor.mixed_chunks", 5, start=0)
    await _compress_all_states_chunks(db)
    await write_states(writer, "sensor.mixed_chunks", 5, start=60 * 60 * 24 * 30)

    rows = await writer.query(
        "SELECT time, state FROM states WHERE entity_id = 'sensor.mixed_chunks' "
        "ORDER BY time"
    )

    assert len(rows) == 10
    assert rows[0]["time"] == BASE_TIME
    assert rows[-1]["time"] == BASE_TIME + timedelta(seconds=60 * 60 * 24 * 30 + 4)


# ---------------------------------------------------------------------------
# TimescaleDB's ceiling on rows decompressed per transaction (#113)
# ---------------------------------------------------------------------------

_LIMIT_GUC = "timescaledb.max_tuples_decompressed_per_dml_transaction"


@pytest.fixture
async def low_limit_writer(hass, clean_db):
    """A writer whose connections refuse to decompress more than 5 rows.

    The default ceiling is 100 000; lowering it is what lets a test reach it
    with a handful of states instead of writing a real installation's history.
    """
    from unittest.mock import patch

    from custom_components.scribe.writer import ScribeWriter

    from .conftest import make_writer

    original = ScribeWriter._server_settings

    def with_low_limit(self):
        return {**(original(self) or {}), _LIMIT_GUC: "5"}

    with patch.object(ScribeWriter, "_server_settings", with_low_limit):
        w = make_writer(hass)
        await w.start()
        assert w._pool is not None
        async with w._pool.acquire() as conn:
            if (
                await conn.fetchval("SELECT current_setting($1, true)", _LIMIT_GUC)
                is None
            ):
                await w.stop()
                pytest.skip("this TimescaleDB has no decompression ceiling")
        yield w
        await w.stop()


async def _split_history(hass, writer, db, *, old_rows, new_rows):
    """Reproduce #113: a rename that failed, leaving one entity under two rows.

    The old plug's sensor recorded `old_rows` states, compressed, then the
    device was removed. Its replacement appeared under another name and was
    renamed onto the old one; the merge failed, and the registry sync then gave
    the old row the replacement's coordinates — what the listener does next.
    """
    from unittest.mock import patch

    from homeassistant.helpers import entity_registry as er

    await register_entity(hass, "sensor.plug_power", "uid-old-plug", platform="mqtt")
    await sync_metadata(writer, hass, "sensor.plug_power")
    await write_states(writer, "sensor.plug_power", old_rows, start=0)
    await _compress_all_states_chunks(db)
    er.async_get(hass).async_remove("sensor.plug_power")

    await register_entity(
        hass, "sensor.0x00158d_power", "uid-new-plug", platform="mqtt"
    )
    await sync_metadata(writer, hass, "sensor.0x00158d_power")
    await write_states(
        writer, "sensor.0x00158d_power", new_rows, start=60 * 60 * 24 * 30
    )
    er.async_get(hass).async_update_entity(
        "sensor.0x00158d_power", new_entity_id="sensor.plug_power"
    )
    with patch.object(
        type(writer), "_fold_history", side_effect=RuntimeError("limit exceeded")
    ):
        await writer.rename_entity("sensor.0x00158d_power", "sensor.plug_power")
    await sync_metadata(writer, hass, "sensor.plug_power")


def _issue(hass, entity_id):
    from homeassistant.helpers import issue_registry as ir

    from custom_components.scribe.const import DOMAIN

    return ir.async_get(hass).async_get_issue(DOMAIN, f"rename_collision_{entity_id}")


@pytest.mark.asyncio
async def test_rename_merge_is_not_stopped_by_the_decompression_limit(
    low_limit_writer, hass
):
    """#113: the merge decompresses more than the ceiling and still completes."""
    writer = low_limit_writer
    db = writer._pool
    from homeassistant.helpers import entity_registry as er

    await register_entity(hass, "sensor.victim", "uid-victim")
    await sync_metadata(writer, hass, "sensor.victim")
    await write_states(writer, "sensor.victim", 20, start=0)
    assert await _compress_all_states_chunks(db) >= 1
    er.async_get(hass).async_remove("sensor.victim")

    await register_entity(hass, "sensor.phoenix", "uid-phoenix")
    await sync_metadata(writer, hass, "sensor.phoenix")
    await write_states(writer, "sensor.phoenix", 3, start=60 * 60 * 24 * 30)

    await writer.rename_entity("sensor.phoenix", "sensor.victim")

    _, rows = await entity_rows(db, "sensor.victim")
    assert rows == 23, "the merge was rolled back by the decompression limit"
    assert _issue(hass, "sensor.victim") is None
    async with db.acquire() as conn:
        # Lifted for the merge's own transaction, nowhere else.
        assert await conn.fetchval("SELECT current_setting($1)", _LIMIT_GUC) == "5"


@pytest.mark.asyncio
async def test_a_failed_merge_is_retried_and_its_issue_kept_until_done(
    low_limit_writer, hass
):
    """#113: the split is recorded, survives a later rename, and heals."""
    writer = low_limit_writer
    db = writer._pool
    await _split_history(hass, writer, db, old_rows=20, new_rows=4)

    issue = _issue(hass, "sensor.plug_power")
    assert issue is not None and issue.translation_key == "rename_failed"
    assert issue.is_persistent, "lost at the next restart"
    assert (await entity_rows(db, "sensor.0x00158d_power"))[1] == 4

    # What the reporter saw clear the notice: another rename to the same name.
    await writer.rename_entity("sensor.plug_power", "sensor.plug_power_tmp")
    await writer.rename_entity("sensor.plug_power_tmp", "sensor.plug_power")
    assert _issue(hass, "sensor.plug_power") is not None

    # A merge that fails again keeps the notice, now saying so.
    from unittest.mock import patch

    with patch.object(
        type(writer), "_fold_history", side_effect=RuntimeError("database gone")
    ):
        await writer.retry_pending_merges()
    assert _issue(hass, "sensor.plug_power").translation_key == "history_split"

    # At the next start, it goes through, under the low limit.
    await writer.retry_pending_merges()
    assert (await entity_rows(db, "sensor.plug_power"))[1] == 24
    assert (await entity_rows(db, "sensor.0x00158d_power"))[0] is None
    assert _issue(hass, "sensor.plug_power") is None


@pytest.mark.asyncio
async def test_history_scribe_did_not_split_is_left_alone(writer, hass, db):
    """Two rows of one entity with no merge owed: nothing is folded.

    Old installations hold such pairs — a rename made before Scribe followed
    renames, a sensor moved from one room to another. Merging them is the
    user's call, not something to do behind their back at an upgrade.
    """
    await register_entity(hass, "sensor.bedroom_temperature", "uid-t")
    await sync_metadata(writer, hass, "sensor.bedroom_temperature")
    await write_states(writer, "sensor.bedroom_temperature", 3, start=1000)
    async with db.acquire() as conn:
        # The row a rename before Scribe followed renames leaves behind.
        await conn.execute(
            "INSERT INTO entities (entity_id, unique_id, platform, domain) "
            "SELECT 'sensor.shed_temperature', unique_id, platform, domain "
            "FROM entities WHERE entity_id = 'sensor.bedroom_temperature'"
        )
    await write_states(writer, "sensor.shed_temperature", 5)

    await writer.retry_pending_merges()
    await writer.merge_split_history("sensor.bedroom_temperature")

    assert (await entity_rows(db, "sensor.bedroom_temperature"))[1] == 3
    assert (await entity_rows(db, "sensor.shed_temperature"))[1] == 5
