"""What `scribe.query` refuses, against a real PostgreSQL.

The service takes SQL as written — no keyword filtering, which would be a
denylist to keep up with — and runs it in a `READ ONLY` transaction with a
statement timeout. That is the whole protection, so it is worth proving.
"""

import asyncpg
import pytest

from .conftest import write_states


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE entities SET entity_id = 'hacked' WHERE true",
        "DELETE FROM states_raw WHERE true",
        "TRUNCATE states_raw",
        "DROP TABLE entities",
        "INSERT INTO entities (entity_id) VALUES ('sneaked_in')",
        "CREATE TABLE mine (id int)",
        "ALTER TABLE entities ADD COLUMN mine int",
        "CREATE INDEX mine ON states_raw (time)",
        "GRANT ALL ON entities TO PUBLIC",
    ],
)
@pytest.mark.asyncio
async def test_a_query_that_writes_is_refused(writer, sql):
    await write_states(writer, "sensor.untouched", 3)

    with pytest.raises(asyncpg.PostgresError) as raised:
        await writer.query(sql)

    assert "read-only transaction" in str(raised.value).lower(), str(raised.value)

    # And the database is exactly as it was.
    async with writer._pool.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM states WHERE entity_id = 'sensor.untouched'"
            )
            == 3
        )
        assert await conn.fetchval("SELECT to_regclass('entities') IS NOT NULL"), (
            "the table is still there"
        )


@pytest.mark.asyncio
async def test_reading_is_allowed(writer):
    """The point of the service: a SELECT still works."""
    await write_states(writer, "sensor.readable", 2)

    rows = await writer.query(
        "SELECT entity_id, value FROM states WHERE entity_id = 'sensor.readable'"
    )

    assert [row["value"] for row in rows] == [0.0, 1.0]


@pytest.mark.asyncio
async def test_a_query_cannot_hold_a_connection_for_ever(hass, clean_db):
    """The server ends it, not the caller giving up.

    One second rather than the configured sixty, so the test is quick; what is
    checked is that the configured value is the one that applies.
    """
    from .conftest import make_writer

    writer = make_writer(hass, query_timeout=1)
    await writer.start()

    with pytest.raises(asyncpg.PostgresError) as raised:
        await writer.query("SELECT pg_sleep(30)")
    await writer.stop()

    assert "statement timeout" in str(raised.value).lower()


@pytest.mark.asyncio
async def test_two_statements_in_one_query_are_refused(writer):
    """Stacking a write behind a SELECT does not even reach the transaction.

    The query is sent through the extended protocol, which takes one statement
    and no more — so `SELECT 1; DELETE …` is refused by the driver, before the
    read-only transaction would have refused it anyway.
    """
    await write_states(writer, "sensor.safe", 3)

    with pytest.raises(asyncpg.PostgresError) as raised:
        await writer.query("SELECT 1; DELETE FROM states_raw WHERE true")

    assert "multiple commands" in str(raised.value).lower(), str(raised.value)

    async with writer._pool.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM states WHERE entity_id = 'sensor.safe'"
            )
            == 3
        )


@pytest.mark.asyncio
async def test_a_query_returning_too_much_is_refused(hass, clean_db):
    """Millions of rows would reach Home Assistant's memory, not a chart.

    The refusal says what to do about it, and the query is stopped while it
    streams rather than after everything has been loaded.
    """
    from .conftest import make_writer

    writer = make_writer(hass, query_max_rows=50)
    await writer.start()

    with pytest.raises(ValueError) as raised:
        await writer.query("SELECT generate_series(1, 5000) AS n")

    message = str(raised.value)
    assert "more than 50 rows" in message
    assert "time_bucket()" in message and "LIMIT" in message

    # And what fits still comes back.
    rows = await writer.query("SELECT generate_series(1, 10) AS n")
    assert len(rows) == 10
    await writer.stop()


@pytest.mark.asyncio
async def test_the_defaults_are_what_the_documentation_says(hass, clean_db):
    """60 seconds and 20 000 rows, unless the configuration says otherwise."""
    from custom_components.scribe.const import (
        DEFAULT_QUERY_MAX_ROWS,
        DEFAULT_QUERY_TIMEOUT,
    )

    from .conftest import make_writer

    writer = make_writer(hass)

    assert (writer.query_timeout, writer.query_max_rows) == (60, 20_000)
    assert DEFAULT_QUERY_TIMEOUT == 60
    assert DEFAULT_QUERY_MAX_ROWS == 20_000
