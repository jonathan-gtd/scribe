"""Schema creation against a real TimescaleDB: tables, hypertables, view."""

import asyncpg
import pytest

from .conftest import DSN, make_writer, table_exists


@pytest.mark.asyncio
async def test_creates_all_tables(writer, db):
    """A default configuration creates every table Scribe needs."""
    for name in (
        "entities",
        "states_raw",
        "events",
        "users",
        "areas",
        "devices",
        "integrations",
    ):
        assert await table_exists(db, name), f"{name} missing"
    # `states` is a view over states_raw, never a table.
    assert await table_exists(db, "states", kind="VIEW")
    assert not await table_exists(db, "states")


@pytest.mark.asyncio
async def test_states_raw_has_its_primary_key(writer, db):
    """The (metadata_id, time) PK must exist from creation, not from migration.

    Migration scripts rely on it for `ON CONFLICT (metadata_id, time)`, which
    fails outright if the constraint was never created.
    """
    async with db.acquire() as conn:
        pk_columns = await conn.fetch(
            """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid
                               AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = 'states_raw'::regclass AND i.indisprimary
            ORDER BY a.attname
            """
        )
        assert [r["attname"] for r in pk_columns] == ["metadata_id", "time"]


@pytest.mark.asyncio
async def test_entities_entity_id_is_unique(writer, db):
    """The UNIQUE constraint is what makes rename collisions detectable."""
    async with db.acquire() as conn:
        await conn.execute("INSERT INTO entities (entity_id) VALUES ('sensor.dup')")
        with pytest.raises(asyncpg.UniqueViolationError):
            await conn.execute("INSERT INTO entities (entity_id) VALUES ('sensor.dup')")


@pytest.mark.asyncio
async def test_hypertables_are_created(writer, db):
    """states_raw and events are TimescaleDB hypertables, not plain tables."""
    async with db.acquire() as conn:
        names = await conn.fetch(
            "SELECT hypertable_name FROM timescaledb_information.hypertables"
        )
        got = {r["hypertable_name"] for r in names}
    assert {"states_raw", "events"} <= got


@pytest.mark.asyncio
async def test_states_view_joins_entity_ids(writer, db):
    """The compatibility view resolves metadata_id back to entity_id."""
    async with db.acquire() as conn:
        mid = await conn.fetchval(
            "INSERT INTO entities (entity_id) VALUES ('sensor.viewed') RETURNING id"
        )
        await conn.execute(
            "INSERT INTO states_raw (time, metadata_id, state, value) "
            "VALUES (now(), $1, 'on', 1.0)",
            mid,
        )
        row = await conn.fetchrow(
            "SELECT entity_id, state, value FROM states WHERE entity_id = 'sensor.viewed'"
        )
    assert row["entity_id"] == "sensor.viewed"
    assert row["state"] == "on"
    assert row["value"] == 1.0


@pytest.mark.asyncio
async def test_init_is_idempotent(writer, db, hass):
    """Starting a second writer over an existing schema changes nothing."""
    async with db.acquire() as conn:
        before = await conn.fetchval(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema = 'public'"
        )

    second = make_writer(hass)
    await second.start()
    try:
        assert second._pool is not None
        async with second._pool.acquire() as conn:
            after = await conn.fetchval(
                "SELECT count(*) FROM information_schema.tables "
                "WHERE table_schema = 'public'"
            )
        assert after == before
    finally:
        await second.stop()


@pytest.mark.asyncio
async def test_record_flags_skip_their_tables(hass, clean_db):
    """record_states=False / record_events=False leave those tables uncreated."""
    w = make_writer(hass, record_states=False, record_events=False)
    await w.start()
    try:
        assert not await table_exists(w._pool, "states_raw")
        assert not await table_exists(w._pool, "events")
        # Metadata tables are unaffected.
        assert await table_exists(w._pool, "entities")
    finally:
        await w.stop()


@pytest.mark.asyncio
async def test_optional_metadata_tables_can_be_disabled(hass, clean_db):
    """The enable_table_* flags each gate exactly one table."""
    w = make_writer(
        hass,
        enable_table_areas=False,
        enable_table_devices=False,
        enable_table_integrations=False,
        enable_table_users=False,
    )
    await w.start()
    try:
        for name in ("areas", "devices", "integrations", "users"):
            assert not await table_exists(w._pool, name), f"{name} should be absent"
        # entities is mandatory: every state write upserts into it.
        assert await table_exists(w._pool, "entities")
    finally:
        await w.stop()


@pytest.mark.asyncio
async def test_the_states_view_materializes_only_what_it_projects(writer, db):
    """The CTE feeding the lateral join must not carry the whole entities row.

    It once selected `*`, which materialized every entity's `capabilities`
    jsonb — usually the largest column in that table — on every query through
    the view, while only `id` and `entity_id` are ever used.
    """
    async with db.acquire() as conn:
        definition = await conn.fetchval(
            "SELECT pg_get_viewdef('states'::regclass, true)"
        )

    drive = definition.split("drive AS MATERIALIZED")[1].split(")")[0]
    assert "capabilities" not in drive
    assert "entity_id" in drive
    assert "id" in drive


@pytest.mark.asyncio
async def test_states_raw_carries_no_index_the_primary_key_duplicates(writer, db):
    """An index on (metadata_id, time DESC) serves nothing the key does not.

    A B-tree is scanned in either direction, so the primary key answers
    `WHERE metadata_id = x ORDER BY time DESC` on its own. Keeping both
    doubled the index footprint of the largest table and cost every write.
    """
    async with db.acquire() as conn:
        assert await conn.fetchval(
            "SELECT to_regclass('states_raw_meta_time_idx') IS NULL"
        )


@pytest.mark.asyncio
async def test_an_index_left_by_an_earlier_version_is_dropped(hass, clean_db):
    """Existing installations carry it; starting up removes it."""
    import asyncpg

    from .conftest import DSN, make_writer

    conn = await asyncpg.connect(DSN)
    try:
        await conn.execute(
            "CREATE TABLE states_raw (time TIMESTAMPTZ NOT NULL, "
            "metadata_id INTEGER NOT NULL, state TEXT, value DOUBLE PRECISION, "
            "attributes JSONB, PRIMARY KEY (metadata_id, time))"
        )
        await conn.execute(
            "CREATE INDEX states_raw_meta_time_idx ON states_raw (metadata_id, time DESC)"
        )
    finally:
        await conn.close()

    w = make_writer(hass)
    await w.start()
    try:
        async with w._pool.acquire() as conn:
            assert await conn.fetchval(
                "SELECT to_regclass('states_raw_meta_time_idx') IS NULL"
            )

            # And the query it used to serve still returns the right row.
            # Which index the planner picks depends on the data, so that is
            # left to the benchmark rather than asserted here.
            await conn.execute(
                "INSERT INTO states_raw (time, metadata_id, state, value, attributes) "
                "SELECT now() - (s || ' seconds')::interval, 1, 'on', s, NULL "
                "FROM generate_series(1, 2000) s"
            )
            newest = await conn.fetchval(
                "SELECT value FROM states_raw "
                "WHERE metadata_id = 1 ORDER BY time DESC LIMIT 1"
            )
        assert newest == 1.0
    finally:
        await w.stop()


@pytest.mark.asyncio
async def test_the_states_function_answers_like_the_view(writer, db):
    """`states_for('x')` returns exactly the view's rows for that entity."""
    async with db.acquire() as conn:
        for entity_id in ("sensor.looked_up", "sensor.other"):
            mid = await conn.fetchval(
                "INSERT INTO entities (entity_id) VALUES ($1) RETURNING id",
                entity_id,
            )
            await conn.execute(
                "INSERT INTO states_raw (time, metadata_id, state, value, attributes) "
                "SELECT now() - (s || ' seconds')::interval, $1, 'on', s, "
                "jsonb_build_object('n', s) "
                "FROM generate_series(1, 3) s",
                mid,
            )

        through_function = await conn.fetch(
            "SELECT time, entity_id, state, value, attributes "
            "FROM states_for('sensor.looked_up') ORDER BY time"
        )
        through_view = await conn.fetch(
            "SELECT time, entity_id, state, value, attributes "
            "FROM states WHERE entity_id = 'sensor.looked_up' ORDER BY time"
        )

    assert [dict(r) for r in through_function] == [dict(r) for r in through_view]
    assert len(through_function) == 3
    assert all(r["entity_id"] == "sensor.looked_up" for r in through_function)


@pytest.mark.asyncio
async def test_an_unknown_entity_returns_no_rows(writer, db):
    """The entity lookup inside the function yields NULL, which matches nothing."""
    async with db.acquire() as conn:
        assert (
            await conn.fetch("SELECT * FROM states_for('sensor.never_recorded')") == []
        )
        assert await conn.fetch("SELECT * FROM states_for(NULL)") == []


@pytest.mark.asyncio
async def test_the_states_function_is_inlined_into_the_calling_query(writer, db):
    """What makes it fast: the filter and the LIMIT must reach `states_raw`.

    PostgreSQL inlines a one-statement SQL function, and only then can the
    planner push `ORDER BY time DESC LIMIT 1` down to the hypertable instead
    of sorting every entity's history above the join — which is exactly what
    the view cannot avoid (#97). A plan containing `Function Scan` means the
    inlining did not happen and the whole point is lost.
    """
    async with db.acquire() as conn:
        mid = await conn.fetchval(
            "INSERT INTO entities (entity_id) VALUES ('sensor.planned') RETURNING id"
        )
        await conn.execute(
            "INSERT INTO states_raw (time, metadata_id, state, value) "
            "SELECT now() - (s || ' seconds')::interval, $1, 'on', s "
            "FROM generate_series(1, 2000) s",
            mid,
        )
        await conn.execute("ANALYZE states_raw")
        plan = "\n".join(
            r["QUERY PLAN"]
            for r in await conn.fetch(
                "EXPLAIN SELECT * FROM states_for('sensor.planned') "
                "WHERE time <= now() AND value IS NOT NULL "
                "ORDER BY time DESC LIMIT 1"
            )
        )

    assert "Function Scan" not in plan, plan
    # The entity was resolved underneath, against the column states_raw is
    # keyed and segmented by.
    assert "metadata_id" in plan, plan


@pytest.mark.asyncio
async def test_the_states_function_is_named_after_the_view(hass, clean_db):
    """A renamed states view gets a matching `<name>_for`."""
    w = make_writer(hass, table_name_states="history")
    await w.start()
    try:
        async with w._pool.acquire() as conn:
            mid = await conn.fetchval(
                "INSERT INTO entities (entity_id) VALUES ('sensor.renamed') RETURNING id"
            )
            await conn.execute(
                "INSERT INTO states_raw (time, metadata_id, state) "
                "VALUES (now(), $1, 'on')",
                mid,
            )
            assert await conn.fetchval("SELECT to_regclass('states_for') IS NULL")
            row = await conn.fetchrow("SELECT * FROM history_for('sensor.renamed')")
        assert row["entity_id"] == "sensor.renamed"
        assert row["state"] == "on"
    finally:
        await w.stop()


@pytest.mark.asyncio
async def test_a_function_whose_columns_differ_is_replaced(hass, clean_db):
    """CREATE OR REPLACE cannot change output columns; startup drops and recreates.

    An installation carrying a `states_for` from a version that returned
    something else must end up with this one, not with an error.
    """
    conn = await asyncpg.connect(DSN)
    try:
        await conn.execute(
            """
            CREATE FUNCTION states_for(p_entity_id TEXT)
            RETURNS TABLE ("time" TIMESTAMPTZ, entity_id TEXT)
            LANGUAGE sql STABLE AS $fn$ SELECT now(), p_entity_id $fn$
            """
        )
    finally:
        await conn.close()

    w = make_writer(hass)
    await w.start()
    try:
        async with w._pool.acquire() as conn:
            mid = await conn.fetchval(
                "INSERT INTO entities (entity_id) VALUES ('sensor.replaced') RETURNING id"
            )
            await conn.execute(
                "INSERT INTO states_raw (time, metadata_id, state, value) "
                "VALUES (now(), $1, 'on', 2.0)",
                mid,
            )
            row = await conn.fetchrow("SELECT * FROM states_for('sensor.replaced')")
        assert dict(row).keys() == {"time", "entity_id", "state", "value", "attributes"}
        assert row["value"] == 2.0
    finally:
        await w.stop()


@pytest.mark.asyncio
async def test_a_restart_keeps_grants_and_views_on_the_states_view(writer, db, hass):
    """The view is replaced in place, so what users built on it survives a restart.

    Dropping and recreating it gave a new object each start: GRANTs made on the
    old one were gone, and CASCADE took views built on it along.
    """
    role = "scribe_test_reader"
    async with db.acquire() as conn:
        await conn.execute(
            f"DO $$ BEGIN CREATE ROLE {role}; "
            "EXCEPTION WHEN duplicate_object THEN NULL; END $$"
        )
        await conn.execute(f"GRANT SELECT ON states TO {role}")
        await conn.execute("CREATE VIEW user_states AS SELECT * FROM states")

    second = make_writer(hass)
    await second.start()
    try:
        async with second._pool.acquire() as conn:
            assert await conn.fetchval(
                "SELECT has_table_privilege($1, 'states', 'SELECT')", role
            )
            assert await conn.fetchval("SELECT to_regclass('user_states') IS NOT NULL")
    finally:
        await second.stop()
        async with db.acquire() as conn:
            await conn.execute("DROP VIEW IF EXISTS user_states")
            # Revokes the grant as well, which DROP ROLE would refuse over.
            await conn.execute(f"DROP OWNED BY {role}")
            await conn.execute(f"DROP ROLE {role}")


@pytest.mark.asyncio
async def test_a_states_view_with_other_columns_is_recreated(hass, clean_db):
    """CREATE OR REPLACE cannot drop view columns; startup drops and recreates."""
    conn = await asyncpg.connect(DSN)
    try:
        await conn.execute(
            "CREATE TABLE states_raw (time TIMESTAMPTZ NOT NULL, "
            "metadata_id INTEGER NOT NULL, state TEXT, value DOUBLE PRECISION, "
            "attributes JSONB, PRIMARY KEY (metadata_id, time))"
        )
        await conn.execute(
            "CREATE VIEW states AS SELECT time, metadata_id, state, value, "
            "attributes, 'extra'::text AS extra FROM states_raw"
        )
    finally:
        await conn.close()

    w = make_writer(hass)
    await w.start()
    try:
        async with w._pool.acquire() as conn:
            columns = await conn.fetch(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = 'states' "
                "ORDER BY ordinal_position"
            )
        assert [c["column_name"] for c in columns] == [
            "time",
            "entity_id",
            "state",
            "value",
            "attributes",
        ]
    finally:
        await w.stop()
