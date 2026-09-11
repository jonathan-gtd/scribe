"""Fill a database the way an older Scribe release does. Run by tests/upgrade/conftest.py.

This file is copied into a worktree of that release and run there, so
`custom_components.scribe` below is the *old* code. It only goes through the
public setup path — a config entry, state changes, an event, the `flush`
service — which every 3.x and 4.x release shares, so one seed works for all
of them.

It is deliberately not named `test_*.py`: the normal suite must not collect it.
"""

import os
from unittest.mock import patch

import asyncpg
import custom_components.scribe.writer  # noqa: F401  (the release under test)
import pytest
from homeassistant.helpers import entity_registry as er
from pytest_homeassistant_custom_component.common import MockConfigEntry

DSN = os.environ["SCRIBE_UPGRADE_DSN"]


@pytest.fixture(autouse=True)
def _custom_integrations(enable_custom_integrations):
    yield


@pytest.fixture(autouse=True)
def _real_pool():
    """Real asyncpg, minus the idle timers Home Assistant's teardown rejects."""
    real = asyncpg.create_pool

    def factory(*args, **kwargs):
        kwargs.setdefault("max_inactive_connection_lifetime", 0)
        return real(*args, **kwargs)

    with patch(
        "custom_components.scribe.writer.asyncpg.create_pool", side_effect=factory
    ):
        yield


# Old releases may leave a timer or a task behind at unload; that is theirs to
# have, not something this seed is checking.
@pytest.mark.parametrize("expected_lingering_timers", [True])
@pytest.mark.parametrize("expected_lingering_tasks", [True])
async def test_seed(
    hass, socket_enabled, expected_lingering_timers, expected_lingering_tasks
):
    registry = er.async_get(hass)
    registry.async_get_or_create(
        "sensor", "demo", "upgrade-temp", suggested_object_id="upgrade_temp"
    )
    registry.async_get_or_create(
        "sensor", "demo", "upgrade-old", suggested_object_id="upgrade_old"
    )

    entry = MockConfigEntry(
        domain="scribe",
        data={"db_url": DSN, "record_states": True, "record_events": True},
        entry_id="upgrade_entry",
    )
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    for i in range(20):
        hass.states.async_set(
            "sensor.upgrade_temp", str(20 + i), {"unit_of_measurement": "°C"}
        )
        hass.states.async_set("sensor.upgrade_old", "on" if i % 2 else "off")
    hass.bus.async_fire("upgrade_event", {"n": 1})
    await hass.async_block_till_done()
    await hass.services.async_call("scribe", "flush", blocking=True)
    await hass.async_block_till_done()

    conn = await asyncpg.connect(DSN)
    try:
        written = await conn.fetchval(
            "SELECT count(*) FROM states WHERE entity_id = 'sensor.upgrade_temp'"
        )
    finally:
        await conn.close()
    assert written == 20, f"the release under test wrote {written} of 20 states"

    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
