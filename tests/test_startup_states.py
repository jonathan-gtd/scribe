"""What Home Assistant had already set when Scribe started.

The listener only sees what changes after it is registered, and Scribe is set
up well into a Home Assistant start. Two things narrow that gap: the listener
is registered before the platforms, and the states already set are recorded
once at setup.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from custom_components.scribe import _record_current_states, _state_row


def _writer(deduplicates=True):
    writer = MagicMock()
    writer.deduplicates_states = deduplicates
    writer.enqueue = MagicMock()
    return writer


def _queued(writer):
    """The states queued, by entity id. Events are queued too, and have none."""
    return {
        call.args[0]["entity_id"]: call.args[0]
        for call in writer.enqueue.mock_calls
        if call.args[0].get("type") == "state"
    }


async def test_every_state_already_set_is_recorded(hass):
    """An entity that never changes again would otherwise never be recorded."""
    hass.states.async_set("sensor.temperature", "21.5", {"unit_of_measurement": "°C"})
    hass.states.async_set("binary_sensor.door", "on")
    writer = _writer()

    assert _record_current_states(hass, writer, lambda _: True, set()) == 2

    queued = _queued(writer)
    assert queued["sensor.temperature"]["value"] == 21.5
    assert queued["sensor.temperature"]["state"] is None
    assert queued["binary_sensor.door"]["state"] == "on"
    assert queued["binary_sensor.door"]["value"] is None


async def test_the_snapshot_carries_the_state_home_assistant_holds(hass):
    """Same `last_updated` as the live row, which is what drops the duplicate."""
    hass.states.async_set("sensor.temperature", "21.5", {"icon": "mdi:thermometer"})
    state = hass.states.get("sensor.temperature")
    writer = _writer()

    _record_current_states(hass, writer, lambda _: True, {"icon"})

    row = _queued(writer)["sensor.temperature"]
    assert row["time"] == state.last_updated
    assert row == _state_row(state, {"icon"})
    assert "icon" not in row["attributes"]


async def test_the_filter_applies_to_the_snapshot_too(hass):
    """Whatever the listener would drop must not come in through the back door."""
    hass.states.async_set("sensor.kept", "1")
    hass.states.async_set("sensor.secret", "2")
    writer = _writer()

    recorded = _record_current_states(
        hass, writer, lambda entity_id: entity_id != "sensor.secret", set()
    )

    assert recorded == 1
    assert set(_queued(writer)) == {"sensor.kept"}


async def test_a_database_that_cannot_deduplicate_gets_no_snapshot(hass, caplog):
    """Without the key on (metadata_id, time) every restart would duplicate.

    Databases created by Scribe 3.1 to 3.5 have no such key, and no release
    ever added it.
    """
    hass.states.async_set("sensor.temperature", "21.5")
    writer = _writer(deduplicates=False)

    assert _record_current_states(hass, writer, lambda _: True, set()) == 0

    writer.enqueue.assert_not_called()
    assert "no primary key on states_raw" in caplog.text


async def test_a_state_set_while_the_platforms_load_is_recorded(
    hass, mock_config_entry
):
    """The listener is registered before the platforms, not after.

    Forwarding the platforms first cost Scribe every state set while it waited
    — starting with its own entities, which is how the connectivity sensor was
    never recorded at a start.
    """
    from custom_components.scribe import async_setup_entry

    writer = MagicMock()
    writer.start = AsyncMock()
    writer.stop = AsyncMock()
    writer.deduplicates_states = True
    writer.enable_table_devices = False
    writer.enable_table_areas = False
    writer.enable_table_users = False
    writer.enable_table_integrations = False
    writer.enqueue = MagicMock()

    async def set_a_state_while_loading(entry, platforms):
        # Twice: the snapshot alone would only ever see the second value, so
        # this is what tells the two orders apart.
        hass.states.async_set("sensor.set_during_platform_setup", "7")
        hass.states.async_set("sensor.set_during_platform_setup", "8")

    mock_config_entry.add_to_hass(hass)
    with (
        patch("custom_components.scribe.ScribeWriter", return_value=writer),
        patch.object(
            hass.config_entries,
            "async_forward_entry_setups",
            side_effect=set_a_state_while_loading,
        ),
    ):
        assert await async_setup_entry(hass, mock_config_entry)
        await hass.async_block_till_done()

    queued = [
        call.args[0]["value"]
        for call in writer.enqueue.mock_calls
        if call.args[0].get("entity_id") == "sensor.set_during_platform_setup"
    ]
    assert queued == [7.0, 8.0], (
        "both transitions must be recorded; with the platforms set up first "
        "only the snapshot sees this entity, and only its last value"
    )


@pytest.mark.parametrize(
    ("state", "expected_value", "expected_state"),
    [
        ("21.5", 21.5, None),
        ("-3", -3.0, None),
        ("unavailable", None, "unavailable"),
        ("", None, ""),
    ],
)
def test_a_state_is_split_into_a_number_or_a_text(
    state, expected_value, expected_state
):
    """The snapshot and the listener must not disagree on what a state is."""
    holder = MagicMock()
    holder.state = state
    holder.entity_id = "sensor.x"
    holder.attributes = {}

    row = _state_row(holder, set())

    assert row["value"] == expected_value
    assert row["state"] == expected_state
