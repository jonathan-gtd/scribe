"""Reading the device registry at startup.

Home Assistant 2026.9 turned `device_registry.devices` from a mapping into a
collection of entries, deprecated every mapping method on it — to stop working
in 2027.9 — and added child devices, kept apart and without a model or
manufacturer of their own (#56). These run against the real registry of
whichever Home Assistant the suite is pinned to, so the same test covers the
mapping before 2026.9 and the collection after it.
"""

from unittest.mock import patch

import pytest
from homeassistant.helpers import device_registry as dr
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.scribe import _collect_devices, _device_row


@pytest.mark.asyncio
async def test_every_device_is_read_without_a_deprecation(hass, caplog):
    """`.values()` on the 2026.9 collection logs a warning and breaks in 2027.9."""
    entry = MockConfigEntry(domain="hue")
    entry.add_to_hass(hass)
    registry = dr.async_get(hass)
    device = registry.async_get_or_create(
        config_entry_id=entry.entry_id,
        identifiers={("hue", "bulb-1")},
        name="Hue Bulb",
        manufacturer="Philips",
        model="LCT001",
        sw_version="1.0.0",
    )

    rows = _collect_devices(hass)

    assert [row["device_id"] for row in rows] == [device.id]
    assert rows[0]["manufacturer"] == "Philips"
    assert rows[0]["model"] == "LCT001"
    assert rows[0]["primary_config_entry"] == entry.entry_id
    assert "deprecated" not in caplog.text


class _ChildDevice:
    """A 2026.9 child device: the DeviceEntry-only fields are not there.

    Every read of a missing field is recorded, not only refused: an
    AttributeError alone would pass unnoticed through a `getattr(d, name,
    None)` in the code under test, which is exactly the read to catch.
    """

    id = "child_1"
    name = "Outlet 2"
    name_by_user = None
    area_id = "kitchen"
    config_entries = {"entry_1"}

    def __init__(self):
        self.missing_reads = []

    def __getattr__(self, name):
        self.missing_reads.append(name)
        raise AttributeError(name)


def test_a_child_device_is_not_asked_for_what_it_does_not_have():
    """Home Assistant answers None, but logs a deprecation on every such read."""
    child = _ChildDevice()
    with patch("custom_components.scribe._CHILD_DEVICE_ENTRY", _ChildDevice):
        row = _device_row(child)

    assert child.missing_reads == []
    assert row == {
        "device_id": "child_1",
        "name": "Outlet 2",
        "name_by_user": None,
        "model": None,
        "manufacturer": None,
        "sw_version": None,
        "area_id": "kitchen",
        "primary_config_entry": "entry_1",
    }
