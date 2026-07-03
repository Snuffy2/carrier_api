"""Tests for merging Carrier websocket updates into loaded system models."""

import json
from pathlib import Path
from typing import Any

import pytest

from carrier_api import (
    ActivityTypes,
    Config,
    Energy,
    FanModes,
    Profile,
    Status,
    System,
    WebsocketDataUpdater,
)
from carrier_api.api_websocket_data_updater import find_by_id

FIXTURE_ROOT = Path(__file__).parent


@pytest.fixture
def system_response() -> dict[str, Any]:
    """Load the GraphQL systems fixture.

    Returns:
        The parsed systems response fixture.
    """
    response = json.loads((FIXTURE_ROOT / "graphql/systems.json").read_text())
    if not isinstance(response, dict):
        raise TypeError("systems fixture must contain a JSON object")
    return response


@pytest.fixture
def energy_response() -> dict[str, Any]:
    """Load the GraphQL energy fixture.

    Returns:
        The parsed energy response fixture.
    """
    response = json.loads((FIXTURE_ROOT / "graphql/energy.json").read_text())
    if not isinstance(response, dict):
        raise TypeError("energy fixture must contain a JSON object")
    return response


@pytest.fixture
def systems(system_response: dict[str, Any], energy_response: dict[str, Any]) -> list[System]:
    """Build Carrier systems from stored API fixtures.

    Args:
        system_response: The parsed systems response fixture.
        energy_response: The parsed energy response fixture.

    Returns:
        Carrier systems built from the fixture responses.
    """
    prepared_systems: list[System] = []
    for single_system_response in system_response["infinitySystems"]:
        profile = Profile(raw=single_system_response["profile"])
        status = Status(raw=single_system_response["status"])
        config = Config(raw=single_system_response["config"])
        energy = Energy(raw=energy_response["infinityEnergy"])
        prepared_systems.append(
            System(profile=profile, status=status, config=config, energy=energy)
        )
    return prepared_systems


@pytest.fixture
def data_updater(systems: list[System]) -> WebsocketDataUpdater:
    """Build a websocket data updater for the prepared systems.

    Args:
        systems: Carrier systems built from fixture responses.

    Returns:
        A websocket data updater using the prepared systems.
    """
    return WebsocketDataUpdater(systems)


@pytest.fixture
def carrier_system(systems: list[System]) -> System:
    """Return the primary Carrier system under test.

    Args:
        systems: Carrier systems built from fixture responses.

    Returns:
        The first Carrier system from the fixtures.
    """
    return systems[0]


@pytest.fixture
def websocket_message_str(request: pytest.FixtureRequest) -> str:
    """Load a websocket message fixture selected by the test.

    Args:
        request: The pytest fixture request containing the message path parameter.

    Returns:
        The raw websocket message fixture contents.
    """
    message_path = request.param
    if not isinstance(message_path, str):
        raise TypeError("websocket message fixture parameter must be a string")
    return (FIXTURE_ROOT / message_path).read_text()


def test_find_by_id_error_message_omits_collection() -> None:
    """Raise a concise formatted error when no collection item matches."""
    collection = [{"id": "1", "name": "Zone 1"}]

    with pytest.raises(ValueError) as error:
        find_by_id(collection, "2")

    assert str(error.value) == "id: 2 not found in collection"
    assert error.value.args == ("id: 2 not found in collection",)


def test_carrier_system_error_message_is_formatted(
    data_updater: WebsocketDataUpdater,
) -> None:
    """Raise a formatted error when no loaded system matches a serial number.

    Args:
        data_updater: A websocket data updater built from fixture systems.
    """
    with pytest.raises(ValueError) as error:
        data_updater.carrier_system(serial_id="missing-serial")

    assert str(error.value) == "No carrier_system found for serial missing-serial"
    assert error.value.args == ("No carrier_system found for serial missing-serial",)


@pytest.mark.asyncio
@pytest.mark.parametrize("websocket_message_str", ["messages/status_idu_cfm.json"], indirect=True)
async def test_status_idu_cfm_setup(
    system_response: dict[str, Any],
    carrier_system: System,
    websocket_message_str: str,
) -> None:
    """Verify the base fixture state used before applying an IDU CFM message.

    Args:
        system_response: Parsed GraphQL system fixture.
        carrier_system: Prepared system model built from the fixture.
        websocket_message_str: Raw IDU CFM websocket message fixture.
    """
    assert carrier_system.status.raw == system_response["infinitySystems"][0]["status"]
    assert websocket_message_str


@pytest.mark.asyncio
@pytest.mark.parametrize("websocket_message_str", ["messages/status_idu_cfm.json"], indirect=True)
async def test_status_idu_cfm_message_handler(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
    websocket_message_str: str,
) -> None:
    """Apply an IDU CFM status message and rebuild the status model.

    Args:
        data_updater: Websocket updater under test.
        carrier_system: Prepared system model that receives the update.
        websocket_message_str: Raw IDU CFM websocket message fixture.
    """
    assert carrier_system.status.airflow_cfm == 1239
    await data_updater.message_handler(websocket_message_str)
    assert carrier_system.status.airflow_cfm == 525
    assert Status(raw=carrier_system.status.raw).airflow_cfm == 525


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "websocket_message_str", ["messages/status_odu_opmode.json"], indirect=True
)
async def test_status_odu_opmode_message_handler(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
    websocket_message_str: str,
) -> None:
    """Apply an ODU operating mode status message without losing status state.

    Args:
        data_updater: Websocket updater under test.
        carrier_system: Prepared system model that receives the update.
        websocket_message_str: Raw ODU operating mode websocket message fixture.
    """
    assert carrier_system.status.mode == "heat"
    await data_updater.message_handler(websocket_message_str)
    assert carrier_system.status.mode == "heat"
    assert Status(raw=carrier_system.status.raw).mode == "heat"


@pytest.mark.asyncio
@pytest.mark.parametrize("websocket_message_str", ["messages/status_zone_rh.json"], indirect=True)
async def test_status_zone_rh_message_handler(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
    websocket_message_str: str,
) -> None:
    """Apply a zone humidity status message to the matching zone.

    Args:
        data_updater: Websocket updater under test.
        carrier_system: Prepared system model that receives the update.
        websocket_message_str: Raw zone humidity websocket message fixture.
    """
    assert carrier_system.status.zones[0].humidity == 32
    await data_updater.message_handler(websocket_message_str)
    assert carrier_system.status.zones[0].humidity == 34
    assert Status(raw=carrier_system.status.raw).zones[0].humidity == 34


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "websocket_message_str", ["messages/status_zone_conditioning.json"], indirect=True
)
async def test_status_zone_conditioning_message_handler(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
    websocket_message_str: str,
) -> None:
    """Apply a zone conditioning status message and preserve parseability.

    Args:
        data_updater: Websocket updater under test.
        carrier_system: Prepared system model that receives the update.
        websocket_message_str: Raw zone conditioning websocket message fixture.
    """
    assert carrier_system.status.zones[0].conditioning == "active_heat"
    await data_updater.message_handler(websocket_message_str)
    assert carrier_system.status.zones[0].conditioning == "idle"
    assert Status(raw=carrier_system.status.raw).zones[0].conditioning == "idle"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "websocket_message_str", ["messages/status_zone_activity.json"], indirect=True
)
async def test_status_zone_activity_message_handler(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
    websocket_message_str: str,
) -> None:
    """Apply a zone activity status message with set point changes.

    Args:
        data_updater: Websocket updater under test.
        carrier_system: Prepared system model that receives the update.
        websocket_message_str: Raw zone activity websocket message fixture.
    """
    assert carrier_system.status.zones[0].current_status_activity_type == ActivityTypes.WAKE
    assert carrier_system.status.zones[0].heat_set_point == 74
    assert carrier_system.status.zones[0].cool_set_point == 78

    await data_updater.message_handler(websocket_message_str)
    assert carrier_system.status.zones[0].current_status_activity_type == ActivityTypes.HOME
    assert carrier_system.status.zones[0].heat_set_point == 77
    assert carrier_system.status.zones[0].cool_set_point == 79
    reprocessed_status = Status(raw=carrier_system.status.raw)
    assert reprocessed_status.zones[0].current_status_activity_type == ActivityTypes.HOME
    assert reprocessed_status.zones[0].heat_set_point == 77
    assert reprocessed_status.zones[0].cool_set_point == 79


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "websocket_message_str", ["messages/status_zone_activity_only.json"], indirect=True
)
async def test_status_zone_activity_only_message_handler(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
    websocket_message_str: str,
) -> None:
    """Apply a zone activity-only message while preserving unrelated fan state.

    Args:
        data_updater: Websocket updater under test.
        carrier_system: Prepared system model that receives the update.
        websocket_message_str: Raw activity-only websocket message fixture.
    """
    assert carrier_system.status.zones[0].current_status_activity_type == ActivityTypes.WAKE
    assert carrier_system.status.zones[0].fan == FanModes.MED
    await data_updater.message_handler(websocket_message_str)
    assert carrier_system.status.zones[0].current_status_activity_type == ActivityTypes.SLEEP
    assert carrier_system.status.zones[0].fan == FanModes.MED
    reprocessed_status = Status(raw=carrier_system.status.raw)
    assert reprocessed_status.zones[0].current_status_activity_type == ActivityTypes.SLEEP
    assert carrier_system.status.zones[0].fan == FanModes.MED


@pytest.mark.asyncio
@pytest.mark.parametrize("websocket_message_str", ["messages/status_zone_hold.json"], indirect=True)
async def test_status_zone_hold_message_handler(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
    websocket_message_str: str,
) -> None:
    """Apply a zone hold status message that moves the zone to manual activity.

    Args:
        data_updater: Websocket updater under test.
        carrier_system: Prepared system model that receives the update.
        websocket_message_str: Raw zone hold websocket message fixture.
    """
    assert carrier_system.status.zones[0].current_status_activity_type == ActivityTypes.WAKE
    await data_updater.message_handler(websocket_message_str)
    assert carrier_system.status.zones[0].current_status_activity_type == ActivityTypes.MANUAL
    reprocessed_status = Status(raw=carrier_system.status.raw)
    assert reprocessed_status.zones[0].current_status_activity_type == ActivityTypes.MANUAL


@pytest.mark.asyncio
@pytest.mark.parametrize("websocket_message_str", ["messages/status_zone_htsp.json"], indirect=True)
async def test_status_zone_htsp_message_handler(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
    websocket_message_str: str,
) -> None:
    """Apply zone heat and cool set point changes from a status message.

    Args:
        data_updater: Websocket updater under test.
        carrier_system: Prepared system model that receives the update.
        websocket_message_str: Raw zone set point websocket message fixture.
    """
    assert carrier_system.status.zones[0].heat_set_point == 74
    assert carrier_system.status.zones[0].cool_set_point == 78

    await data_updater.message_handler(websocket_message_str)
    assert carrier_system.status.zones[0].heat_set_point == 72
    assert carrier_system.status.zones[0].cool_set_point == 85
    reprocessed_status = Status(raw=carrier_system.status.raw)
    assert reprocessed_status.zones[0].heat_set_point == 72
    assert reprocessed_status.zones[0].cool_set_point == 85


async def _send_zone_config(
    data_updater: WebsocketDataUpdater,
    *,
    zone_id: int = 1,
    hold: str = "on",
    hold_activity: str | None = "manual",
    heat_set_point: float = 65,
    cool_set_point: float = 75,
) -> None:
    """Send a compact manual activity config websocket update.

    Args:
        data_updater: Websocket updater under test.
        zone_id: Carrier zone identifier to update.
        hold: Raw Carrier hold flag for the zone.
        hold_activity: Raw Carrier hold activity for the zone.
        heat_set_point: Manual activity heat set point.
        cool_set_point: Manual activity cool set point.
    """
    await data_updater.message_handler(
        json.dumps(
            {
                "messageType": "InfinityConfig",
                "deviceId": "SERIALXXX",
                "zones": [
                    {
                        "id": zone_id,
                        "hold": hold,
                        "holdActivity": hold_activity,
                        "activities": [
                            {
                                "id": str(zone_id),
                                "type": "manual",
                                "htsp": heat_set_point,
                                "clsp": cool_set_point,
                            }
                        ],
                    }
                ],
            }
        )
    )


async def _send_zone_status(
    data_updater: WebsocketDataUpdater,
    zone_update: dict[str, Any],
) -> None:
    """Send a compact zone status websocket update.

    Args:
        data_updater: Websocket updater under test.
        zone_update: Raw status zone fragment to send.
    """
    await data_updater.message_handler(
        json.dumps(
            {
                "messageType": "InfinityStatus",
                "deviceId": "SERIALXXX",
                "zones": [zone_update],
            }
        )
    )


def _assert_zone_setpoints(
    carrier_system: System,
    heat_set_point: float,
    cool_set_point: float,
) -> None:
    """Assert current and reparsed zone set points.

    Args:
        carrier_system: Prepared system model to inspect.
        heat_set_point: Expected heat set point.
        cool_set_point: Expected cool set point.
    """
    assert carrier_system.status.zones[0].heat_set_point == heat_set_point
    assert carrier_system.status.zones[0].cool_set_point == cool_set_point
    reprocessed_status = Status(raw=carrier_system.status.raw)
    assert reprocessed_status.zones[0].heat_set_point == heat_set_point
    assert reprocessed_status.zones[0].cool_set_point == cool_set_point


@pytest.mark.asyncio
async def test_status_zone_update_skips_missing_or_unknown_zone_id(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
) -> None:
    """Skip invalid status zone fragments while applying the rest of the batch.

    Args:
        data_updater: Websocket updater under test.
        carrier_system: Prepared system model that receives the update.
    """
    assert carrier_system.status.zones[0].humidity == 32

    await data_updater.message_handler(
        json.dumps(
            {
                "messageType": "InfinityStatus",
                "deviceId": "SERIALXXX",
                "zones": [
                    {"rh": 99},
                    {"id": "unknown-zone", "rh": 88},
                    {"id": 1, "rh": 35},
                ],
            }
        )
    )

    assert carrier_system.status.zones[0].humidity == 35
    assert Status(raw=carrier_system.status.raw).zones[0].humidity == 35


@pytest.mark.asyncio
async def test_status_zone_manual_activity_uses_config_setpoints_when_status_lags(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
) -> None:
    """Ignore stale status set points when manual activity config is newer.

    Args:
        data_updater: Websocket updater under test.
        carrier_system: Prepared system model that receives the update.
    """
    await _send_zone_config(data_updater)

    await _send_zone_status(
        data_updater,
        {
            "id": 1,
            "currentActivity": "manual",
            "hold": "on",
            "htsp": 74,
            "clsp": 78,
        },
    )

    _assert_zone_setpoints(carrier_system, 65, 75)


@pytest.mark.asyncio
async def test_status_zone_manual_activity_preserves_legitimate_status_setpoints(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
) -> None:
    """Keep manual status set points when they do not replay the stale pair.

    Args:
        data_updater: Websocket updater under test.
        carrier_system: Prepared system model that receives the update.
    """
    await _send_zone_config(data_updater)

    await _send_zone_status(
        data_updater,
        {
            "id": 1,
            "currentActivity": "manual",
            "hold": "on",
            "htsp": 79,
            "clsp": 81,
        },
    )

    assert carrier_system.status.zones[0].current_status_activity_type == ActivityTypes.MANUAL
    _assert_zone_setpoints(carrier_system, 79, 81)


@pytest.mark.asyncio
async def test_status_zone_manual_activity_only_payload_uses_config_setpoints(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
) -> None:
    """Apply manual config set points when status enters manual hold without set points.

    Args:
        data_updater: Websocket updater under test.
        carrier_system: Prepared system model that receives the update.
    """
    await _send_zone_config(data_updater)

    await _send_zone_status(
        data_updater,
        {
            "id": 1,
            "currentActivity": "manual",
            "hold": "on",
        },
    )

    _assert_zone_setpoints(carrier_system, 65, 75)


@pytest.mark.asyncio
async def test_status_zone_manual_activity_replay_is_single_use_after_correction(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
) -> None:
    """Allow later real changes after one stale manual status replay is corrected.

    Args:
        data_updater: Websocket updater under test.
        carrier_system: Prepared system model that receives the update.
    """
    await _send_zone_config(data_updater)
    stale_status = {
        "id": 1,
        "currentActivity": "manual",
        "hold": "on",
        "htsp": 74,
        "clsp": 78,
    }

    await _send_zone_status(data_updater, stale_status)
    _assert_zone_setpoints(carrier_system, 65, 75)

    await _send_zone_status(data_updater, stale_status)
    _assert_zone_setpoints(carrier_system, 74, 78)


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_heat_set_point", ["not-a-number", float("nan"), False])
async def test_status_zone_manual_activity_malformed_heat_setpoint_is_not_merged(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
    bad_heat_set_point: Any,
) -> None:
    """Ignore malformed status set points while preserving valid fields.

    Args:
        data_updater: Websocket updater under test.
        carrier_system: Prepared system model that receives the update.
        bad_heat_set_point: Invalid heat set point payload value.
    """
    await _send_zone_status(
        data_updater,
        {
            "id": 1,
            "currentActivity": "manual",
            "hold": "on",
            "htsp": bad_heat_set_point,
            "clsp": 79,
        },
    )

    assert carrier_system.status.zones[0].current_status_activity_type == ActivityTypes.MANUAL
    _assert_zone_setpoints(carrier_system, 74, 79)


@pytest.mark.asyncio
async def test_status_zone_manual_activity_with_incoming_hold_off_keeps_incoming_setpoints(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
) -> None:
    """Avoid stale corrections when incoming status explicitly turns hold off.

    Args:
        data_updater: Websocket updater under test.
        carrier_system: Prepared system model that receives the update.
    """
    await _send_zone_config(data_updater)

    await _send_zone_status(
        data_updater,
        {
            "id": 1,
            "currentActivity": "manual",
            "hold": "off",
            "htsp": 74,
            "clsp": 78,
        },
    )

    assert carrier_system.status.zones[0].current_status_activity_type == ActivityTypes.MANUAL
    assert carrier_system.status.zones[0].hold is False
    _assert_zone_setpoints(carrier_system, 74, 78)


@pytest.mark.asyncio
async def test_status_zone_manual_activity_ignores_stale_status_when_config_hold_cleared(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
) -> None:
    """Keep incoming status set points when config hold is no longer active.

    Args:
        data_updater: Websocket updater under test.
        carrier_system: Prepared system model that receives the update.
    """
    await _send_zone_config(data_updater, hold="off", hold_activity=None)

    await _send_zone_status(
        data_updater,
        {
            "id": 1,
            "currentActivity": "manual",
            "hold": "on",
            "htsp": 74,
            "clsp": 78,
        },
    )

    assert carrier_system.status.zones[0].current_status_activity_type == ActivityTypes.MANUAL
    _assert_zone_setpoints(carrier_system, 74, 78)


@pytest.mark.asyncio
@pytest.mark.parametrize("websocket_message_str", ["messages/config_zone_hold.json"], indirect=True)
async def test_config_zone_hold_message_handler(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
    websocket_message_str: str,
) -> None:
    """Apply a zone hold config message and rebuild the config model.

    Args:
        data_updater: Websocket updater under test.
        carrier_system: Prepared system model that receives the update.
        websocket_message_str: Raw zone hold config websocket message fixture.
    """
    assert carrier_system.config.zones[0].hold_activity is None
    await data_updater.message_handler(websocket_message_str)
    assert carrier_system.config.zones[0].hold_activity == ActivityTypes.MANUAL
    reprocessed_config = Config(raw=carrier_system.config.raw)
    assert reprocessed_config.zones[0].hold_activity == ActivityTypes.MANUAL


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "websocket_message_str", ["messages/config_zone_program.json"], indirect=True
)
async def test_config_zone_program_message_handler(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
    websocket_message_str: str,
) -> None:
    """Apply a zone program config message without corrupting schedule data.

    Args:
        data_updater: Websocket updater under test.
        carrier_system: Prepared system model that receives the update.
        websocket_message_str: Raw zone program config websocket message fixture.
    """
    await data_updater.message_handler(websocket_message_str)
    reprocessed_config = Config(raw=carrier_system.config.raw)
    assert carrier_system.config.zones[0].program_json == reprocessed_config.zones[0].program_json


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "websocket_message_str", ["messages/heartbeat_with_no_device_id.json"], indirect=True
)
async def test_heartbeat_with_no_device_id_message_handler(
    data_updater: WebsocketDataUpdater,
    websocket_message_str: str,
) -> None:
    """Ignore a heartbeat message that does not identify a Carrier device.

    Args:
        data_updater: Websocket updater under test.
        websocket_message_str: Raw heartbeat websocket message fixture.
    """
    await data_updater.message_handler(websocket_message_str)
