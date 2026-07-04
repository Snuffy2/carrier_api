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
TEST_DEVICE_ID = "SERIALXXX"
TEST_ZONE_ID = 1
DEFAULT_STATUS_SETPOINTS = (74.0, 78.0)
DEFAULT_MANUAL_SETPOINTS = (65.0, 75.0)
TEST_REPLAY_KEY = (TEST_DEVICE_ID, str(TEST_ZONE_ID))


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
    zone_id: int = TEST_ZONE_ID,
    hold: str | bool | int = "on",
    hold_activity: str | None = "manual",
    heat_set_point: float = DEFAULT_MANUAL_SETPOINTS[0],
    cool_set_point: float = DEFAULT_MANUAL_SETPOINTS[1],
    timestamp: str | None = None,
) -> None:
    """Send a compact manual activity config websocket update.

    Args:
        data_updater: Websocket updater under test.
        zone_id: Carrier zone identifier to update.
        hold: Raw Carrier hold flag for the zone.
        hold_activity: Raw Carrier hold activity for the zone.
        heat_set_point: Manual activity heat set point.
        cool_set_point: Manual activity cool set point.
        timestamp: Raw ISO timestamp on the outbound status/config frame.
    """
    await data_updater.message_handler(
        json.dumps(
            {
                "messageType": "InfinityConfig",
                "deviceId": TEST_DEVICE_ID,
                "zones": [
                    {
                        "id": zone_id,
                        "hold": hold,
                        "holdActivity": hold_activity,
                        **({"timestamp": timestamp} if timestamp is not None else {}),
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
    *,
    timestamp: str | None = None,
) -> None:
    """Send a compact zone status websocket update.

    Args:
        data_updater: Websocket updater under test.
        zone_update: Raw status zone fragment to send.
        timestamp: Raw ISO timestamp on the outbound status frame.
    """
    await data_updater.message_handler(
        json.dumps(
            {
                "messageType": "InfinityStatus",
                "deviceId": TEST_DEVICE_ID,
                "zones": [
                    {
                        **zone_update,
                        **({"timestamp": timestamp} if timestamp is not None else {}),
                    }
                ],
            }
        )
    )


def _manual_status_update(
    *,
    heat_set_point: Any = DEFAULT_STATUS_SETPOINTS[0],
    cool_set_point: Any = DEFAULT_STATUS_SETPOINTS[1],
    **zone_update: Any,
) -> dict[str, Any]:
    """Build a compact manual status zone update for replay-focused tests.

    Args:
        heat_set_point: Heat set point to include.
        cool_set_point: Cool set point to include.
        **zone_update: Zone fields that override the default manual payload.

    Returns:
        A raw status zone fragment suitable for ``_send_zone_status``.
    """
    update = {
        "id": TEST_ZONE_ID,
        "currentActivity": "manual",
        "hold": "on",
        "htsp": heat_set_point,
        "clsp": cool_set_point,
    }
    update.update(zone_update)
    return update


def _seed_manual_replay(
    data_updater: WebsocketDataUpdater,
    *,
    stale_set_points: list[tuple[float, float]] | None = None,
    manual_set_points: tuple[float, float] = DEFAULT_MANUAL_SETPOINTS,
) -> None:
    """Seed manual replay state directly for narrow replay tests.

    Args:
        data_updater: Websocket updater under test.
        stale_set_points: Candidate stale status set point pairs.
        manual_set_points: Active manual config set point pair.
    """
    data_updater._manual_status_replays[TEST_REPLAY_KEY] = (
        stale_set_points or [DEFAULT_STATUS_SETPOINTS],
        manual_set_points,
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
                "deviceId": TEST_DEVICE_ID,
                "zones": [
                    {"rh": 99},
                    {"id": "unknown-zone", "rh": 88},
                    {"id": TEST_ZONE_ID, "rh": 35},
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
        _manual_status_update(),
    )

    _assert_zone_setpoints(carrier_system, *DEFAULT_MANUAL_SETPOINTS)


@pytest.mark.asyncio
async def test_status_zone_manual_activity_uses_config_setpoints_without_current_activity(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
) -> None:
    """Align stale set points when config is manual even without status activity."""
    await _send_zone_config(data_updater)

    await _send_zone_status(
        data_updater,
        {
            "id": TEST_ZONE_ID,
            "hold": "on",
            "htsp": DEFAULT_STATUS_SETPOINTS[0],
            "clsp": DEFAULT_STATUS_SETPOINTS[1],
        },
    )

    _assert_zone_setpoints(carrier_system, *DEFAULT_MANUAL_SETPOINTS)


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
            "id": TEST_ZONE_ID,
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
            "id": TEST_ZONE_ID,
            "currentActivity": "manual",
            "hold": "on",
        },
    )

    _assert_zone_setpoints(carrier_system, *DEFAULT_MANUAL_SETPOINTS)


@pytest.mark.asyncio
async def test_status_zone_manual_activity_replay_is_single_use_after_correction(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
) -> None:
    """Keep replay armed until status actually catches up to manual values.

    Args:
        data_updater: Websocket updater under test.
        carrier_system: Prepared system model that receives the update.
    """
    await _send_zone_config(data_updater)
    stale_status = _manual_status_update()

    await _send_zone_status(data_updater, stale_status)
    _assert_zone_setpoints(carrier_system, *DEFAULT_MANUAL_SETPOINTS)

    await _send_zone_status(data_updater, stale_status)
    _assert_zone_setpoints(carrier_system, *DEFAULT_MANUAL_SETPOINTS)

    await _send_zone_status(
        data_updater,
        {
            "id": TEST_ZONE_ID,
            "currentActivity": "manual",
            "hold": "on",
            "htsp": DEFAULT_MANUAL_SETPOINTS[0],
            "clsp": DEFAULT_MANUAL_SETPOINTS[1],
        },
    )
    _assert_zone_setpoints(carrier_system, *DEFAULT_MANUAL_SETPOINTS)

    await _send_zone_status(data_updater, stale_status)
    _assert_zone_setpoints(carrier_system, *DEFAULT_STATUS_SETPOINTS)


@pytest.mark.asyncio
async def test_status_zone_manual_activity_keeps_replay_through_stale_non_manual_status(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
) -> None:
    """Keep replay armed through stale non-manual status before manual-only correction."""
    await _send_zone_config(data_updater)

    await _send_zone_status(
        data_updater,
        {
            "id": TEST_ZONE_ID,
            "currentActivity": "wake",
            "htsp": DEFAULT_STATUS_SETPOINTS[0],
            "clsp": DEFAULT_STATUS_SETPOINTS[1],
        },
    )
    assert TEST_REPLAY_KEY in data_updater._manual_status_replays

    await _send_zone_status(
        data_updater,
        {
            "id": TEST_ZONE_ID,
            "currentActivity": "manual",
            "hold": "on",
        },
    )
    _assert_zone_setpoints(carrier_system, *DEFAULT_MANUAL_SETPOINTS)


@pytest.mark.asyncio
async def test_status_zone_manual_activity_full_pair_matching_manual_setpoints_clears_replay_state(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
) -> None:
    """Clear stale replay state when incoming status arrives at the active manual pair."""
    _seed_manual_replay(
        data_updater,
        stale_set_points=[DEFAULT_STATUS_SETPOINTS, DEFAULT_MANUAL_SETPOINTS],
    )

    await _send_zone_status(
        data_updater,
        _manual_status_update(
            heat_set_point=DEFAULT_MANUAL_SETPOINTS[0],
            cool_set_point=DEFAULT_MANUAL_SETPOINTS[1],
        ),
    )

    assert TEST_REPLAY_KEY not in data_updater._manual_status_replays
    _assert_zone_setpoints(carrier_system, *DEFAULT_MANUAL_SETPOINTS)


@pytest.mark.asyncio
async def test_status_zone_manual_activity_partial_status_disproves_replay_only_on_valid_side(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
) -> None:
    """Keep replay when malformed one-side values arrive; clear it on a contradictory side."""
    _seed_manual_replay(data_updater)

    await _send_zone_status(
        data_updater,
        {
            "id": TEST_ZONE_ID,
            "currentActivity": "manual",
            "hold": "on",
            "htsp": 73,
        },
    )
    assert TEST_REPLAY_KEY in data_updater._manual_status_replays
    _assert_zone_setpoints(carrier_system, 73, DEFAULT_MANUAL_SETPOINTS[1])

    _seed_manual_replay(data_updater)
    await _send_zone_status(
        data_updater,
        _manual_status_update(heat_set_point="bad"),
    )

    assert TEST_REPLAY_KEY in data_updater._manual_status_replays
    _assert_zone_setpoints(carrier_system, 73, DEFAULT_STATUS_SETPOINTS[1])


@pytest.mark.asyncio
async def test_status_zone_manual_activity_partial_status_disproves_replay_only_on_cool_side(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
) -> None:
    """Backfill missing heat set point when only cool arrives in manual status."""
    _seed_manual_replay(data_updater)

    await _send_zone_status(
        data_updater,
        {
            "id": TEST_ZONE_ID,
            "currentActivity": "manual",
            "hold": "on",
            "clsp": 73,
        },
    )
    assert TEST_REPLAY_KEY in data_updater._manual_status_replays
    _assert_zone_setpoints(carrier_system, DEFAULT_MANUAL_SETPOINTS[0], 73)


@pytest.mark.asyncio
async def test_status_zone_manual_activity_two_one_sided_updates_do_not_reintroduce_stale_values(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
) -> None:
    """Keep stale-replay correction after one-sided updates arrive in sequence."""
    _seed_manual_replay(data_updater)

    await _send_zone_status(
        data_updater,
        {
            "id": TEST_ZONE_ID,
            "currentActivity": "manual",
            "hold": "on",
            "clsp": 73,
        },
    )
    assert TEST_REPLAY_KEY in data_updater._manual_status_replays
    _assert_zone_setpoints(carrier_system, DEFAULT_MANUAL_SETPOINTS[0], 73)

    await _send_zone_status(
        data_updater,
        {
            "id": TEST_ZONE_ID,
            "currentActivity": "manual",
            "hold": "on",
            "htsp": DEFAULT_STATUS_SETPOINTS[0],
        },
    )
    assert TEST_REPLAY_KEY in data_updater._manual_status_replays
    _assert_zone_setpoints(carrier_system, DEFAULT_MANUAL_SETPOINTS[0], 73)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("incoming_set_points", "expected_set_points"),
    [((74.0, 75.0), (65.0, 75.0)), ((65.0, 78.0), (65.0, 75.0))],
)
async def test_status_zone_manual_activity_mixed_full_pairs_correct_stale_side_without_clearing_replay(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
    incoming_set_points: tuple[float, float],
    expected_set_points: tuple[float, float],
) -> None:
    """Align mixed manual/stale full frames to the active manual pair and keep replay."""
    _seed_manual_replay(data_updater)

    await _send_zone_status(
        data_updater,
        _manual_status_update(
            heat_set_point=incoming_set_points[0],
            cool_set_point=incoming_set_points[1],
        ),
    )

    assert TEST_REPLAY_KEY in data_updater._manual_status_replays
    _assert_zone_setpoints(carrier_system, expected_set_points[0], expected_set_points[1])


@pytest.mark.asyncio
async def test_status_zone_manual_activity_config_update_keeps_replay_after_local_status_correction(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
) -> None:
    """Do not clear replay for config updates that observe local status correction."""
    _seed_manual_replay(data_updater)
    carrier_system.status.raw["zones"][0].update(
        {
            "htsp": DEFAULT_MANUAL_SETPOINTS[0],
            "clsp": DEFAULT_MANUAL_SETPOINTS[1],
        }
    )
    carrier_system.status = Status(raw=carrier_system.status.raw)

    await _send_zone_config(data_updater)

    assert TEST_REPLAY_KEY in data_updater._manual_status_replays


@pytest.mark.asyncio
async def test_status_zone_manual_activity_stale_config_timestamp_does_not_update_replay(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
) -> None:
    """Handle newer status then delayed manual config without rolling status back."""
    initial_manual_activity = carrier_system.config.zones[0].find_activity(ActivityTypes.MANUAL)
    initial_manual_setpoints = (
        (initial_manual_activity.heat_set_point, initial_manual_activity.cool_set_point)
        if initial_manual_activity is not None
        else None
    )
    initial_hold_activity = carrier_system.config.zones[0].hold_activity
    assert TEST_REPLAY_KEY not in data_updater._manual_status_replays

    await _send_zone_status(
        data_updater,
        _manual_status_update(heat_set_point=70.0, cool_set_point=80.0),
        timestamp="2026-07-04T15:00:00.000Z",
    )
    _assert_zone_setpoints(carrier_system, 70.0, 80.0)
    assert TEST_REPLAY_KEY not in data_updater._manual_status_replays

    await _send_zone_config(
        data_updater,
        heat_set_point=65.0,
        cool_set_point=75.0,
        timestamp="2026-07-04T14:00:00.000Z",
    )
    final_manual_activity = carrier_system.config.zones[0].find_activity(ActivityTypes.MANUAL)
    final_manual_setpoints = (
        (final_manual_activity.heat_set_point, final_manual_activity.cool_set_point)
        if final_manual_activity is not None
        else None
    )
    assert carrier_system.config.zones[0].hold_activity == initial_hold_activity
    assert final_manual_setpoints == initial_manual_setpoints
    assert TEST_REPLAY_KEY not in data_updater._manual_status_replays

    await _send_zone_status(
        data_updater,
        _manual_status_update(heat_set_point=70.0, cool_set_point=80.0),
        timestamp="2026-07-04T15:00:00.000Z",
    )
    _assert_zone_setpoints(carrier_system, 70.0, 80.0)
    assert TEST_REPLAY_KEY not in data_updater._manual_status_replays


@pytest.mark.asyncio
async def test_infinity_config_stale_after_status_no_prior_config_watermark(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
) -> None:
    """Status high-water should block older config frames before config merge."""
    initial_manual_activity = carrier_system.config.zones[0].find_activity(ActivityTypes.MANUAL)
    initial_manual_setpoints = (
        (initial_manual_activity.heat_set_point, initial_manual_activity.cool_set_point)
        if initial_manual_activity is not None
        else None
    )
    initial_hold_activity = carrier_system.config.zones[0].hold_activity

    await _send_zone_status(
        data_updater,
        _manual_status_update(heat_set_point=70.0, cool_set_point=80.0),
        timestamp="2026-07-04T15:00:00.000Z",
    )

    await _send_zone_config(
        data_updater,
        heat_set_point=66.0,
        cool_set_point=76.0,
        timestamp="2026-07-04T14:00:00.000Z",
    )

    final_manual_activity = carrier_system.config.zones[0].find_activity(ActivityTypes.MANUAL)
    final_manual_setpoints = (
        (final_manual_activity.heat_set_point, final_manual_activity.cool_set_point)
        if final_manual_activity is not None
        else None
    )

    assert carrier_system.config.zones[0].hold_activity == initial_hold_activity
    assert final_manual_setpoints == initial_manual_setpoints


@pytest.mark.asyncio
async def test_status_zone_manual_activity_stale_status_does_not_lower_high_water_for_config_gate(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
) -> None:
    """Do not lower last-status high-water with delayed status frames."""
    await _send_zone_status(
        data_updater,
        _manual_status_update(heat_set_point=70.0, cool_set_point=80.0),
        timestamp="2026-07-04T15:00:00.000Z",
    )
    assert TEST_REPLAY_KEY not in data_updater._manual_status_replays
    _assert_zone_setpoints(carrier_system, 70.0, 80.0)

    await _send_zone_status(
        data_updater,
        {"id": TEST_ZONE_ID, "rh": 40},
        timestamp="2026-07-04T14:00:00.000Z",
    )

    await _send_zone_config(
        data_updater,
        heat_set_point=65.0,
        cool_set_point=75.0,
        timestamp="2026-07-04T14:00:00.000Z",
    )
    assert TEST_REPLAY_KEY not in data_updater._manual_status_replays
    _assert_zone_setpoints(carrier_system, 70.0, 80.0)

    await _send_zone_status(
        data_updater,
        _manual_status_update(heat_set_point=70.0, cool_set_point=80.0),
        timestamp="2026-07-04T15:00:00.000Z",
    )
    _assert_zone_setpoints(carrier_system, 70.0, 80.0)


@pytest.mark.asyncio
async def test_infinity_config_stale_frame_with_newer_status_does_not_roll_back_config(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
) -> None:
    """Ignore config zone updates older than latest manual status timestamp for config replay."""
    await _send_zone_config(
        data_updater,
        heat_set_point=66.0,
        cool_set_point=76.0,
        timestamp="2026-07-04T15:00:00.000Z",
    )

    await _send_zone_status(
        data_updater,
        _manual_status_update(heat_set_point=70.0, cool_set_point=80.0),
        timestamp="2026-07-04T15:00:30.000Z",
    )

    await _send_zone_config(
        data_updater,
        heat_set_point=65.0,
        cool_set_point=75.0,
        timestamp="2026-07-04T14:00:00.000Z",
    )

    manual_activity = carrier_system.config.zones[0].find_activity(ActivityTypes.MANUAL)
    assert manual_activity is not None
    assert (manual_activity.heat_set_point, manual_activity.cool_set_point) == (66.0, 76.0)
    assert TEST_REPLAY_KEY not in data_updater._manual_status_replays


@pytest.mark.asyncio
async def test_infinity_config_stale_zone_updates_do_not_roll_back_top_level_keys(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
) -> None:
    """Keep newer top-level config state when a stale zone update is rejected."""
    initial_vacmint = carrier_system.config.raw["vacmint"]

    await _send_zone_config(
        data_updater,
        heat_set_point=66.0,
        cool_set_point=76.0,
        timestamp="2026-07-04T16:00:00.000Z",
    )
    await _send_zone_status(
        data_updater,
        _manual_status_update(heat_set_point=70.0, cool_set_point=80.0),
        timestamp="2026-07-04T16:30:00.000Z",
    )
    manual_activity = carrier_system.config.zones[0].find_activity(ActivityTypes.MANUAL)
    assert manual_activity is not None
    assert (manual_activity.heat_set_point, manual_activity.cool_set_point) == (66.0, 76.0)

    await data_updater.message_handler(
        json.dumps(
            {
                "messageType": "InfinityConfig",
                "deviceId": TEST_DEVICE_ID,
                "zones": [
                    {
                        "id": TEST_ZONE_ID,
                        "hold": "on",
                        "holdActivity": "manual",
                        "timestamp": "2026-07-04T15:00:00.000Z",
                        "activities": [
                            {
                                "id": str(TEST_ZONE_ID),
                                "type": "manual",
                                "htsp": 65.0,
                                "clsp": 75.0,
                            }
                        ],
                    }
                ],
                "vacmint": 10,
            }
        )
    )

    stale_manual_activity = carrier_system.config.zones[0].find_activity(ActivityTypes.MANUAL)
    assert stale_manual_activity is not None
    assert (stale_manual_activity.heat_set_point, stale_manual_activity.cool_set_point) == (
        66.0,
        76.0,
    )
    assert carrier_system.config.raw["vacmint"] == initial_vacmint


@pytest.mark.asyncio
async def test_infinity_config_out_of_order_without_status_does_not_rewrite_config(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
) -> None:
    """Keep newer config manual setpoints when an older config frame arrives later."""
    assert TEST_REPLAY_KEY not in data_updater._manual_status_replays
    await _send_zone_config(
        data_updater,
        heat_set_point=66.0,
        cool_set_point=76.0,
        timestamp="2026-07-04T15:00:00.000Z",
    )
    replay_before = data_updater._manual_status_replays[TEST_REPLAY_KEY]

    await _send_zone_config(
        data_updater,
        heat_set_point=65.0,
        cool_set_point=75.0,
        timestamp="2026-07-04T14:00:00.000Z",
    )

    manual_activity = carrier_system.config.zones[0].find_activity(ActivityTypes.MANUAL)
    assert manual_activity is not None
    assert (manual_activity.heat_set_point, manual_activity.cool_set_point) == (66.0, 76.0)
    assert data_updater._manual_status_replays[TEST_REPLAY_KEY] == replay_before


@pytest.mark.asyncio
async def test_status_zone_manual_activity_config_update_does_not_arm_when_status_already_matches(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
) -> None:
    """Do not create replay state when status already has the manual set points."""
    carrier_system.status.raw["zones"][0].update(
        {
            "htsp": DEFAULT_MANUAL_SETPOINTS[0],
            "clsp": DEFAULT_MANUAL_SETPOINTS[1],
        }
    )
    carrier_system.status = Status(raw=carrier_system.status.raw)

    await _send_zone_config(data_updater)

    assert TEST_REPLAY_KEY not in data_updater._manual_status_replays


@pytest.mark.asyncio
async def test_status_zone_manual_activity_preserves_multiple_stale_pairs_across_updates(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
) -> None:
    """Keep earlier stale pairs so later matching stale frames still align."""
    await _send_zone_config(data_updater)

    await _send_zone_status(
        data_updater,
        {
            "id": TEST_ZONE_ID,
            "currentActivity": "manual",
            "hold": "on",
            "htsp": DEFAULT_STATUS_SETPOINTS[0],
        },
    )
    assert carrier_system.status.zones[0].cool_set_point == DEFAULT_MANUAL_SETPOINTS[1]

    await _send_zone_config(
        data_updater,
        heat_set_point=66,
        cool_set_point=76,
    )

    await _send_zone_status(
        data_updater,
        _manual_status_update(),
    )
    _assert_zone_setpoints(carrier_system, 66, 76)


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
            "id": TEST_ZONE_ID,
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
        _manual_status_update(hold="off"),
    )

    assert carrier_system.status.zones[0].current_status_activity_type == ActivityTypes.MANUAL
    assert carrier_system.status.zones[0].hold is False
    _assert_zone_setpoints(carrier_system, *DEFAULT_STATUS_SETPOINTS)


@pytest.mark.asyncio
async def test_status_zone_manual_activity_stale_hold_off_does_not_clear_replay(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
) -> None:
    """Keep stale replay candidate when a delayed hold-off arrives after manual config."""
    await _send_zone_config(
        data_updater,
        timestamp="2026-07-04T15:00:00.000Z",
    )
    assert TEST_REPLAY_KEY in data_updater._manual_status_replays

    await _send_zone_status(
        data_updater,
        _manual_status_update(
            heat_set_point=DEFAULT_STATUS_SETPOINTS[0],
            cool_set_point=DEFAULT_STATUS_SETPOINTS[1],
            hold="off",
        ),
        timestamp="2026-07-04T14:00:00.000Z",
    )
    assert TEST_REPLAY_KEY in data_updater._manual_status_replays
    assert carrier_system.status.zones[0].hold is False

    await _send_zone_status(data_updater, _manual_status_update())
    assert TEST_REPLAY_KEY in data_updater._manual_status_replays
    _assert_zone_setpoints(carrier_system, *DEFAULT_MANUAL_SETPOINTS)


@pytest.mark.asyncio
@pytest.mark.parametrize("status_hold", [True, 1])
async def test_status_zone_update_normalizes_hold_for_status_model(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
    status_hold: bool | int,
) -> None:
    """Normalize boolean and numeric hold values before rebuilding status zones."""
    await _send_zone_status(
        data_updater,
        {
            "id": TEST_ZONE_ID,
            "currentActivity": "manual",
            "hold": status_hold,
        },
    )

    assert carrier_system.status.zones[0].hold is True


@pytest.mark.asyncio
async def test_status_zone_manual_activity_invalid_config_clears_stale_replay_state(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
) -> None:
    """Clear stale replay when a manual config payload has invalid set points."""
    await _send_zone_config(data_updater)

    await data_updater.message_handler(
        json.dumps(
            {
                "messageType": "InfinityConfig",
                "deviceId": TEST_DEVICE_ID,
                "zones": [
                    {
                        "id": TEST_ZONE_ID,
                        "hold": "on",
                        "holdActivity": "manual",
                        "activities": [
                            {
                                "id": str(TEST_ZONE_ID),
                                "type": "manual",
                                "htsp": "not-a-number",
                                "clsp": DEFAULT_MANUAL_SETPOINTS[1],
                            }
                        ],
                    }
                ],
            }
        )
    )

    await _send_zone_status(
        data_updater,
        _manual_status_update(),
    )

    _assert_zone_setpoints(carrier_system, *DEFAULT_STATUS_SETPOINTS)


@pytest.mark.asyncio
@pytest.mark.parametrize("websocket_message_str", ["messages/config_zone_hold.json"], indirect=True)
async def test_config_zone_hold_only_message_arms_manual_replay(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
    websocket_message_str: str,
) -> None:
    """Arm manual replay when config hold frames omit a hold value."""
    assert TEST_REPLAY_KEY not in data_updater._manual_status_replays

    await data_updater.message_handler(websocket_message_str)
    await _send_zone_status(
        data_updater,
        _manual_status_update(),
    )

    manual_activity = carrier_system.config.zones[0].find_activity(ActivityTypes.MANUAL)
    assert manual_activity is not None
    _assert_zone_setpoints(
        carrier_system,
        manual_activity.heat_set_point,
        manual_activity.cool_set_point,
    )
    assert TEST_REPLAY_KEY in data_updater._manual_status_replays


@pytest.mark.asyncio
async def test_infinity_config_with_missing_status_zone_keeps_updater_stable(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
) -> None:
    """Handle config updates even when previous status is missing that zone."""
    carrier_system.status.raw["zones"] = [
        zone for zone in carrier_system.status.raw["zones"] if str(zone["id"]) != "1"
    ]
    carrier_system.status = Status(raw=carrier_system.status.raw)

    status_zone_ids = [str(zone["id"]) for zone in carrier_system.status.raw["zones"]]

    await _send_zone_config(data_updater)

    assert [str(zone["id"]) for zone in carrier_system.status.raw["zones"]] == status_zone_ids
    assert carrier_system.config.zones[0].hold_activity == ActivityTypes.MANUAL


@pytest.mark.asyncio
@pytest.mark.parametrize("config_hold", [True, 1])
async def test_infinity_config_normalizes_hold_for_config_model(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
    config_hold: bool | int,
) -> None:
    """Normalize boolean and numeric hold values before rebuilding config zones."""
    await data_updater.message_handler(
        json.dumps(
            {
                "messageType": "InfinityConfig",
                "deviceId": TEST_DEVICE_ID,
                "zones": [
                    {
                        "id": TEST_ZONE_ID,
                        "hold": config_hold,
                        "holdActivity": "manual",
                        "activities": [
                            {
                                "id": str(TEST_ZONE_ID),
                                "type": "manual",
                                "htsp": DEFAULT_MANUAL_SETPOINTS[0],
                                "clsp": DEFAULT_MANUAL_SETPOINTS[1],
                            }
                        ],
                    }
                ],
            }
        )
    )

    assert carrier_system.config.zones[0].hold is True
    manual_activity = carrier_system.config.zones[0].find_activity(ActivityTypes.MANUAL)
    assert manual_activity is not None
    assert (
        manual_activity.heat_set_point,
        manual_activity.cool_set_point,
    ) == DEFAULT_MANUAL_SETPOINTS


@pytest.mark.asyncio
async def test_infinity_config_skips_missing_zone_and_activity_ids(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
) -> None:
    """Skip missing config zone/activity ids while applying valid updates."""
    await data_updater.message_handler(
        json.dumps(
            {
                "messageType": "InfinityConfig",
                "deviceId": TEST_DEVICE_ID,
                "zones": [
                    {
                        "id": "missing-zone",
                        "hold": "on",
                        "holdActivity": "manual",
                    },
                    {
                        "id": TEST_ZONE_ID,
                        "hold": "on",
                        "holdActivity": "manual",
                        "activities": [
                            {
                                "id": "missing-activity",
                                "htsp": 63,
                                "clsp": 77,
                            },
                            {
                                "id": str(TEST_ZONE_ID),
                                "type": "manual",
                                "htsp": 66,
                                "clsp": 76,
                            },
                        ],
                    },
                ],
            }
        )
    )

    assert carrier_system.config.zones[0].hold_activity == ActivityTypes.MANUAL
    manual_activity = carrier_system.config.zones[0].find_activity(ActivityTypes.MANUAL)
    assert manual_activity is not None
    assert (manual_activity.heat_set_point, manual_activity.cool_set_point) == (66.0, 76.0)


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
        _manual_status_update(),
    )

    assert carrier_system.status.zones[0].current_status_activity_type == ActivityTypes.MANUAL
    _assert_zone_setpoints(carrier_system, *DEFAULT_STATUS_SETPOINTS)


@pytest.mark.asyncio
async def test_status_zone_manual_activity_uses_config_setpoints_when_hold_true(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
) -> None:
    """Accept boolean hold values as equivalent to ``on`` for replay arming."""
    await _send_zone_config(data_updater, hold=True)

    await _send_zone_status(
        data_updater,
        _manual_status_update(hold=True),
    )

    _assert_zone_setpoints(carrier_system, *DEFAULT_MANUAL_SETPOINTS)


@pytest.mark.asyncio
async def test_status_zone_manual_activity_humidity_only_frame_does_not_rewrite_setpoints(
    data_updater: WebsocketDataUpdater,
    carrier_system: System,
) -> None:
    """Avoid injecting manual set points when a replayed frame has no manual signal."""
    _seed_manual_replay(data_updater)

    await _send_zone_status(
        data_updater,
        {
            "id": TEST_ZONE_ID,
            "rh": 42,
        },
    )

    assert carrier_system.status.zones[0].humidity == 42
    _assert_zone_setpoints(carrier_system, *DEFAULT_STATUS_SETPOINTS)


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
