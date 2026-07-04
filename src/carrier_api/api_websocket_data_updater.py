"""Apply Carrier realtime websocket messages to in-memory system models."""

from datetime import UTC, datetime
from json import loads
from logging import getLogger
from math import isfinite
from typing import Any

from deepmerge import always_merger

from .config import Config
from .const import ActivityTypes
from .status import Status
from .system import System

_LOGGER = getLogger(__name__)

SetPointPair = tuple[float, float]
ManualReplay = tuple[list[SetPointPair], SetPointPair]


def _is_hold_on(value: Any) -> bool:
    """Return ``True`` when a Carrier hold value indicates an active hold."""
    return value in ("on", True, 1)


def find_by_id(collection: list[dict], item_id: str) -> dict:
    """Find an item in a Carrier payload collection by id.

    Args:
        collection: List of dictionaries containing Carrier ``id`` fields.
        item_id: Identifier to match, compared as a string for API consistency.

    Returns:
        The matching dictionary from the collection.

    Raises:
        ValueError: If no item in the collection has the requested id.
    """
    for item in collection:
        if str(item["id"]) == str(item_id):
            return item
    raise ValueError(f"id: {item_id} not found in collection")


def _align_manual_status_setpoints_with_config(
    system: System,
    zone: dict[str, Any],
    replay: ManualReplay | None,
) -> bool:
    """Replace stale manual status set points with matching config values.

    Args:
        system: Loaded Carrier system whose config contains activity profiles.
        zone: Incoming status zone payload to normalize before merging.
        replay: Stale status pair and replacement manual config pair.

    Returns:
        ``True`` when the incoming payload was aligned with config set points.
    """
    if replay is None or "id" not in zone:
        return False
    stale_set_points, manual_set_points = replay
    incoming_heat_set_point = _float_set_point(zone.get("htsp"))
    incoming_cool_set_point = _float_set_point(zone.get("clsp"))
    incoming_activity = _activity_type(zone.get("currentActivity"))
    incoming_hold = zone.get("hold")
    if "currentActivity" in zone and incoming_activity is not ActivityTypes.MANUAL:
        return False
    if incoming_hold is not None and not _is_hold_on(incoming_hold):
        return False

    try:
        raw_status_zone = find_by_id(system.status.raw["zones"], zone["id"])
    except ValueError:
        return False
    try:
        config_zone = find_by_id(system.config.raw["zones"], zone["id"])
    except ValueError:
        return False

    incoming_activity_value = zone.get("currentActivity")
    status_is_manual = (
        _activity_type(incoming_activity_value) is ActivityTypes.MANUAL
        if incoming_activity_value is not None
        else _activity_type(raw_status_zone.get("currentActivity")) is ActivityTypes.MANUAL
    )
    config_is_manual = (
        _is_hold_on(config_zone.get("hold"))
        and config_zone.get("holdActivity") == ActivityTypes.MANUAL.value
    )
    if not status_is_manual and not config_is_manual:
        return False

    incoming_pair = _raw_set_point_pair(zone)
    incoming_manual_signal = _activity_type(
        zone.get("currentActivity")
    ) is ActivityTypes.MANUAL or _is_hold_on(zone.get("hold"))

    if incoming_pair is not None:
        if incoming_pair == manual_set_points or incoming_pair not in stale_set_points:
            return False
    elif (
        "htsp" in zone
        or "clsp" in zone
        or not incoming_manual_signal
        or _raw_set_point_pair(raw_status_zone) not in stale_set_points
    ):
        return False

    zone["htsp"], zone["clsp"] = manual_set_points
    _LOGGER.debug(
        "Replacing stale manual status set points for zone %s: raw=%s/%s, local=%s/%s",
        zone["id"],
        incoming_heat_set_point,
        incoming_cool_set_point,
        zone["htsp"],
        zone["clsp"],
    )
    return True


def _float_set_point(value: Any) -> float | None:
    """Convert a potential set point value into a float if valid.

    Args:
        value: Raw Carrier value for a heat/cool set point.

    Returns:
        The parsed float value, or ``None`` when conversion is not possible.
    """
    if isinstance(value, bool):
        return None

    try:
        parsed = float(value)
    except TypeError, ValueError:
        return None

    if not isfinite(parsed):
        return None

    return parsed


def _activity_type(value: Any) -> ActivityTypes | None:
    """Return a Carrier activity type when the value is valid.

    Args:
        value: Raw Carrier activity value.

    Returns:
        Parsed activity type, or ``None`` when unavailable or invalid.
    """
    try:
        return ActivityTypes(value)
    except TypeError, ValueError:
        return None


def _raw_set_point_pair(zone: dict[str, Any]) -> SetPointPair | None:
    """Return a finite heat/cool set point pair from a raw zone payload.

    Args:
        zone: Raw status or config zone payload.

    Returns:
        A finite heat/cool set point pair, or ``None`` when unavailable.
    """
    heat_set_point = _float_set_point(zone.get("htsp"))
    cool_set_point = _float_set_point(zone.get("clsp"))
    if heat_set_point is None or cool_set_point is None:
        return None
    return (heat_set_point, cool_set_point)


def _drop_non_finite_setpoints(zone: dict[str, Any]) -> None:
    """Remove non-finite status set points before merging raw payloads.

    Args:
        zone: Incoming status zone payload.
    """
    for key in ("htsp", "clsp"):
        if key not in zone:
            continue
        if _float_set_point(zone[key]) is None:
            zone.pop(key)


class WebsocketDataUpdater:
    """Merge Carrier websocket payloads into existing system model instances."""

    def __init__(
        self,
        systems: list[System],
    ) -> None:
        """Create a data updater for a set of Carrier systems.

        Args:
            systems: System objects previously loaded from the GraphQL API.
        """
        self.systems = systems
        self._manual_status_replays: dict[tuple[str, str], ManualReplay] = {}

    def carrier_system(self, serial_id: str) -> System:
        """Return the loaded system with the requested serial number.

        Args:
            serial_id: Carrier system serial number from a websocket message.

        Returns:
            The matching system object.

        Raises:
            ValueError: If no loaded system has the requested serial number.
        """
        for system in self.systems:
            if system.profile.serial == serial_id:
                return system
        raise ValueError(f"No carrier_system found for serial {serial_id}")

    async def message_handler(self, websocket_message: str) -> None:
        """Apply one raw Carrier websocket message to the matching system.

        Status messages update the raw status payload, refresh its timestamp,
        and rebuild the ``Status`` model. Config messages merge zone activity and
        program changes into the raw config payload before rebuilding ``Config``.

        Args:
            websocket_message: JSON websocket message text from Carrier realtime
                updates.
        """
        websocket_message_json = loads(websocket_message)
        message_type = websocket_message_json.pop("messageType", None)
        serial_id = websocket_message_json.pop("deviceId", None)
        _timestamp = websocket_message_json.pop("timestamp", None)
        _updated_time = websocket_message_json.pop("updatedTime", None)
        if serial_id is None:
            _LOGGER.debug(
                "Received message without deviceId, skipping messageType=%s", message_type
            )
            return
        system = self.carrier_system(serial_id=serial_id)
        match message_type:
            case "InfinityStatus":
                _LOGGER.debug("InfinityStatus received: %s", websocket_message)
                zones = websocket_message_json.pop("zones", [])
                for zone in zones:
                    _timestamp = zone.pop("timestamp", None)
                    if "id" not in zone:
                        continue
                    zone_id = str(zone["id"])
                    replay_key = (serial_id, zone_id)
                    try:
                        stale_zone = find_by_id(system.status.raw["zones"], zone["id"])
                    except ValueError:
                        continue
                    aligned = _align_manual_status_setpoints_with_config(
                        system,
                        zone,
                        self._manual_status_replays.get(replay_key),
                    )
                    _drop_non_finite_setpoints(zone)
                    always_merger.merge(stale_zone, zone)
                    self._clear_manual_replay(replay_key, zone, aligned)
                merged_status = always_merger.merge(system.status.raw, websocket_message_json)
                merged_status.update({"utcTime": datetime.now(UTC).isoformat()})
                system.status = Status(merged_status)
            case "InfinityConfig":
                _message_id = websocket_message_json.pop("id", None)
                _config_id = websocket_message_json.pop("infinitySystemConfigurationId", None)
                _LOGGER.debug("InfinityConfig received: %s", websocket_message)
                zones = websocket_message_json.pop("zones", [])
                for zone in zones:
                    _timestamp = zone.pop("timestamp", None)
                    if "id" in zone:
                        zone_id = zone["id"]
                        stale_zone = find_by_id(system.config.raw["zones"], zone_id)
                        try:
                            status_zone = find_by_id(system.status.raw["zones"], zone_id)
                        except ValueError:
                            status_zone = None
                        previous_status_set_points = (
                            _raw_set_point_pair(status_zone) if status_zone is not None else None
                        )
                        activities = zone.pop("activities", [])
                        for activity in activities:
                            _timestamp = activity.pop("timestamp", None)
                            _zone_configuration_id = activity.pop("zoneConfigurationId", None)
                            _fan_setting_id = activity.pop("fanSettingId", None)
                            stale_activity = find_by_id(stale_zone["activities"], activity["id"])
                            always_merger.merge(stale_activity, activity)
                        always_merger.merge(stale_zone, zone)
                        self._update_manual_replay_candidate(
                            replay_key=(serial_id, str(zone_id)),
                            zone=stale_zone,
                            previous_status_set_points=previous_status_set_points,
                        )
                always_merger.merge(system.config.raw, websocket_message_json)
                system.config = Config(system.config.raw)
            case _:
                _LOGGER.error("Received unknown message: %s", websocket_message)

    def _clear_manual_replay(
        self,
        replay_key: tuple[str, str],
        zone: dict[str, Any],
        aligned: bool,
    ) -> None:
        """Clear stale manual replay tracking when incoming status proves it stale.

        Args:
            replay_key: System serial and zone ID key for candidate tracking.
            zone: Incoming status zone payload.
            aligned: Whether the incoming payload was rewritten from replay state.
        """
        replay = self._manual_status_replays.get(replay_key)
        if replay is None:
            return

        stale_set_points, manual_pair = replay
        if aligned:
            return
        incoming_pair = _raw_set_point_pair(zone)
        should_clear = False
        if (
            incoming_pair == manual_pair
            or (zone.get("hold") is not None and not _is_hold_on(zone.get("hold")))
            or ("currentActivity" in zone and zone["currentActivity"] != ActivityTypes.MANUAL.value)
        ):
            should_clear = True
        elif incoming_pair is None:
            incoming_heat_set_point = _float_set_point(zone.get("htsp"))
            incoming_cool_set_point = _float_set_point(zone.get("clsp"))
            if incoming_heat_set_point is None and incoming_cool_set_point is None:
                return
            incoming_heat_disproves_replay = all(
                incoming_heat_set_point is not None
                and incoming_heat_set_point != stale_heat_set_point
                for stale_heat_set_point, _ in stale_set_points
            )
            incoming_cool_disproves_replay = all(
                incoming_cool_set_point is not None
                and incoming_cool_set_point != stale_cool_set_point
                for _, stale_cool_set_point in stale_set_points
            )
            should_clear = incoming_heat_disproves_replay or incoming_cool_disproves_replay
        elif incoming_pair not in stale_set_points:
            should_clear = True

        if should_clear:
            self._manual_status_replays.pop(replay_key, None)

    def _update_manual_replay_candidate(
        self,
        replay_key: tuple[str, str],
        zone: dict[str, Any],
        previous_status_set_points: SetPointPair | None,
    ) -> None:
        """Track the status set points that can be replayed after manual config.

        Args:
            replay_key: System serial and zone ID key for candidate tracking.
            zone: Merged raw config zone payload.
            previous_status_set_points: Status set points before the config merge.
        """
        manual_set_points: SetPointPair | None = None
        if _is_hold_on(zone.get("hold")) and zone.get("holdActivity") == ActivityTypes.MANUAL.value:
            manual_activity = next(
                (
                    activity
                    for activity in zone.get("activities", [])
                    if str(activity.get("type")) == ActivityTypes.MANUAL.value
                ),
                None,
            )
            if manual_activity is not None:
                manual_heat_set_point = _float_set_point(manual_activity.get("htsp"))
                manual_cool_set_point = _float_set_point(manual_activity.get("clsp"))
                if manual_heat_set_point is not None and manual_cool_set_point is not None:
                    manual_set_points = (manual_heat_set_point, manual_cool_set_point)

        if (
            manual_set_points is None
            or previous_status_set_points is None
            or previous_status_set_points == manual_set_points
        ):
            self._manual_status_replays.pop(replay_key, None)
            return

        stale_set_points = []
        existing_replay = self._manual_status_replays.get(replay_key)
        if existing_replay is not None:
            stale_set_points.extend(existing_replay[0])
        stale_set_points.append(previous_status_set_points)

        self._manual_status_replays[replay_key] = (
            stale_set_points,
            manual_set_points,
        )
