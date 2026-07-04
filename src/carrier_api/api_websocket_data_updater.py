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


def _parse_websocket_timestamp(value: Any) -> datetime | None:
    """Parse an ISO-8601 websocket timestamp into an aware datetime."""
    if not isinstance(value, str):
        return None

    timestamp = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(timestamp)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        return None

    return parsed


def _normalize_hold_value(value: Any) -> Any:
    """Normalize Carrier hold values before they are merged into raw payloads."""
    if isinstance(value, bool):
        return "on" if value else "off"
    if isinstance(value, int):
        if value == 1:
            return "on"
        if value == 0:
            return "off"
    return value


def _normalize_zone_hold(zone: dict[str, Any]) -> None:
    """Normalize accepted hold values in a zone payload."""
    if "hold" in zone:
        zone["hold"] = _normalize_hold_value(zone["hold"])


def _is_manual_config_hold(zone: dict[str, Any]) -> bool:
    """Return whether a config payload represents manual hold."""
    if zone.get("holdActivity") != ActivityTypes.MANUAL.value:
        return False
    if "hold" not in zone:
        return True
    return zone.get("hold") in ("on", True, 1)


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


def _align_partial_manual_status_setpoints(
    zone: dict[str, Any],
    stale_set_points: list[SetPointPair],
    manual_set_points: SetPointPair,
    incoming_heat_set_point: float | None,
    incoming_cool_set_point: float | None,
    raw_status_zone: dict[str, Any],
    incoming_heat_is_valid: bool,
    incoming_cool_is_valid: bool,
    incoming_heat_is_stale: bool,
    incoming_cool_is_stale: bool,
) -> bool:
    """Align partial manual status set point payloads.

    Args:
        zone: Incoming status zone payload to mutate when values are stale/missing.
        stale_set_points: Candidate stale status pair(s) from prior replay state.
        manual_set_points: Active manual config pair to recover from.
        incoming_heat_set_point: Parsed incoming heat value when possible.
        incoming_cool_set_point: Parsed incoming cool value when possible.
        raw_status_zone: Current raw status zone before merge.
        incoming_heat_is_valid: Whether incoming heat value is present and valid.
        incoming_cool_is_valid: Whether incoming cool value is present and valid.
        incoming_heat_is_stale: Whether incoming heat matches a stale pair value.
        incoming_cool_is_stale: Whether incoming cool matches a stale pair value.

    Returns:
        ``True`` when the zone payload was modified.
    """
    did_align = False

    if not incoming_heat_is_valid and not incoming_cool_is_valid:
        incoming_heat_is_present = "htsp" in zone
        incoming_cool_is_present = "clsp" in zone
        if incoming_heat_is_present and incoming_cool_is_present:
            return False
        if incoming_heat_is_present:
            zone["clsp"] = stale_set_points[0][1]
            did_align = True
        elif incoming_cool_is_present:
            zone["htsp"] = stale_set_points[0][0]
            did_align = True
        else:
            zone["htsp"] = manual_set_points[0]
            zone["clsp"] = manual_set_points[1]
            did_align = True

    if incoming_heat_is_valid:
        use_manual_heat = incoming_heat_is_stale
        if incoming_cool_set_point is None and "clsp" in zone:
            use_manual_heat = False
        updated_heat_set_point = (
            manual_set_points[0] if use_manual_heat else incoming_heat_set_point
        )
        if zone.get("htsp") != updated_heat_set_point:
            zone["htsp"] = updated_heat_set_point
            did_align = True

    if incoming_cool_is_valid:
        use_manual_cool = incoming_cool_is_stale
        if incoming_heat_set_point is None and "htsp" in zone:
            use_manual_cool = False
        updated_cool_set_point = (
            manual_set_points[1] if use_manual_cool else incoming_cool_set_point
        )
        if zone.get("clsp") != updated_cool_set_point:
            zone["clsp"] = updated_cool_set_point
            did_align = True

    if not incoming_heat_is_valid:
        raw_heat_set_point = _float_set_point(raw_status_zone.get("htsp"))
        raw_heat_is_stale = any(
            raw_heat_set_point == stale_heat_set_point
            for stale_heat_set_point, _ in stale_set_points
        )
        if raw_heat_is_stale and zone.get("htsp") != manual_set_points[0]:
            zone["htsp"] = manual_set_points[0]
            did_align = True

    if not incoming_cool_is_valid:
        raw_cool_set_point = _float_set_point(raw_status_zone.get("clsp"))
        raw_cool_is_stale = any(
            raw_cool_set_point == stale_cool_set_point
            for _, stale_cool_set_point in stale_set_points
        )
        if raw_cool_is_stale and zone.get("clsp") != manual_set_points[1]:
            zone["clsp"] = manual_set_points[1]
            did_align = True

    return did_align


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
    if incoming_hold is not None and incoming_hold not in ("on", True, 1):
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
    config_is_manual = _is_manual_config_hold(config_zone)
    if not status_is_manual and not config_is_manual:
        return False

    incoming_pair = _raw_set_point_pair(zone)
    incoming_manual_signal = _activity_type(
        zone.get("currentActivity")
    ) is ActivityTypes.MANUAL or zone.get("hold") in ("on", True, 1)

    if incoming_pair is not None:
        if incoming_pair == manual_set_points:
            return False
        if incoming_pair in stale_set_points:
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

        incoming_heat_is_stale = any(
            incoming_heat_set_point == stale_heat_set_point
            for stale_heat_set_point, _ in stale_set_points
        )
        incoming_cool_is_stale = any(
            incoming_cool_set_point == stale_cool_set_point
            for _, stale_cool_set_point in stale_set_points
        )
        incoming_heat_is_manual = incoming_heat_set_point == manual_set_points[0]
        incoming_cool_is_manual = incoming_cool_set_point == manual_set_points[1]
        if incoming_heat_is_manual == incoming_cool_is_manual or (
            incoming_heat_is_stale == incoming_cool_is_stale
        ):
            return False

        zone["htsp"] = manual_set_points[0] if incoming_heat_is_stale else incoming_heat_set_point
        zone["clsp"] = manual_set_points[1] if incoming_cool_is_stale else incoming_cool_set_point
        if zone["htsp"] == incoming_heat_set_point and zone["clsp"] == incoming_cool_set_point:
            return False
        _LOGGER.debug(
            "Replacing stale manual status set points for zone %s: raw=%s/%s, local=%s/%s",
            zone["id"],
            incoming_heat_set_point,
            incoming_cool_set_point,
            zone["htsp"],
            zone["clsp"],
        )
        return True

    raw_status_pair = _raw_set_point_pair(raw_status_zone)
    if not incoming_manual_signal or raw_status_pair is None:
        return False

    incoming_heat_is_stale = any(
        incoming_heat_set_point == stale_heat_set_point
        for stale_heat_set_point, _ in stale_set_points
    )
    incoming_cool_is_stale = any(
        incoming_cool_set_point == stale_cool_set_point
        for _, stale_cool_set_point in stale_set_points
    )
    if raw_status_pair not in stale_set_points and not (
        incoming_heat_is_stale or incoming_cool_is_stale
    ):
        return False

    incoming_heat_is_valid = "htsp" in zone and incoming_heat_set_point is not None
    incoming_cool_is_valid = "clsp" in zone and incoming_cool_set_point is not None
    if incoming_heat_is_valid and incoming_cool_is_valid:
        return False
    did_align = _align_partial_manual_status_setpoints(
        zone=zone,
        stale_set_points=stale_set_points,
        manual_set_points=manual_set_points,
        incoming_heat_set_point=incoming_heat_set_point,
        incoming_cool_set_point=incoming_cool_set_point,
        raw_status_zone=raw_status_zone,
        incoming_heat_is_valid=incoming_heat_is_valid,
        incoming_cool_is_valid=incoming_cool_is_valid,
        incoming_heat_is_stale=incoming_heat_is_stale,
        incoming_cool_is_stale=incoming_cool_is_stale,
    )

    if not did_align:
        return False

    current_heat_set_point = zone.get("htsp")
    current_cool_set_point = zone.get("clsp")

    _LOGGER.debug(
        "Replacing stale manual status set points for zone %s: raw=%s/%s, local=%s/%s",
        zone["id"],
        incoming_heat_set_point,
        incoming_cool_set_point,
        current_heat_set_point,
        current_cool_set_point,
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
        self._last_status_timestamps: dict[tuple[str, str], datetime] = {}
        self._last_config_timestamps: dict[tuple[str, str], datetime] = {}

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
                    zone_timestamp = _parse_websocket_timestamp(zone.pop("timestamp", None))
                    if "id" not in zone:
                        continue
                    zone_id = str(zone["id"])
                    _normalize_zone_hold(zone)
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
                    if zone_timestamp is not None:
                        last_status_timestamp = self._last_status_timestamps.get(replay_key)
                        if last_status_timestamp is None or zone_timestamp > last_status_timestamp:
                            self._last_status_timestamps[replay_key] = zone_timestamp
                    self._clear_manual_replay(
                        replay_key,
                        zone,
                        aligned,
                        zone_timestamp,
                    )
                merged_status = always_merger.merge(system.status.raw, websocket_message_json)
                merged_status.update({"utcTime": datetime.now(UTC).isoformat()})
                system.status = Status(merged_status)
            case "InfinityConfig":
                _message_id = websocket_message_json.pop("id", None)
                _config_id = websocket_message_json.pop("infinitySystemConfigurationId", None)
                _LOGGER.debug("InfinityConfig received: %s", websocket_message)
                zones = websocket_message_json.pop("zones", [])
                has_stale_zone = False
                has_applied_zone = False
                for zone in zones:
                    zone_timestamp = _parse_websocket_timestamp(zone.pop("timestamp", None))
                    if "id" not in zone:
                        continue
                    zone_id = zone["id"]
                    replay_key = (serial_id, str(zone_id))
                    if self._is_stale_config_zone_timestamp(
                        replay_key=replay_key,
                        zone_timestamp=zone_timestamp,
                    ):
                        has_stale_zone = True
                        continue
                    _normalize_zone_hold(zone)
                    try:
                        stale_zone = find_by_id(system.config.raw["zones"], zone_id)
                    except ValueError:
                        continue
                    try:
                        status_zone = find_by_id(system.status.raw["zones"], zone_id)
                    except ValueError:
                        status_zone = None
                    previous_status_set_points = (
                        _raw_set_point_pair(status_zone) if status_zone is not None else None
                    )
                    activities = zone.pop("activities", [])
                    hold_activity_only = (
                        "hold" not in zone
                        and zone.get("holdActivity") == ActivityTypes.MANUAL.value
                    )
                    for activity in activities:
                        _timestamp = activity.pop("timestamp", None)
                        _zone_configuration_id = activity.pop("zoneConfigurationId", None)
                        _fan_setting_id = activity.pop("fanSettingId", None)
                        if "id" not in activity:
                            continue
                        try:
                            stale_activity = find_by_id(stale_zone["activities"], activity["id"])
                        except ValueError:
                            continue
                        always_merger.merge(stale_activity, activity)
                    always_merger.merge(stale_zone, zone)
                    self._update_manual_replay_candidate(
                        replay_key=(serial_id, str(zone_id)),
                        zone=stale_zone,
                        previous_status_set_points=previous_status_set_points,
                        allow_incoming_manual_hold_only=hold_activity_only,
                    )
                    has_applied_zone = True
                    self._update_config_watermark(
                        replay_key=replay_key,
                        zone_timestamp=zone_timestamp,
                    )
                if not (has_stale_zone and not has_applied_zone):
                    always_merger.merge(system.config.raw, websocket_message_json)
                system.config = Config(system.config.raw)
            case _:
                _LOGGER.error("Received unknown message: %s", websocket_message)

    def _is_stale_config_zone_timestamp(
        self,
        *,
        replay_key: tuple[str, str],
        zone_timestamp: datetime | None,
    ) -> bool:
        """Return ``True`` when a config frame is older than status/config high-water."""
        if zone_timestamp is None:
            return False
        last_status_timestamp = self._last_status_timestamps.get(replay_key)
        if last_status_timestamp is not None and zone_timestamp < last_status_timestamp:
            return True
        last_config_timestamp = self._last_config_timestamps.get(replay_key)
        return last_config_timestamp is not None and zone_timestamp < last_config_timestamp

    def _update_config_watermark(
        self,
        *,
        replay_key: tuple[str, str],
        zone_timestamp: datetime | None,
    ) -> None:
        """Record the newest config frame timestamp for a zone when available."""
        if zone_timestamp is None:
            return
        last_config_timestamp = self._last_config_timestamps.get(replay_key)
        if last_config_timestamp is None or zone_timestamp > last_config_timestamp:
            self._last_config_timestamps[replay_key] = zone_timestamp

    def _clear_manual_replay(
        self,
        replay_key: tuple[str, str],
        zone: dict[str, Any],
        aligned: bool,
        zone_timestamp: datetime | None,
    ) -> None:
        """Clear stale manual replay tracking when incoming status proves it stale.

        Args:
        replay_key: System serial and zone ID key for candidate tracking.
        zone: Incoming status zone payload.
        aligned: Whether the incoming payload was rewritten from replay state.
        zone_timestamp: Timestamp parsed from the status frame when available.
        """
        replay = self._manual_status_replays.get(replay_key)
        if replay is None:
            return

        stale_set_points, manual_pair = replay
        if aligned:
            return
        incoming_pair = _raw_set_point_pair(zone)
        should_clear = False
        if incoming_pair == manual_pair or (
            zone.get("hold") is not None and zone.get("hold") not in ("on", True, 1)
        ):
            should_clear = not self._is_stale_status_zone_timestamp(
                replay_key=replay_key,
                zone_timestamp=zone_timestamp,
            )
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

    def _is_stale_status_zone_timestamp(
        self,
        *,
        replay_key: tuple[str, str],
        zone_timestamp: datetime | None,
    ) -> bool:
        """Return ``True`` when a status frame timestamp predates known watermarks."""
        if zone_timestamp is None:
            return False
        last_status_timestamp = self._last_status_timestamps.get(replay_key)
        if last_status_timestamp is not None and zone_timestamp < last_status_timestamp:
            return True
        last_config_timestamp = self._last_config_timestamps.get(replay_key)
        return last_config_timestamp is not None and zone_timestamp < last_config_timestamp

    def _update_manual_replay_candidate(
        self,
        replay_key: tuple[str, str],
        zone: dict[str, Any],
        previous_status_set_points: SetPointPair | None,
        allow_incoming_manual_hold_only: bool = False,
    ) -> None:
        """Track the status set points that can be replayed after manual config.

        Args:
            replay_key: System serial and zone ID key for candidate tracking.
            zone: Merged raw config zone payload.
            previous_status_set_points: Status set points before the config merge.
            allow_incoming_manual_hold_only: Whether the incoming config payload
                omitted ``hold`` but explicitly selected manual hold activity.
        """
        manual_set_points: SetPointPair | None = None
        is_manual_config_hold = _is_manual_config_hold(zone)
        if not is_manual_config_hold and allow_incoming_manual_hold_only:
            is_manual_config_hold = zone.get("holdActivity") == ActivityTypes.MANUAL.value
        if is_manual_config_hold:
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
            or (
                previous_status_set_points == manual_set_points
                and replay_key not in self._manual_status_replays
            )
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
