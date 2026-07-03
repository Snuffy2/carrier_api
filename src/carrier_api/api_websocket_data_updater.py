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
MaybeSetPointPair = tuple[float | None, float | None]


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
    stale_set_points: SetPointPair | None,
) -> bool:
    """Replace stale manual status set points with matching config values.

    Args:
        system: Loaded Carrier system whose config contains activity profiles.
        zone: Incoming status zone payload to normalize before merging.
        stale_set_points: Status set points observed before the manual config update.

    Returns:
        ``True`` when the incoming payload was aligned with config set points.
    """
    if "id" not in zone:
        return False
    zone_id = str(zone["id"])
    incoming_heat_set_point = _float_set_point(zone.get("htsp"))
    incoming_cool_set_point = _float_set_point(zone.get("clsp"))
    incoming_has_setpoints = "htsp" in zone or "clsp" in zone

    try:
        raw_status_zone = find_by_id(system.status.raw["zones"], zone["id"])
    except ValueError:
        return False

    incoming_hold = zone.get("hold")
    raw_hold = raw_status_zone.get("hold")
    if incoming_hold is not None:
        if incoming_hold != "on":
            return False
    elif raw_hold != "on":
        return False

    raw_heat_set_point = _float_set_point(raw_status_zone.get("htsp"))
    raw_cool_set_point = _float_set_point(raw_status_zone.get("clsp"))

    raw_activity = raw_status_zone.get("currentActivity")
    try:
        raw_status_activity = ActivityTypes(raw_activity)
    except TypeError, ValueError:
        raw_status_activity = None

    incoming_activity = zone.get("currentActivity")
    if incoming_activity is not None:
        try:
            if ActivityTypes(incoming_activity) is not ActivityTypes.MANUAL:
                return False
        except TypeError, ValueError:
            return False
    elif raw_status_activity is not ActivityTypes.MANUAL:
        return False

    if (
        not incoming_has_setpoints
        and raw_status_activity is ActivityTypes.MANUAL
        and raw_hold == "on"
        and stale_set_points is None
    ):
        return False

    try:
        raw_config_zone = find_by_id(system.config.raw["zones"], zone["id"])
    except ValueError:
        return False
    if raw_config_zone.get("hold") != "on" or raw_config_zone.get("holdActivity") != "manual":
        return False

    activities = raw_config_zone.get("activities")
    if not isinstance(activities, list):
        return False

    manual_activity = next(
        (
            activity
            for activity in activities
            if str(activity.get("type")) == ActivityTypes.MANUAL.value
        ),
        None,
    )
    if manual_activity is None:
        return False

    manual_heat_set_point = _float_set_point(manual_activity.get("htsp"))
    manual_cool_set_point = _float_set_point(manual_activity.get("clsp"))
    if manual_heat_set_point is None or manual_cool_set_point is None:
        return False
    if raw_heat_set_point is None or raw_cool_set_point is None:
        return False
    if stale_set_points is None:
        return False
    if incoming_has_setpoints:
        if "htsp" in zone and incoming_heat_set_point != stale_set_points[0]:
            return False
        if "clsp" in zone and incoming_cool_set_point != stale_set_points[1]:
            return False
    elif (raw_heat_set_point, raw_cool_set_point) != stale_set_points:
        return False
    if (
        not incoming_has_setpoints
        and raw_heat_set_point == manual_heat_set_point
        and raw_cool_set_point == manual_cool_set_point
    ):
        return False

    zone["htsp"] = manual_heat_set_point
    zone["clsp"] = manual_cool_set_point

    if incoming_has_setpoints:
        _LOGGER.debug(
            "Replacing stale manual status set points for zone %s: raw=%s/%s, local=%s/%s",
            zone_id,
            incoming_heat_set_point,
            incoming_cool_set_point,
            zone["htsp"],
            zone["clsp"],
        )
    else:
        _LOGGER.debug(
            "Replacing stale manual status set points for zone %s without explicit set points: "
            "raw=%s/%s, local=%s/%s",
            zone_id,
            raw_heat_set_point,
            raw_cool_set_point,
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
    try:
        parsed = float(value)
    except TypeError, ValueError:
        return None

    if not isfinite(parsed):
        return None

    return parsed


def _drop_non_finite_setpoints(zone: dict[str, Any]) -> None:
    """Remove non-finite status set points before merging raw payloads.

    Args:
        zone: Incoming status zone payload.
    """
    for key in ("htsp", "clsp"):
        if key not in zone:
            continue
        try:
            parsed = float(zone[key])
        except TypeError, ValueError:
            continue
        if not isfinite(parsed):
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
        self._manual_status_replay_candidates: dict[tuple[str, str], SetPointPair] = {}

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
        if system is None:
            return
        match message_type:
            case "InfinityStatus":
                _LOGGER.debug("InfinityStatus received: %s", websocket_message)
                zones = websocket_message_json.pop("zones", [])
                for zone in zones:
                    _timestamp = zone.pop("timestamp", None)
                    zone_id = str(zone["id"])
                    replay_key = (serial_id, zone_id)
                    aligned = _align_manual_status_setpoints_with_config(
                        system,
                        zone,
                        self._manual_status_replay_candidates.get(replay_key),
                    )
                    if not aligned:
                        self._clear_manual_replay_candidate(
                            replay_key=replay_key,
                            zone=zone,
                        )
                    _drop_non_finite_setpoints(zone)
                    stale_zone = find_by_id(system.status.raw["zones"], zone["id"])
                    always_merger.merge(stale_zone, zone)
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
                        activities = zone.pop("activities", [])
                        for activity in activities:
                            _timestamp = activity.pop("timestamp", None)
                            _zone_configuration_id = activity.pop("zoneConfigurationId", None)
                            _fan_setting_id = activity.pop("fanSettingId", None)
                            stale_activity = find_by_id(stale_zone["activities"], activity["id"])
                            if stale_activity is not None:
                                always_merger.merge(stale_activity, activity)
                        always_merger.merge(stale_zone, zone)
                        self._update_manual_replay_candidate(
                            replay_key=(serial_id, str(zone_id)),
                            system=system,
                            zone=stale_zone,
                        )
                always_merger.merge(system.config.raw, websocket_message_json)
                system.config = Config(system.config.raw)
            case _:
                _LOGGER.error("Received unknown message: %s", websocket_message)

    def _clear_manual_replay_candidate(
        self,
        replay_key: tuple[str, str],
        zone: dict[str, Any],
    ) -> None:
        """Clear stale manual replay tracking when incoming status proves it stale.

        Args:
            replay_key: System serial and zone ID key for candidate tracking.
            zone: Incoming status zone payload.
        """
        candidate = self._manual_status_replay_candidates.get(replay_key)
        if candidate is None:
            return

        if zone.get("hold") not in (None, "on"):
            self._manual_status_replay_candidates.pop(replay_key, None)
            return

        if "currentActivity" in zone and zone["currentActivity"] != ActivityTypes.MANUAL.value:
            self._manual_status_replay_candidates.pop(replay_key, None)
            return

        config_set_points = self._manual_config_set_points(replay_key=replay_key)

        if "htsp" in zone:
            incoming_heat_set_point = _float_set_point(zone.get("htsp"))
            if incoming_heat_set_point is None:
                self._manual_status_replay_candidates.pop(replay_key, None)
                return
            if incoming_heat_set_point not in (candidate[0], config_set_points[0]):
                self._manual_status_replay_candidates.pop(replay_key, None)
                return

        if "clsp" in zone:
            incoming_cool_set_point = _float_set_point(zone.get("clsp"))
            if incoming_cool_set_point is None:
                self._manual_status_replay_candidates.pop(replay_key, None)
                return
            if incoming_cool_set_point not in (candidate[1], config_set_points[1]):
                self._manual_status_replay_candidates.pop(replay_key, None)

    def _manual_config_set_points(
        self,
        replay_key: tuple[str, str],
    ) -> MaybeSetPointPair:
        """Return the config set points paired with a stale replay candidate.

        Args:
            replay_key: System serial and zone ID key for candidate tracking.

        Returns:
            Manual config heat and cool set points, or ``(None, None)`` when
            config is unavailable.
        """
        serial_id, zone_id = replay_key
        try:
            system = self.carrier_system(serial_id=serial_id)
            raw_config_zone = find_by_id(system.config.raw["zones"], zone_id)
        except ValueError:
            return (None, None)

        manual_activity = next(
            (
                activity
                for activity in raw_config_zone.get("activities", [])
                if str(activity.get("type")) == ActivityTypes.MANUAL.value
            ),
            None,
        )
        if manual_activity is None:
            return (None, None)

        return (
            _float_set_point(manual_activity.get("htsp")),
            _float_set_point(manual_activity.get("clsp")),
        )

    def _update_manual_replay_candidate(
        self,
        replay_key: tuple[str, str],
        system: System,
        zone: dict[str, Any],
    ) -> None:
        """Track the status set points that can be replayed after manual config.

        Args:
            replay_key: System serial and zone ID key for candidate tracking.
            system: Loaded Carrier system whose config contains activity profiles.
            zone: Merged raw config zone payload.
        """
        if zone.get("hold") != "on" or zone.get("holdActivity") != ActivityTypes.MANUAL.value:
            self._manual_status_replay_candidates.pop(replay_key, None)
            return

        manual_activity = next(
            (
                activity
                for activity in zone.get("activities", [])
                if str(activity.get("type")) == ActivityTypes.MANUAL.value
            ),
            None,
        )
        if manual_activity is None:
            self._manual_status_replay_candidates.pop(replay_key, None)
            return

        manual_heat_set_point = _float_set_point(manual_activity.get("htsp"))
        manual_cool_set_point = _float_set_point(manual_activity.get("clsp"))
        if manual_heat_set_point is None or manual_cool_set_point is None:
            self._manual_status_replay_candidates.pop(replay_key, None)
            return

        try:
            raw_status_zone = find_by_id(system.status.raw["zones"], zone["id"])
        except ValueError:
            self._manual_status_replay_candidates.pop(replay_key, None)
            return

        status_heat_set_point = _float_set_point(raw_status_zone.get("htsp"))
        status_cool_set_point = _float_set_point(raw_status_zone.get("clsp"))
        if status_heat_set_point is None or status_cool_set_point is None:
            self._manual_status_replay_candidates.pop(replay_key, None)
            return

        if (
            status_heat_set_point == manual_heat_set_point
            and status_cool_set_point == manual_cool_set_point
        ):
            self._manual_status_replay_candidates.pop(replay_key, None)
            return

        self._manual_status_replay_candidates[replay_key] = (
            status_heat_set_point,
            status_cool_set_point,
        )
