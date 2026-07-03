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
SetPointCandidates = set[SetPointPair]


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
    stale_set_points: SetPointCandidates | None,
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
    incoming_has_setpoints = (
        incoming_heat_set_point is not None or incoming_cool_set_point is not None
    )
    incoming_has_full_setpoints = (
        incoming_heat_set_point is not None and incoming_cool_set_point is not None
    )
    incoming_activity = _activity_type(zone.get("currentActivity"))
    incoming_hold = zone.get("hold")
    incoming_has_manual_indicator = (
        incoming_activity is ActivityTypes.MANUAL or incoming_hold == "on"
    )
    incoming_matches_stale_pair = (
        incoming_has_full_setpoints
        and stale_set_points is not None
        and _matches_candidate_setpoints(
            candidates=stale_set_points,
            heat_set_point=incoming_heat_set_point,
            cool_set_point=incoming_cool_set_point,
        )
    )
    incoming_is_manual_transition = (
        not incoming_has_setpoints
        and stale_set_points is not None
        and (
            incoming_activity is ActivityTypes.MANUAL
            or ("currentActivity" not in zone and zone.get("hold") == "on")
        )
    )
    if "currentActivity" in zone and incoming_activity is not ActivityTypes.MANUAL:
        return False
    if (
        incoming_has_full_setpoints
        and incoming_matches_stale_pair
        and not incoming_has_manual_indicator
    ):
        return False

    try:
        raw_status_zone = find_by_id(system.status.raw["zones"], zone["id"])
    except ValueError:
        return False

    raw_hold = raw_status_zone.get("hold")
    if incoming_hold is not None:
        if incoming_hold != "on":
            return False
    elif (
        raw_hold != "on"
        and not incoming_is_manual_transition
        and not (incoming_matches_stale_pair and incoming_activity is ActivityTypes.MANUAL)
    ):
        return False

    raw_heat_set_point = _float_set_point(raw_status_zone.get("htsp"))
    raw_cool_set_point = _float_set_point(raw_status_zone.get("clsp"))

    raw_status_activity = _activity_type(raw_status_zone.get("currentActivity"))
    if (
        not incoming_is_manual_transition
        and not _status_payload_is_manual(zone, raw_status_activity)
        and not (
            incoming_matches_stale_pair and incoming_hold == "on" and "currentActivity" not in zone
        )
    ):
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
    if not incoming_has_setpoints and not incoming_is_manual_transition:
        return False
    if incoming_has_setpoints:
        if incoming_heat_set_point is None or incoming_cool_set_point is None:
            return False
        if not _matches_candidate_setpoints(
            candidates=stale_set_points,
            heat_set_point=incoming_heat_set_point,
            cool_set_point=incoming_cool_set_point,
        ):
            return False
    elif (raw_heat_set_point, raw_cool_set_point) not in stale_set_points:
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


def _status_payload_is_manual(
    zone: dict[str, Any],
    raw_status_activity: ActivityTypes | None,
) -> bool:
    """Return whether the incoming or current status activity is manual.

    Args:
        zone: Incoming status zone payload.
        raw_status_activity: Current raw status activity before merging.

    Returns:
        ``True`` when the incoming payload or existing raw status is manual.
    """
    incoming_activity = zone.get("currentActivity")
    if incoming_activity is not None:
        return _activity_type(incoming_activity) is ActivityTypes.MANUAL
    return raw_status_activity is ActivityTypes.MANUAL


def _matches_candidate_setpoints(
    candidates: SetPointCandidates,
    heat_set_point: float | None = None,
    cool_set_point: float | None = None,
) -> bool:
    """Return whether the provided set point values match any candidate pair.

    Args:
        candidates: Candidate heat/cool pairs eligible for stale replay correction.
        heat_set_point: Optional heat set point to match.
        cool_set_point: Optional cool set point to match.

    Returns:
        ``True`` when all provided values match at least one candidate pair.
    """
    for candidate_heat, candidate_cool in candidates:
        if heat_set_point is not None and heat_set_point != candidate_heat:
            continue
        if cool_set_point is not None and cool_set_point != candidate_cool:
            continue
        return True
    return False


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
        self._manual_status_replay_candidates: dict[tuple[str, str], SetPointCandidates] = {}
        self._pre_setpoint_status_set_points: dict[tuple[str, str], SetPointCandidates] = {}
        self._pre_malformed_status_set_points: dict[tuple[str, str], SetPointCandidates] = {}

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

    def _clear_replay_state(self, replay_key: tuple[str, str]) -> None:
        """Clear all stale manual replay tracking for a zone.

        Args:
            replay_key: System serial and zone ID key for candidate tracking.
        """
        self._manual_status_replay_candidates.pop(replay_key, None)
        self._pre_setpoint_status_set_points.pop(replay_key, None)
        self._pre_malformed_status_set_points.pop(replay_key, None)

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
                    incoming_heat_set_point = _float_set_point(zone.get("htsp"))
                    incoming_cool_set_point = _float_set_point(zone.get("clsp"))
                    incoming_had_full_setpoints = (
                        incoming_heat_set_point is not None and incoming_cool_set_point is not None
                    )
                    stale_zone = find_by_id(system.status.raw["zones"], zone["id"])
                    previous_status_set_points = _raw_set_point_pair(stale_zone)
                    incoming_setpoint_keys = {"htsp", "clsp"} & zone.keys()
                    incoming_valid_setpoint_count = sum(
                        set_point is not None
                        for set_point in (
                            incoming_heat_set_point,
                            incoming_cool_set_point,
                        )
                    )
                    if (
                        previous_status_set_points is not None
                        and len(incoming_setpoint_keys) == 1
                        and incoming_valid_setpoint_count == 1
                    ):
                        self._pre_setpoint_status_set_points.setdefault(replay_key, set()).add(
                            previous_status_set_points
                        )
                    elif (
                        previous_status_set_points is not None
                        and len(incoming_setpoint_keys) == 2
                        and incoming_valid_setpoint_count == 1
                    ):
                        self._pre_malformed_status_set_points.setdefault(replay_key, set()).add(
                            previous_status_set_points
                        )
                    elif incoming_had_full_setpoints or (
                        zone.get("hold") not in (None, "on")
                        or (
                            "currentActivity" in zone
                            and zone["currentActivity"] != ActivityTypes.MANUAL.value
                        )
                    ):
                        self._pre_setpoint_status_set_points.pop(replay_key, None)
                        self._pre_malformed_status_set_points.pop(replay_key, None)
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
                        previous_manual_set_points = self._manual_config_set_points_from_zone(
                            stale_zone
                        )
                        previous_manual_hold = (
                            stale_zone.get("hold") == "on"
                            and stale_zone.get("holdActivity") == ActivityTypes.MANUAL.value
                        )
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
                            previous_manual_set_points=previous_manual_set_points,
                            previous_manual_hold=previous_manual_hold,
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
        candidates = self._manual_status_replay_candidates.get(replay_key)
        if not candidates:
            return

        if zone.get("hold") not in (None, "on"):
            self._manual_status_replay_candidates.pop(replay_key, None)
            return

        if "currentActivity" in zone and zone["currentActivity"] != ActivityTypes.MANUAL.value:
            self._manual_status_replay_candidates.pop(replay_key, None)
            return

        config_set_points = self._manual_config_set_points(replay_key=replay_key)
        incoming_heat_set_point = _float_set_point(zone.get("htsp"))
        incoming_cool_set_point = _float_set_point(zone.get("clsp"))
        if config_set_points[0] is None or config_set_points[1] is None:
            return
        if ("htsp" in zone and incoming_heat_set_point is None) or (
            "clsp" in zone and incoming_cool_set_point is None
        ):
            return

        if "htsp" in zone and "clsp" in zone:
            incoming_pair = (incoming_heat_set_point, incoming_cool_set_point)
            if incoming_pair == config_set_points:
                self._manual_status_replay_candidates.pop(replay_key, None)
                return
            if incoming_pair not in candidates:
                self._manual_status_replay_candidates.pop(replay_key, None)
            return

        if "htsp" in zone:
            if incoming_heat_set_point is None:
                return
            if (
                not _matches_candidate_setpoints(
                    candidates,
                    heat_set_point=incoming_heat_set_point,
                )
                and incoming_heat_set_point != config_set_points[0]
            ):
                self._manual_status_replay_candidates.pop(replay_key, None)
                return

        if "clsp" in zone:
            if incoming_cool_set_point is None:
                return
            if (
                not _matches_candidate_setpoints(
                    candidates,
                    cool_set_point=incoming_cool_set_point,
                )
                and incoming_cool_set_point != config_set_points[1]
            ):
                self._manual_status_replay_candidates.pop(replay_key, None)
                return

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

        return self._manual_config_set_points_from_zone(raw_config_zone)

    def _manual_config_set_points_from_zone(self, zone: dict[str, Any]) -> MaybeSetPointPair:
        """Return manual config heat/cool set points from a raw config zone.

        Args:
            zone: Raw config zone payload.

        Returns:
            Manual config heat and cool set points, or ``(None, None)`` when
            unavailable.
        """
        manual_activity = next(
            (
                activity
                for activity in zone.get("activities", [])
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
        previous_manual_set_points: MaybeSetPointPair,
        previous_manual_hold: bool,
    ) -> None:
        """Track the status set points that can be replayed after manual config.

        Args:
            replay_key: System serial and zone ID key for candidate tracking.
            system: Loaded Carrier system whose config contains activity profiles.
            zone: Merged raw config zone payload.
            previous_manual_set_points: Manual set points before the config merge.
            previous_manual_hold: Whether the zone was in manual hold before the merge.
        """
        if zone.get("hold") != "on" or zone.get("holdActivity") != ActivityTypes.MANUAL.value:
            self._clear_replay_state(replay_key)
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
            self._clear_replay_state(replay_key)
            return

        manual_heat_set_point = _float_set_point(manual_activity.get("htsp"))
        manual_cool_set_point = _float_set_point(manual_activity.get("clsp"))
        if manual_heat_set_point is None or manual_cool_set_point is None:
            return
        if previous_manual_hold and previous_manual_set_points == (
            manual_heat_set_point,
            manual_cool_set_point,
        ):
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

        candidates = {
            (status_heat_set_point, status_cool_set_point),
        }
        pre_setpoint_status_set_points = self._pre_setpoint_status_set_points.get(replay_key)
        if pre_setpoint_status_set_points is not None and (
            status_heat_set_point == manual_heat_set_point
            or status_cool_set_point == manual_cool_set_point
        ):
            candidates.update(pre_setpoint_status_set_points)
        pre_malformed_status_set_points = self._pre_malformed_status_set_points.get(replay_key)
        if pre_malformed_status_set_points is not None:
            candidates.update(pre_malformed_status_set_points)
        candidates.update(self._manual_status_replay_candidates.get(replay_key, set()))
        candidates.discard((manual_heat_set_point, manual_cool_set_point))
        if not candidates:
            self._manual_status_replay_candidates.pop(replay_key, None)
            return

        if (
            status_heat_set_point == manual_heat_set_point
            and status_cool_set_point == manual_cool_set_point
        ):
            self._manual_status_replay_candidates.pop(replay_key, None)
            return

        self._manual_status_replay_candidates[replay_key] = candidates
