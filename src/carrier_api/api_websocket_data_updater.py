"""Apply Carrier realtime websocket messages to in-memory system models."""

from datetime import UTC, datetime
from json import loads
from logging import getLogger
from typing import Any

from deepmerge import always_merger

from .config import Config
from .const import ActivityTypes
from .status import Status
from .system import System

_LOGGER = getLogger(__name__)


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


def _align_manual_status_setpoints_with_config(system: System, zone: dict[str, Any]) -> None:
    """Replace stale manual status set points with matching config values.

    Args:
        system: Loaded Carrier system whose config contains activity profiles.
        zone: Incoming status zone payload to normalize before merging.
    """
    if "id" not in zone:
        return
    zone_id = str(zone["id"])

    incoming_heat_set_point = _float_set_point(zone.get("htsp"))
    incoming_cool_set_point = _float_set_point(zone.get("clsp"))
    if incoming_heat_set_point is None or incoming_cool_set_point is None:
        return

    try:
        raw_status_zone = find_by_id(system.status.raw["zones"], zone["id"])
    except ValueError:
        return

    raw_heat_set_point = _float_set_point(raw_status_zone.get("htsp"))
    raw_cool_set_point = _float_set_point(raw_status_zone.get("clsp"))
    if raw_heat_set_point is None or raw_cool_set_point is None:
        return

    if (
        incoming_heat_set_point != raw_heat_set_point
        or incoming_cool_set_point != raw_cool_set_point
    ):
        return

    incoming_activity = zone.get("currentActivity")
    if incoming_activity is not None:
        try:
            if ActivityTypes(incoming_activity) is not ActivityTypes.MANUAL:
                return
        except ValueError:
            return
    else:
        try:
            if ActivityTypes(raw_status_zone.get("currentActivity")) is not ActivityTypes.MANUAL:
                return
        except TypeError, ValueError:
            return

    try:
        raw_config_zone = find_by_id(system.config.raw["zones"], zone["id"])
    except ValueError:
        return
    if raw_config_zone.get("hold") != "on" or raw_config_zone.get("holdActivity") != "manual":
        return
    activities = raw_config_zone.get("activities")
    if not isinstance(activities, list):
        return
    manual_activity = next(
        (
            activity
            for activity in activities
            if str(activity.get("type")) == ActivityTypes.MANUAL.value
        ),
        None,
    )
    if manual_activity is None:
        return

    manual_heat_set_point = _float_set_point(manual_activity.get("htsp"))
    manual_cool_set_point = _float_set_point(manual_activity.get("clsp"))
    if manual_heat_set_point is None or manual_cool_set_point is None:
        return
    if raw_heat_set_point == manual_heat_set_point and raw_cool_set_point == manual_cool_set_point:
        return

    zone["htsp"] = manual_heat_set_point
    zone["clsp"] = manual_cool_set_point

    _LOGGER.debug(
        "Replacing stale manual status set points for zone %s: raw=%s/%s, local=%s/%s",
        zone_id,
        incoming_heat_set_point,
        incoming_cool_set_point,
        zone["htsp"],
        zone["clsp"],
    )


def _float_set_point(value: Any) -> float | None:
    """Convert a potential set point value into a float if valid.

    Args:
        value: Raw Carrier value for a heat/cool set point.

    Returns:
        The parsed float value, or ``None`` when conversion is not possible.
    """
    try:
        return float(value)
    except TypeError, ValueError:
        return None


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
                    _align_manual_status_setpoints_with_config(system, zone)
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
                always_merger.merge(system.config.raw, websocket_message_json)
                system.config = Config(system.config.raw)
            case _:
                _LOGGER.error("Received unknown message: %s", websocket_message)
