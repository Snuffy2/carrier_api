"""Apply Carrier realtime websocket messages to in-memory system models."""

from datetime import UTC, datetime
from json import loads
from logging import getLogger

from deepmerge import always_merger

from .config import Config
from .const import ActivityTypes
from .status import Status
from .system import System
from .util import safely_get_json_value

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
                    stale_zone = find_by_id(system.status.raw["zones"], zone["id"])
                    stale_heat = bool(
                        stale_zone.get(
                            "_setpointsStaleForActivityHeat",
                            stale_zone.get("_setpointsStaleForActivity", False),
                        )
                    )
                    stale_cool = bool(
                        stale_zone.get(
                            "_setpointsStaleForActivityCool",
                            stale_zone.get("_setpointsStaleForActivity", False),
                        )
                    )
                    has_heat_set_point = "htsp" in zone
                    has_cool_set_point = "clsp" in zone
                    incoming_activity_changed = "currentActivity" in zone and zone[
                        "currentActivity"
                    ] != stale_zone.get("currentActivity")
                    if has_heat_set_point or has_cool_set_point:
                        if has_heat_set_point:
                            stale_heat = False
                        elif incoming_activity_changed:
                            stale_heat = True
                        if has_cool_set_point:
                            stale_cool = False
                        elif incoming_activity_changed:
                            stale_cool = True
                        stale_zone["_setpointsStaleForActivityHeat"] = stale_heat
                        stale_zone["_setpointsStaleForActivityCool"] = stale_cool
                        stale_zone["_setpointsStaleForActivity"] = stale_heat or stale_cool
                    elif incoming_activity_changed:
                        stale_heat = True
                        stale_cool = True
                        stale_zone["_setpointsStaleForActivityHeat"] = stale_heat
                        stale_zone["_setpointsStaleForActivityCool"] = stale_cool
                        stale_zone["_setpointsStaleForActivity"] = stale_heat or stale_cool
                    always_merger.merge(stale_zone, zone)
                merged_status = always_merger.merge(system.status.raw, websocket_message_json)
                merged_status.update({"utcTime": datetime.now(UTC).isoformat()})
                system.status = Status(merged_status)
            case "InfinityConfig":
                _message_id = websocket_message_json.pop("id", None)
                _config_id = websocket_message_json.pop("infinitySystemConfigurationId", None)
                _LOGGER.debug("InfinityConfig received: %s", websocket_message)
                zones = websocket_message_json.pop("zones", [])
                status_zone_by_id = {
                    str(status_zone["id"]): status_zone
                    for status_zone in system.status.raw["zones"]
                }
                vacation_heat_target_changed = (
                    "vacmint" in websocket_message_json
                    and safely_get_json_value(websocket_message_json, "vacmint", float)
                    != safely_get_json_value(system.config.raw, "vacmint", float)
                )
                vacation_cool_target_changed = (
                    "vacmaxt" in websocket_message_json
                    and safely_get_json_value(websocket_message_json, "vacmaxt", float)
                    != safely_get_json_value(system.config.raw, "vacmaxt", float)
                )
                if vacation_heat_target_changed or vacation_cool_target_changed:
                    for status_zone in status_zone_by_id.values():
                        if status_zone.get("currentActivity") == ActivityTypes.VACATION.value:
                            if vacation_heat_target_changed:
                                status_zone["_setpointsStaleForActivityHeat"] = True
                            if vacation_cool_target_changed:
                                status_zone["_setpointsStaleForActivityCool"] = True
                            status_zone["_setpointsStaleForActivity"] = bool(
                                status_zone.get("_setpointsStaleForActivityHeat", False)
                                or status_zone.get("_setpointsStaleForActivityCool", False)
                            )
                for zone in zones:
                    _timestamp = zone.pop("timestamp", None)
                    if "id" in zone:
                        zone_id = zone["id"]
                        stale_zone = find_by_id(system.config.raw["zones"], zone_id)
                        status_zone = status_zone_by_id.get(str(zone_id))
                        activities = zone.pop("activities", [])
                        for activity in activities:
                            _timestamp = activity.pop("timestamp", None)
                            _zone_configuration_id = activity.pop("zoneConfigurationId", None)
                            _fan_setting_id = activity.pop("fanSettingId", None)
                            incoming_activity = activity.get("type")
                            stale_activity = (
                                next(
                                    (
                                        stale_activity
                                        for stale_activity in stale_zone["activities"]
                                        if stale_activity.get("type") == incoming_activity
                                    ),
                                    None,
                                )
                                if incoming_activity is not None
                                else find_by_id(stale_zone["activities"], activity["id"])
                            )
                            if stale_activity is not None:
                                activity_heat_target_changed = (
                                    "htsp" in activity
                                    and safely_get_json_value(activity, "htsp", float)
                                    != safely_get_json_value(stale_activity, "htsp", float)
                                )
                                activity_cool_target_changed = (
                                    "clsp" in activity
                                    and safely_get_json_value(activity, "clsp", float)
                                    != safely_get_json_value(stale_activity, "clsp", float)
                                )
                                incoming_activity_or_type = (
                                    incoming_activity
                                    or safely_get_json_value(stale_activity, "type")
                                )
                                if (
                                    status_zone is not None
                                    and status_zone.get("currentActivity")
                                    == incoming_activity_or_type
                                    and (
                                        activity_heat_target_changed or activity_cool_target_changed
                                    )
                                ):
                                    if activity_heat_target_changed:
                                        status_zone["_setpointsStaleForActivityHeat"] = True
                                    if activity_cool_target_changed:
                                        status_zone["_setpointsStaleForActivityCool"] = True
                                    status_zone["_setpointsStaleForActivity"] = bool(
                                        status_zone.get("_setpointsStaleForActivityHeat", False)
                                        or status_zone.get("_setpointsStaleForActivityCool", False)
                                    )
                                always_merger.merge(stale_activity, activity)
                        always_merger.merge(stale_zone, zone)
                always_merger.merge(system.config.raw, websocket_message_json)
                system.config = Config(system.config.raw)
                system.status = Status(system.status.raw)
            case _:
                _LOGGER.error("Received unknown message: %s", websocket_message)
