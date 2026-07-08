from datetime import datetime,UTC
from zoneinfo import ZoneInfo

from django.db import transaction

from ingestion.models import DeviceMetadata, DeviceStatus


IST = ZoneInfo("Asia/Kolkata")


def packet_timestamp(packet):
    """
    Returns packet timestamp as UTC datetime.

    Packet contains Unix epoch seconds.
    Example:
    {
        "timestamp":1783499401,
        ...
    }
    """

    ts = packet.get("timestamp")

    if not ts:
        return None

    return datetime.fromtimestamp(
        ts,
        tz=UTC
    )


def packet_timestamp_ist(packet):
    """
    Same timestamp converted to Asia/Kolkata.
    Useful for display/logging.
    """

    dt = packet_timestamp(packet)

    if dt is None:
        return None

    return dt.astimezone(IST)


@transaction.atomic
def update_device_status(packet):
    """
    Updates one device from one telemetry packet.
    """

    device_name = packet.get("Device_ID")

    if not device_name:
        return False

    device_id = device_name.replace(
        "SAMBHAV_",
        ""
    )

    try:

        meta = DeviceMetadata.objects.get(
            device_id=device_id
        )

    except DeviceMetadata.DoesNotExist:

        print(f"Unknown device : {device_id}")

        return False

    last_seen = packet_timestamp(packet)

    DeviceStatus.objects.update_or_create(

        device=meta,

        defaults={

            "last_seen": last_seen,

            "battery": packet.get("battery_Volt"),

            "rssi": packet.get("rssi"),

        }

    )

    return True