from datetime import datetime, timedelta

from django.utils import timezone

from ingestion.models import DeviceMetadata, DeviceStatus
from ingestion.event_logger import log_event
# from ingestion.services import push_log_ws

def process_device(device_json):
    device_name = device_json.get("Device_ID")

    if not device_name:
        return

    clean_device = device_name.replace("SAMBHAV_", "")

    try:
        meta = DeviceMetadata.objects.get(
            device_id=clean_device
        )
    except DeviceMetadata.DoesNotExist:
        return
    
    ts = device_json.get("timestamp")

    if not ts:
        return

    last_seen = timezone.make_aware(
        datetime.fromtimestamp(ts)
    )

    now = timezone.now()

    online = (now - last_seen) <= timedelta(minutes=30)

    old_status = DeviceStatus.objects.filter(
        device=meta
    ).first()

    old_online = None

    if old_status:
        old_online = old_status.online

    DeviceStatus.objects.update_or_create(
        device=meta,
        defaults={
            "last_seen": last_seen,
            "online": online,
            "battery": device_json.get("battery_Volt"),
            "rssi": device_json.get("rssi"),
        }
    )
    log_event(
        clean_device,
        "DATA",
        "Telemetry received"
    )

    push_log_ws({
        "time": now.strftime("%H:%M:%S"),
        "device": clean_device,
        "event": "DATA",
        "message": "Telemetry received"
    })
    if old_online is None:

        log_event(
            clean_device,
            "STATUS",
            f"Device is {'ONLINE' if online else 'OFFLINE'}"
        )

        push_log_ws({
            "time": now.strftime("%H:%M:%S"),
            "device": clean_device,
            "event": "STATUS",
            "message": f"Device is {'ONLINE' if online else 'OFFLINE'}"
        })

    elif old_online != online:

        log_event(
            clean_device,
            "STATUS",
            f"Device became {'ONLINE' if online else 'OFFLINE'}"
        )

        push_log_ws({
            "time": now.strftime("%H:%M:%S"),
            "device": clean_device,
            "event": "STATUS",
            "message": f"Device became {'ONLINE' if online else 'OFFLINE'}"
        })

        battery = device_json.get("battery_Volt")

    if battery is not None and battery < 3.3:

        log_event(
            clean_device,
            "ERROR",
            f"Battery Low ({battery}V)"
        )

        push_log_ws({
            "time": now.strftime("%H:%M:%S"),
            "device": clean_device,
            "event": "ERROR",
            "message": f"Battery Low ({battery}V)"
        })

    rssi = device_json.get("rssi")

    if rssi is not None and rssi < -110:

        log_event(
            clean_device,
            "ERROR",
            f"Weak Signal ({rssi})"
        )

        push_log_ws({
            "time": now.strftime("%H:%M:%S"),
            "device": clean_device,
            "event": "ERROR",
            "message": f"Weak Signal ({rssi})"
        })