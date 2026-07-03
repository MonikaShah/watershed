from django.db import connections
import requests,json,time
from datetime import UTC, timedelta,datetime
from django.utils import timezone

from django.conf import settings
from .models import DeviceMetadata, DeviceStatus
from .models import DeviceEventLog
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from ingestion.event_processor import process_device


def push_log_ws(data):

    channel_layer = get_channel_layer()

    async_to_sync(channel_layer.group_send)(
        "logs",
        {
            "type": "send_log",
            "data": data,
        },
    )


def log_device_event(device_id, event_type, message, extra=None):

    # Save in database
    DeviceEventLog.objects.create(
        device_id=device_id,
        event_type=event_type,
        message=message,
        extra=extra or {},
    )

    # Push to websocket
    push_log_ws({
        "device_id": device_id,
        "event_type": event_type,
        "message": message,
    })



TB_URL = settings.TB_URL
TB_USERNAME = settings.TB_USERNAME
TB_PASSWORD = settings.TB_PASSWORD
def get_latest_devices():
    query = """YOUR SQL HERE"""

    with connections["thingsboard"].cursor() as cursor:
        cursor.execute(query)

        columns = [col[0] for col in cursor.description]

        return [
            dict(zip(columns, row))
            for row in cursor.fetchall()
        ]
    
# ----------------------------
# ThingsBoard helper functions
# ----------------------------
def get_tb_token():
    url = f"{TB_URL}/api/auth/login"

    payload = {
        "username": TB_USERNAME,
        "password": TB_PASSWORD
    }

    r = requests.post(url, json=payload, timeout=10)
    r.raise_for_status()

    return r.json()["token"]


def get_tb_devices(token):
    url = f"{TB_URL}/api/tenant/deviceInfos?pageSize=100&page=0"

    headers = {
        "X-Authorization": f"Bearer {token}"
    }

    r = requests.get(url, headers=headers, timeout=10)
    r.raise_for_status()

    return r.json()["data"]

def get_device_status(token, tb_device_id):

    headers = {
        "X-Authorization": f"Bearer {token}"
    }

    # Get available telemetry keys
    keys_url = (
        f"{TB_URL}/api/plugins/telemetry/DEVICE/"
        f"{tb_device_id}/keys/timeseries"
    )

    r = requests.get(keys_url, headers=headers)
    r.raise_for_status()

    keys = r.json()

    if not keys:
        return {}

    keys_str = ",".join(keys)

    end_ts = int(time.time() * 1000)
    start_ts = end_ts - (24 * 60 * 60 * 1000)   # last 24 hours

    data_url = (
        f"{TB_URL}/api/plugins/telemetry/DEVICE/"
        f"{tb_device_id}/values/timeseries"
        f"?keys={keys_str}"
        f"&startTs={start_ts}"
        f"&endTs={end_ts}"
        f"&limit=50000"
    )

    r = requests.get(data_url, headers=headers)
    r.raise_for_status()

    data = r.json()

    latest = {}

    for values in data.values():

        for item in values:

            try:

                packet = json.loads(item["value"])

                device = packet.get("Device_ID")

                if not device:
                    continue

                # Use ThingsBoard receive time
                ts = item["ts"] // 1000

                # Keep newest packet per device
                if device not in latest or ts > latest[device]:
                    latest[device] = ts

            except Exception:
                pass

    return 


def update_device_status():

    print("Connecting to ThingsBoard...")

    token = get_tb_token()

    devices = get_tb_devices(token)

    tb_device_id = None

    for d in devices:

        print(d["name"])

        if "SAMBHAV" in d["name"].upper():
            tb_device_id = d["id"]["id"]
            break

    if not tb_device_id:
        print("SAMBHAV device not found")
        return

    print("TB Device ID:", tb_device_id)

    headers = {
        "X-Authorization": f"Bearer {token}"
    }

    # -----------------------------
    # Get telemetry keys
    # -----------------------------

    keys_url = (
        f"{TB_URL}/api/plugins/telemetry/DEVICE/"
        f"{tb_device_id}/keys/timeseries"
    )

    r = requests.get(keys_url, headers=headers, timeout=20)
    r.raise_for_status()

    keys = r.json()

    # remove corrupted keys
    keys = [
        k for k in keys
        if k.startswith("SAMBHAV_")
    ]

    print("Devices found:", len(keys))

    # -----------------------------
    # Read telemetry for every key
    # -----------------------------

    now_ms = int(time.time() * 1000)

    start_ms = now_ms - (365 * 24 * 60 * 60 * 1000)

    latest = {}

    for key in keys:

        print("Reading", key)

        url = (
            f"{TB_URL}/api/plugins/telemetry/DEVICE/"
            f"{tb_device_id}/values/timeseries"
            f"?keys={key}"
            f"&startTs={start_ms}"
            f"&endTs={now_ms}"
            f"&limit=50000"
        )

        r = requests.get(url, headers=headers, timeout=60)

        r.raise_for_status()

        data = r.json()

        if key not in data:
            continue

        latest_packet = None
        latest_ts = 0

        for item in data[key]:

            try:

                packet = json.loads(item["value"])
                # print("Device timestamp :", packet.get("timestamp"))
                # print("TB timestamp     :", item["ts"] // 1000)
                # print("Current time     :", int(time.time()))
                # print("---------------------")

                # ts = packet.get("timestamp")
                # Use ThingsBoard receive time instead of device timestamp
                tb_ts = item["ts"] // 1000

                if tb_ts > latest_ts:
                    latest_ts = tb_ts
                    latest_packet = packet
                    latest_packet["_tb_ts"] = tb_ts

            except Exception:
                continue

        if latest_packet:
            latest[key] = latest_packet

    print("\nLatest devices found:", len(latest))
    for device, ts in latest.items():
        print(device, datetime.fromtimestamp(ts, tz=UTC))

    # -----------------------------
    # Save into DeviceStatus table
    # -----------------------------

    online_count = 0

    for meta in DeviceMetadata.objects.all():

        tb_name = f"SAMBHAV_{meta.device_id}"

        packet = latest.get(tb_name)

        battery = None
        rssi = None

        if packet:

            ts = packet["_tb_ts"]

            last_seen = datetime.fromtimestamp(ts, tz=UTC)

            battery = packet.get("battery_Volt")

            rssi = packet.get("rssi")

            online = (
                timezone.now() - last_seen
            ).total_seconds() < 3600

            if online:
                online_count += 1

        else:

            last_seen = None
            online = False
        old_status = DeviceStatus.objects.filter(device=meta).first()

        old_online = None
        if old_status:
            old_online = old_status.online
        DeviceStatus.objects.update_or_create(
            device=meta,
            defaults={
                "last_seen": last_seen,
                "online": online,
                "battery": battery,
                "rssi": rssi,
            }
        )
        log_device_event(
            device_id=meta.device_id,
            event_type="DATA",
            message="Telemetry received",
            extra={
                "battery": battery,
                "rssi": rssi,
            }
        )
        print(
            meta.device_id,
            "->",
            last_seen,
            online
        )

        # First time we see this device
        if old_status is None:

            log_device_event(
                device_id=meta.device_id,
                event_type="STATUS",
                message=f"Initial status: {'Online' if online else 'Offline'}",
                extra={
                    "last_seen": str(last_seen),
                    "online": online
                }
            )

        # Status changed
        elif old_online != online:

            log_device_event(
                device_id=meta.device_id,
                event_type="STATUS",
                message=f"Device became {'Online' if online else 'Offline'}",
                extra={
                    "last_seen": str(last_seen),
                    "online": online
                }
            )
        if battery is not None and battery < 11.5:

            log_device_event(
                device_id=meta.device_id,
                event_type="ERROR",
                message=f"Battery Low ({battery}V)",
                extra={"battery": battery}
            )

    print()
    print("Finished")
    print("Online:", online_count)
    print("Total:", DeviceMetadata.objects.count())