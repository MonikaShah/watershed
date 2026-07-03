import json
import time
import token
import requests
from datetime import timedelta,datetime

from ingestion.services import push_log_ws
from .event_logger import log_event
from django.conf import settings
from django.utils import timezone


from .models import DeviceMetadata, DeviceStatus



TB_URL = settings.TB_URL
TB_USERNAME = settings.TB_USERNAME
TB_PASSWORD = settings.TB_PASSWORD


def get_tb_token():

    url = f"{TB_URL}/api/auth/login"

    r = requests.post(
        url,
        json={
            "username": TB_USERNAME,
            "password": TB_PASSWORD,
        },
        timeout=10,
    )

    r.raise_for_status()

    return r.json()["token"]


def get_tb_devices(token):

    url = f"{TB_URL}/api/tenant/deviceInfos?pageSize=100&page=0"

    headers = {
        "X-Authorization": f"Bearer {token}"
    }

    r = requests.get(url, headers=headers)

    r.raise_for_status()

    return r.json()["data"]

headers = {
    "X-Authorization": f"Bearer {token}"
}

print("🔥 update_device_status STARTED")

def update_device_status():
    print("Connecting to ThingsBoard...")

    token = get_tb_token()
    devices = get_tb_devices(token)

    # find gateway device
    gateway = None
    for d in devices:
        if d["name"].upper() == "SAMBHAV_DEVICES":
            gateway = d["id"]["id"]
            break

    if not gateway:
        print("Gateway not found")
        return

    print("Gateway ID:", gateway)

    headers = {
        "X-Authorization": f"Bearer {token}"
    }

    # =========================================================
    # FETCH ONLY LATEST TELEMETRY FROM GATEWAY
    # =========================================================
    url = (
        f"{TB_URL}/api/plugins/telemetry/DEVICE/"
        f"{gateway}/values/timeseries"
    )

    r = requests.get(url, headers=headers, timeout=15)
    r.raise_for_status()

    data = r.json()

    print("Keys received:", len(data))

    now = timezone.now()

    # =========================================================
    # LOOP EACH DEVICE INSIDE NESTED JSON
    # =========================================================
    for tb_key, records in data.items():

        print("Processing:", tb_key)

        if not records:
            continue

        latest = records[0]

        try:
            payload = json.loads(latest["value"])
        except Exception as e:
            print("Bad JSON:", tb_key, e)
            continue

        device_id = payload.get("Device_ID", tb_key)
        clean_id = device_id.replace("SAMBHAV_", "").strip()

        ts = payload.get("timestamp")
        if not ts:
            continue

        try:
            meta = DeviceMetadata.objects.get(device_id=clean_id)
        except DeviceMetadata.DoesNotExist:
            print("Metadata missing:", clean_id)
            continue

        last_seen = timezone.make_aware(datetime.fromtimestamp(ts))
        diff = now - last_seen

        online = diff <= timedelta(minutes=30)
        status_text = "ONLINE" if online else "OFFLINE"

        # =========================================================
        # GET EXISTING STATUS
        # =========================================================
        status_obj, created = DeviceStatus.objects.get_or_create(
            device=meta,
            defaults={
                "last_seen": last_seen,
                "online": online,
            }
        )

        # =========================================================
        # CHECK CHANGE ONLY
        # =========================================================
        if status_obj.online != online:

            print("STATUS CHANGE:", clean_id, status_text)

            log_event(
                clean_id,
                "STATUS",
                f"Device became {status_text}"
            )

            push_log_ws({
                "time": now.strftime("%H:%M:%S"),
                "device": clean_id,
                "event": "STATUS",
                "message": f"Device became {status_text}"
            })

        # =========================================================
        # UPDATE ALWAYS
        # =========================================================
        status_obj.online = online
        status_obj.last_seen = last_seen
        status_obj.battery = payload.get("battery_Volt")
        status_obj.rssi = payload.get("rssi")
        status_obj.save()

    print("Finished update")