from datetime import datetime, UTC, timedelta

from django.db import connections

from ingestion.models import DeviceMetadata


ONLINE_TIMEOUT = timedelta(minutes=30)


def get_latest_telemetry():
    """
    Returns latest telemetry keyed by device id.
    Reads directly from ThingsBoard PostgreSQL.
    """

    query = """
    SELECT *
    FROM (

        SELECT DISTINCT ON (json_v->>'Device_ID')

            json_v->>'Device_ID' AS device_id,

            (json_v->>'timestamp')::bigint AS device_timestamp,

            to_timestamp((json_v->>'timestamp')::bigint)
                AT TIME ZONE 'Asia/Kolkata'
                AS last_posted,

            (json_v->>'battery_Volt')::numeric
                AS battery,

            (json_v->>'rssi')::int
                AS rssi

        FROM ts_kv

        WHERE entity_id='d4b176a0-3d64-11f1-ba47-977177853c99'

        ORDER BY json_v->>'Device_ID', ts DESC

    ) t;
    """

    with connections["thingsboard"].cursor() as cursor:

        cursor.execute(query)

        columns = [
            c[0]
            for c in cursor.description
        ]

        rows = [
            dict(zip(columns, row))
            for row in cursor.fetchall()
        ]

    telemetry = {}

    for row in rows:

        device_id = row["device_id"].replace(
            "SAMBHAV_",
            ""
        )

        telemetry[device_id] = row

    return telemetry


def is_online(device_timestamp):

    if device_timestamp is None:
        return False

    last_seen = datetime.fromtimestamp(
        device_timestamp,
        tz=UTC
    )

    return (
        datetime.now(UTC) - last_seen
    ) <= ONLINE_TIMEOUT


def get_all_device_status():

    telemetry = get_latest_telemetry()

    devices = []

    for meta in DeviceMetadata.objects.order_by(
        "category",
        "village"
    ):

        t = telemetry.get(
            meta.device_id,
            {}
        )

        devices.append({

            "device_id": meta.device_id,

            "device_name": meta.device_name,

            "category": meta.category,

            "village": meta.village,

            "district": meta.district,

            "battery": t.get("battery"),

            "rssi": t.get("rssi"),

            "last_posted": t.get("last_posted"),

            "online": is_online(
                t.get("device_timestamp")
            ),

        })

    return devices