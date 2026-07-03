from django.http import HttpResponseRedirect
# views.py
from django.http import JsonResponse
from django.db import connections

def latest_devices(request):
    query = """
    SELECT *
    FROM (
        SELECT DISTINCT ON (json_v->>'Device_ID')
            json_v->>'Device_ID' AS device_id,
            to_timestamp(ts/1000.0) AT TIME ZONE 'Asia/Kolkata' AS tb_received_time_ist,
            to_timestamp((json_v->>'timestamp')::bigint) AT TIME ZONE 'Asia/Kolkata' AS device_time_ist,
            (json_v->>'battery_Volt')::numeric AS battery_volt,
            (json_v->>'rssi')::int AS rssi,
            (json_v->'water_level'->>'water_lev_mt')::numeric AS water_level_mt
        FROM ts_kv
        WHERE entity_id='d4b176a0-3d64-11f1-ba47-977177853c99'
          AND json_v->>'Device_ID' IS NOT NULL
        ORDER BY json_v->>'Device_ID', ts DESC
    ) t
    ORDER BY tb_received_time_ist DESC;
    """

    with connections['thingsboard'].cursor() as cursor:
        cursor.execute(query)

        columns = [col[0] for col in cursor.description]

        rows = [
            dict(zip(columns, row))
            for row in cursor.fetchall()
        ]

    return JsonResponse(rows, safe=False)

def thingsboard_redirect(request):
    return HttpResponseRedirect("https://watershed.mahamaps.com")