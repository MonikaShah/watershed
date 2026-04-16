from django.shortcuts import render
from rest_framework.decorators import api_view
from rest_framework.response import Response
import os
from django.db import connection
import pandas as pd
from django.http import HttpResponse
from django.shortcuts import render
from .models import Device
from .utils import fetch_device_data

TB_URL = "http://127.0.0.1:8080"
TB_USERNAME = "thingsboard"       # e.g admin@thingsboard.org
TB_PASSWORD = "thingsboard_pass"

@api_view(['POST'])
def ingest_data(request):
    print(request.data)
    return Response({"status": "ok"})


@api_view(['GET'])
def ota(request, device_type):
    base = f"/home/monika/mysite/watershed/iot_backend/media/ota/{device_type}"
    
    if not os.path.exists(base):
        return Response({"error": "invalid type"}, status=400)

    files = sorted(os.listdir(base))
    
    if not files:
        return Response({"error": "no firmware"}, status=404)

    latest = files[-1]

    return Response({
        "url": f"https://apiwatershed.mahamaps.com/media/ota/{device_type}/{latest}"
    })
# ----------------------------
# ThingsBoard helper functions
# ----------------------------
def get_tb_token():
    url = f"{TB_URL}/api/auth/login"

    payload = {
        "username": TB_USERNAME,
        "password": TB_PASSWORD
    }

    r = requests.post(url, json=payload)
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


def get_telemetry(token, device_id, start_ts, end_ts):
    # first get available keys
    keys_url = f"{TB_URL}/api/plugins/telemetry/DEVICE/{device_id}/keys/timeseries"

    headers = {
        "X-Authorization": f"Bearer {token}"
    }

    keys_resp = requests.get(keys_url, headers=headers)
    keys_resp.raise_for_status()

    keys = keys_resp.json()

    if not keys:
        return pd.DataFrame()

    keys_str = ",".join(keys)

    data_url = (
        f"{TB_URL}/api/plugins/telemetry/DEVICE/{device_id}/values/timeseries"
        f"?keys={keys_str}"
        f"&startTs={start_ts}"
        f"&endTs={end_ts}"
        f"&limit=50000"
    )

    r = requests.get(data_url, headers=headers)
    r.raise_for_status()

    data = r.json()

    rows = []

    for key, values in data.items():
        for item in values:
            value = item["value"]

            try:
                parsed = json.loads(value)

                if isinstance(parsed, dict):
                    for subk, subv in parsed.items():
                        rows.append({
                            "time": pd.to_datetime(item["ts"], unit="ms"),
                            "key": f"{key}_{subk}",
                            "value": subv
                        })
                else:
                    rows.append({
                        "time": pd.to_datetime(item["ts"], unit="ms"),
                        "key": key,
                        "value": value
                    })
            except:
                rows.append({
                    "time": pd.to_datetime(item["ts"], unit="ms"),
                    "key": key,
                    "value": value
                })

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    pivot_df = df.pivot_table(
        index="time",
        columns="key",
        values="value",
        aggfunc="first"
    ).reset_index()

    return pivot_df


# ----------------------------
# Dashboard page
# ----------------------------
@api_view(['GET'])
def dashboard(request):
    token = get_tb_token()
    devices = get_tb_devices(token)

    table_data = []
    columns = []

    device_id = request.GET.get("device")
    from_date = request.GET.get("from_date")
    to_date = request.GET.get("to_date")

    if device_id and from_date and to_date:
        start_ts = int(pd.Timestamp(from_date).timestamp() * 1000)
        end_ts = int(pd.Timestamp(to_date).timestamp() * 1000)

        df = get_telemetry(token, device_id, start_ts, end_ts)

        if not df.empty:
            df = df.fillna("")
            columns = df.columns.tolist()
            table_data = df.to_dict(orient="records")

    return render(
        request._request,
        "ingestion/dashboard.html",
        {
            "devices": devices,
            "table_data": table_data,
            "columns": columns
        }
    )

def db_tables(request):
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        tables = [row[0] for row in cursor.fetchall()]

    return render(request, "ingestion/db_tables.html", {"tables": tables})


# ----------------------------
# View Table Data (SAFE)
# ----------------------------
def db_table_view(request, table_name):
    limit = int(request.GET.get("limit", 100))

    # whitelist tables
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        valid_tables = [row[0] for row in cursor.fetchall()]

    if table_name not in valid_tables:
        return HttpResponse("Invalid table", status=400)

    with connection.cursor() as cursor:
        cursor.execute(f'SELECT * FROM "{table_name}" LIMIT %s', [limit])
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()

    data = [dict(zip(columns, row)) for row in rows]

    return render(
        request,
        "ingestion/db_table.html",
        {
            "table_name": table_name,
            "columns": columns,
            "rows": data
        }
    )


# ----------------------------
# CSV Export
# ----------------------------
@api_view(['GET'])
def export_csv(request):
    token = get_tb_token()

    device_id = request.GET.get("device")
    from_date = request.GET.get("from_date")
    to_date = request.GET.get("to_date")

    start_ts = int(pd.Timestamp(from_date).timestamp() * 1000)
    end_ts = int(pd.Timestamp(to_date).timestamp() * 1000)

    df = get_telemetry(token, device_id, start_ts, end_ts)

    response = HttpResponse(content_type="text/csv")
    response["Content-Disposition"] = "attachment; filename=telemetry.csv"

    df.to_csv(response, index=False)

    return response