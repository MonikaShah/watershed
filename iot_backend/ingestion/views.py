from django.shortcuts import render
from rest_framework.decorators import api_view
from rest_framework.response import Response
import os,json
import requests
# from django.db import connection
import pandas as pd
from django.http import HttpResponse
from django.shortcuts import render
from .models import Device, DeviceMetadata

# from .utils import fetch_device_data

TB_URL = "https://watershed.mahamaps.com"
TB_USERNAME = "monikapatira@gmail.com"       # e.g admin@thingsboard.org
TB_PASSWORD = "Tenant@watershed#"

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


# def get_telemetry(token, device_id, start_ts, end_ts):
#     # first get available keys
#     keys_url = f"{TB_URL}/api/plugins/telemetry/DEVICE/{device_id}/keys/timeseries"

#     headers = {
#         "X-Authorization": f"Bearer {token}"
#     }

#     keys_resp = requests.get(keys_url, headers=headers)
#     keys_resp.raise_for_status()

#     keys = keys_resp.json()

#     # if not keys:
#     #     return pd.DataFrame()
#     if not keys:
#         print("⚠️ No keys found, using fallback keys")
#         keys = ["temperature", "humidity", "battery"]  # adjust based on your device

#     keys_str = ",".join(keys)

#     data_url = (
#         f"{TB_URL}/api/plugins/telemetry/DEVICE/{device_id}/values/timeseries"
#         f"?keys={keys_str}"
#         f"&startTs={start_ts}"
#         f"&endTs={end_ts}"
#         f"&limit=50000"
#     )

#     r = requests.get(data_url, headers=headers)
#     r.raise_for_status()

#     data = r.json()

#     rows = []

#     for key, values in data.items():
#         for item in values:
#             value = item["value"]

#             try:
#                 parsed = json.loads(value)

#                 if isinstance(parsed, dict):
#                     for subk, subv in parsed.items():
#                         rows.append({
#                             "time": pd.to_datetime(item["ts"], unit="ms"),
#                             "key": f"{key}_{subk}",
#                             "value": subv
#                         })
#                 else:
#                     rows.append({
#                         "time": pd.to_datetime(item["ts"], unit="ms"),
#                         "key": key,
#                         "value": value
#                     })
#             except:
#                 rows.append({
#                     "time": pd.to_datetime(item["ts"], unit="ms"),
#                     "key": key,
#                     "value": value
#                 })

#     if not rows:
#         return pd.DataFrame()

#     df = pd.DataFrame(rows)

#     pivot_df = df.pivot_table(
#         index="time",
#         columns="key",
#         values="value",
#         aggfunc="first"
#     ).reset_index()

#     print("DEVICE:", device_id)
#     print("KEYS:", keys)
#     print("START:", start_ts, "END:", end_ts)
#     # print("RAW DATA:", data)

#     print("\n====== DASHBOARD DEBUG ======")
#     print("DEVICE:", device_id)
#     print("FROM:", start_ts, "TO:", end_ts  )

#     print("DF EMPTY?", df.empty)
#     print("DF SHAPE:", df.shape)

#     if not df.empty:
#         print("COLUMNS:", df.columns.tolist())
#         print(df.head())
#     else:
#         print("⚠️ DataFrame is EMPTY")
#     return pivot_df
# =========================================
# FLATTEN NESTED JSON
# =========================================

def flatten_json(data, parent_key=""):

    items = {}

    for k, v in data.items():

        new_key = f"{parent_key}_{k}" if parent_key else k

        if isinstance(v, dict):

            items.update(
                flatten_json(v, new_key)
            )

        else:

            items[new_key] = v

    return items

# =========================================
# GET TELEMETRY
# =========================================

def get_telemetry(
    token,
    tb_device_id,
    selected_device,
    start_ts,
    end_ts
):

    headers = {
        "X-Authorization": f"Bearer {token}"
    }

    # =====================================
    # GET KEYS
    # =====================================

    keys_url = (

        f"{TB_URL}/api/plugins/telemetry/DEVICE/"
        f"{tb_device_id}/keys/timeseries"

    )

    r = requests.get(
        keys_url,
        headers=headers
    )

    r.raise_for_status()

    keys = r.json()

    if not keys:

        return pd.DataFrame()

    keys_str = ",".join(keys)

    # =====================================
    # GET DATA
    # =====================================

    data_url = (

        f"{TB_URL}/api/plugins/telemetry/DEVICE/"
        f"{tb_device_id}/values/timeseries"

        f"?keys={keys_str}"

        f"&startTs={start_ts}"

        f"&endTs={end_ts}"

        f"&limit=50000"

    )

    r = requests.get(
        data_url,
        headers=headers
    )

    r.raise_for_status()

    data = r.json()

    rows = []

    # =====================================
    # LOOP TB KEYS
    # =====================================

    for tb_key, values in data.items():

        for item in values:

            try:

                ts = pd.to_datetime(
                    item["ts"],
                    unit="ms"
                )

                raw_value = item["value"]

                parsed = json.loads(raw_value)

                if not isinstance(parsed, dict):
                    continue

                # =================================
                # FILTER DEVICE
                # =================================

                device_name = parsed.get("Device_ID", "")

                # REMOVE PREFIX
                clean_device = (
                    device_name
                    .replace("SAMBHAV_", "")
                    .strip()
                )

                selected_clean = (
                    selected_device
                    .replace("SAMBHAV_", "")
                    .strip()
                )

                if clean_device != selected_clean:
                    continue

                # =================================
                # FLATTEN JSON
                # =================================

                flat = flatten_json(parsed)

                # =================================
                # REMOVE META
                # =================================

                flat.pop("timestamp", None)
                flat.pop("Device_ID", None)

                # =================================
                # STORE
                # =================================

                row = {
                    "time": ts,
                    "device_id": device_name
                }

                row.update(flat)

                rows.append(row)

            except Exception as e:

                print("PARSE ERROR:", e)

    # =====================================
    # EMPTY
    # =====================================

    if not rows:

        print("NO ROWS FOUND")

        return pd.DataFrame()

    # =====================================
    # DATAFRAME
    # =====================================

    df = pd.DataFrame(rows)

    # =====================================
    # REMOVE DUPLICATES
    # =====================================

    df = df.groupby("time").first().reset_index()

    # =====================================
    # FORMAT TIME
    # =====================================

    df["time"] = pd.to_datetime(
        df["time"],
        utc=True
    )

    df["time"] = (

        df["time"]
        .dt.tz_convert("Asia/Kolkata")

    )

    df["time"] = (

        df["time"]
        .dt.strftime(
            "%d-%b-%Y %I:%M %p"
        )

    )

    print(df.head())

    return df
# ----------------------------
# Dashboard page
# ----------------------------
# @api_view(['GET'])
def dashboard(request):

    token = get_tb_token()

    devices = get_tb_devices(token)

    # DEBUG
    print("\n========= TB DEVICES =========")

    for d in devices:
        print(d["name"])

    print("==============================\n")

    table_data = []
    columns = []

    selected_device = request.GET.get("device")
    from_date = request.GET.get("from_date")
    to_date = request.GET.get("to_date")

    device_meta = DeviceMetadata.objects.all()

    # =====================================
    # FIND THINGSBOARD MAIN DEVICE
    # =====================================

    tb_device_id = None

    for d in devices:

        name = d.get("name", "")

        print("CHECKING DEVICE:", name)

        # IMPORTANT
        # Use CONTAINS instead of exact match

        if "SAMBHAV" in name.upper():

            tb_device_id = d["id"]["id"]

            print("FOUND TB DEVICE:", tb_device_id)

            break

    # =====================================
    # FETCH DATA
    # =====================================

    if (
        tb_device_id
        and
        selected_device
        and
        from_date
        and
        to_date
    ):

        start_ts = int(

            pd.Timestamp(from_date)
            .tz_localize("Asia/Kolkata")
            .timestamp() * 1000

        )

        end_ts = int(

            (
                pd.Timestamp(to_date)
                + pd.Timedelta(days=1)
            )
            .tz_localize("Asia/Kolkata")
            .timestamp() * 1000

        )

        print("SELECTED DEVICE:", selected_device)

        df = get_telemetry(

            token=token,

            tb_device_id=tb_device_id,

            selected_device=selected_device,

            start_ts=start_ts,

            end_ts=end_ts

        )

        if not df.empty:

            df = df.fillna("")

            columns = df.columns.tolist()

            table_data = df.to_dict(
                orient="records"
            )

            print("ROWS:", len(table_data))

        else:

            print("EMPTY DF")

    else:

        print("TB DEVICE NOT FOUND")

    # =====================================
    # DEVICE MAP
    # =====================================

    device_map = {}

    for d in device_meta:

        device_map[d.device_id] = {

            "village": d.village,

            "district": d.district,

            "category": d.category,

            "lat": d.latitude,

            "lon": d.longitude,

            "name": d.device_name

        }

    return render(

        request,

        "ingestion/dashboard_v5.html",

        {

            "devices": devices,

            "table_data": table_data,

            "columns": columns,

            "selected_device": selected_device,

            "from_date": from_date,

            "to_date": to_date,

            "device_map": device_map

        }

    )

# def db_tables(request):
#     with connection.cursor() as cursor:
#         cursor.execute("""
#             SELECT table_name 
#             FROM information_schema.tables 
#             WHERE table_schema = 'public'
#         """)
#         tables = [row[0] for row in cursor.fetchall()]

#     return render(request, "ingestion/db_tables.html", {"tables": tables})


# # ----------------------------
# # View Table Data (SAFE)
# # ----------------------------
# def db_table_view(request, table_name):
#     limit = int(request.GET.get("limit", 100))

#     # whitelist tables
#     with connection.cursor() as cursor:
#         cursor.execute("""
#             SELECT table_name 
#             FROM information_schema.tables 
#             WHERE table_schema = 'public'
#         """)
#         valid_tables = [row[0] for row in cursor.fetchall()]

#     if table_name not in valid_tables:
#         return HttpResponse("Invalid table", status=400)

#     with connection.cursor() as cursor:
#         cursor.execute(f'SELECT * FROM "{table_name}" LIMIT %s', [limit])
#         columns = [col[0] for col in cursor.description]
#         rows = cursor.fetchall()

#     data = [dict(zip(columns, row)) for row in rows]

#     return render(
#         request,
#         "ingestion/db_table.html",
#         {
#             "table_name": table_name,
#             "columns": columns,
#             "rows": data
#         }
#     )


# ----------------------------
# CSV Export
# ----------------------------

def export_csv(request):
    if request.method == "POST":
        table_data = json.loads(request.POST.get("table_data", "[]"))
        columns = json.loads(request.POST.get("columns", "[]"))

        if not table_data:
            return HttpResponse("No data available", status=200)

        df = pd.DataFrame(table_data)

        # ✅ FIX NaT / NaN crash
        df = df.fillna("")
        if "time" in df.columns:
            df["time"] = pd.to_datetime(df["time"], errors="coerce")
            df["time"] = df["time"].dt.tz_localize("UTC").dt.tz_convert("Asia/Kolkata")
            df["time"] = df["time"].dt.strftime("%d-%b-%Y %I:%M %p")

        device = request.POST.get("device", "device")

        device_name = request.POST.get(
            "device_name",
            "device"
        )

        category = request.POST.get(
            "category",
            "category"
        )

        village = request.POST.get(
            "village",
            "village"
        )

        from_date = request.POST.get(
            "from_date",
            ""
        )

        to_date = request.POST.get(
            "to_date",
            ""
        )

        # ====================================
        # CLEAN FILE NAME
        # ====================================

        safe_device = (
            device_name
            .replace(" ", "_")
        )

        safe_village = (
            village
            .replace(" ", "_")
        )

        safe_category = (
            category
            .replace(" ", "_")
        )

        filename = (

            f"{safe_category}"

            f"_{safe_village}"

            f"_{safe_device}"

            f"_{device}"

            f"_{from_date}"

            f"_to_{to_date}.csv"

        )

        response = HttpResponse(content_type="text/csv")
        response["Content-Disposition"] = f'attachment; filename="{filename}"'

        df.to_csv(response, index=False)
        return response