from email import message
# from tkinter import EventType

from django.shortcuts import render
from rest_framework.decorators import api_view
from rest_framework.response import Response
from django.core.paginator import Paginator
from django.utils.dateparse import parse_date
from django.utils import timezone
from datetime import timedelta,datetime
import os,json,time
import requests
from ingestion.utils import is_device_online
from.models import DeviceMetadata,DeviceStatus,DeviceEventLog
# from django.db import connection
import pandas as pd
from django.http import HttpResponse
from django.shortcuts import render
from .models import Device, DeviceMetadata
from rest_framework.decorators import api_view
# views.py
from django.http import JsonResponse
from django.db import connections
from .utils import is_device_online
from django.conf import settings
from .services import (
    get_tb_token,
    get_tb_devices,
    get_device_status,
    update_device_status,
)

TB_URL = settings.TB_URL
TB_USERNAME = settings.TB_USERNAME
TB_PASSWORD = settings.TB_PASSWORD

def main_portal(request):
    return render(request, "ingestion/main_portal.html")

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

                # ts = pd.to_datetime(
                #     item["ts"],
                #     unit="ms"
                # )
                raw_value = item["value"]

                parsed = json.loads(raw_value)
                device_ts = parsed.get("timestamp")

                if not device_ts:
                    continue

                ts = pd.to_datetime(
                    device_ts,
                    unit="s",
                    utc=True
                )

                

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
                device_name = parsed.get("Device_ID", "")

                clean_device = device_name.replace("SAMBHAV_", "").strip()
                selected_clean = selected_device.replace("SAMBHAV_", "").strip()

                # print("Telemetry Device_ID :", repr(device_name))
                # print("Clean telemetry     :", repr(clean_device))
                # print("Selected device     :", repr(selected_clean))

                if clean_device != selected_clean:
                    # print("SKIPPED")
                    continue

                # print("MATCHED")
                if clean_device != selected_clean:
                    continue

                # =================================
                # FLATTEN JSON
                # =================================

                flat = flatten_json(parsed)
                # print("FLAT =", flat)

                for k in flat.keys():

                    if (
                        '"' in k
                        or '!' in k
                        or 'levdl' in k
                        or 'cumd' in k
                    ):

                        print("\n===================")
                        print("BAD KEY:", k)
                        print("RAW JSON:")
                        print(json.dumps(parsed, indent=2))
                        print("===================\n")

                # =======================do==========
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
    ).dt.tz_convert("Asia/Kolkata")


    # df["time"] = pd.to_datetime(
    #     df["time"],
    #     utc=True
    # )

    # df["time"] = (

    #     df["time"]
    #     .dt.tz_convert("Asia/Kolkata")

    # )

    # df["time"] = (

    #     df["time"]
    #     .dt.strftime(
    #         "%d-%b-%Y %I:%M %p"
    #     )

    # )

    # print(df.head())
    # print("TB TS :", item["ts"])

    # print(
    #     "DEVICE TS :",
    #     parsed.get("timestamp")
    # )
    return df
# ----------------------------
# Dashboard page
# ----------------------------
# @api_view(['GET'])
def dashboard_v5(request):

    token = get_tb_token()

    devices = get_tb_devices(token)
    # for d in devices:
    #     print(d["name"], d["id"]["id"])

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

            # print("DF COLUMNS:")
            # for c in df.columns:
            #     print(c)

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

        "ingestion/dashboard_v6.html",

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
def dashboard_compare(request):

    device_meta = DeviceMetadata.objects.all()

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

        "ingestion/dashboard_compare.html",

        {
            "device_map": device_map
        }

    )

@api_view(["GET"])

def device_comparison_api(request):

    selected_devices = request.GET.get(
        "devices",
        ""
    ).split(",")

    metric = request.GET.get(
        "metric",
        ""
    )

    from_date = request.GET.get(
        "from_date"
    )

    to_date = request.GET.get(
        "to_date"
    )

    if (
        not selected_devices
        or not metric
        or not from_date
        or not to_date
    ):
        return Response({
            "labels": [],
            "datasets": [],
            "nodata": ["No data available"]
        })

    token = get_tb_token()

    devices = get_tb_devices(token)

    tb_device_id = None

    for d in devices:

        if "SAMBHAV" in d["name"].upper():

            tb_device_id = d["id"]["id"]

            break

    if not tb_device_id:

        return Response({
            "error": "TB Device not found"
        })

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

    all_times = set()

    device_frames = {}

    # ----------------------------------
    # FETCH EACH DEVICE
    # ----------------------------------

    for device in selected_devices:

        df = get_telemetry(

            token=token,

            tb_device_id=tb_device_id,

            selected_device=device,

            start_ts=start_ts,

            end_ts=end_ts

        )

        if df.empty:

            continue

        if metric not in df.columns:

            print(
                f"{metric} not found in {device}"
            )

            continue

        device_frames[device] = df

        all_times.update(
            df["time"].tolist()
        )

    # ----------------------------------
    # NO DATA
    # ----------------------------------

    if not device_frames:

        return Response({

            "labels": [],

            "datasets": []

        })

    labels = sorted(all_times)
    

    datasets = []

    # ----------------------------------
    # BUILD CHART DATA
    # ----------------------------------

    for device, df in device_frames.items():

        lookup = dict(

            zip(

                df["time"],

                pd.to_numeric(
                    df[metric],
                    errors="coerce"
                )

            )

        )

        values = []

        for t in labels:

            v = lookup.get(t)

            if pd.isna(v):

                values.append(None)

            else:

                values.append(
                    float(v)
                )

        datasets.append({

            "label": device,

            "data": values,

            "borderWidth": 2,

            "tension": 0.3

        })
    label_strings = [

        t.strftime(
            "%d-%b-%Y %I:%M %p"
        )

        for t in labels

    ]
    return Response({

        "labels": label_strings,

        "datasets": datasets,
         "nodata": []

    })

def get_device_last_seen(token, tb_device_id):

    headers = {
        "X-Authorization": f"Bearer {token}"
    }

    # -------------------------------
    # Get available telemetry keys
    # -------------------------------

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

    # -------------------------------
    # Download recent telemetry
    # -------------------------------

    end_ts = int(pd.Timestamp.utcnow().timestamp() * 1000)

    start_ts = end_ts - (30 * 24 * 60 * 60 * 1000)   # last 30 days

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

    last_seen = {}

    for tb_key, values in data.items():

        for item in values:

            try:

                parsed = json.loads(item["value"])

                device = parsed.get("Device_ID", "").replace("SAMBHAV_", "").strip()

                ts = parsed.get("timestamp")

                if not device or not ts:
                    continue

                ts = pd.to_datetime(
                    ts,
                    unit="s",
                    utc=True
                ).tz_convert("Asia/Kolkata")

                if (
                    device not in last_seen
                    or
                    ts > last_seen[device]
                ):
                    last_seen[device] = ts

            except Exception:
                continue

    return last_seen

def get_device_latest_status(token, tb_device_id):

    headers = {
        "X-Authorization": f"Bearer {token}"
    }

    url = (
        f"{TB_URL}/api/plugins/telemetry/DEVICE/"
        f"{tb_device_id}/values/timeseries"
    )


    r = requests.get(
        url,
        headers=headers
    )

    r.raise_for_status()

    data = r.json()
    print(data.keys())

    last_seen = {}


    for key, values in data.items():
        
        print(key, len(values))
        print(repr(key))
    
        for item in values:
            
            try:

                parsed = json.loads(
                    item["value"]
                )

                device_id = (
                    parsed.get("Device_ID","")
                    .replace("SAMBHAV_","")
                    .strip()
                )
                battery = parsed.get("battery_Volt")
                rssi = parsed.get("rssi")

                if not device_id:
                    continue


                timestamp = parsed.get(
                    "timestamp"
                )


                if timestamp:

                    ts = pd.to_datetime(
                        timestamp,
                        unit="s",
                        utc=True
                    ).tz_convert(
                        "Asia/Kolkata"
                    )

                else:

                    ts = pd.to_datetime(
                        item["ts"],
                        unit="ms",
                        utc=True
                    ).tz_convert(
                        "Asia/Kolkata"
                    )
                if device_id == "00100028":
                    print("=" * 60)
                    print("Telemetry key :", repr(key))
                    print("Device_ID     :", parsed.get("Device_ID"))
                    print("Payload ts    :", parsed.get("timestamp"))
                    print("TB ts         :", item["ts"])
                    print("Converted ts  :", ts)
                    print("Now           :", pd.Timestamp.now(tz="Asia/Kolkata"))
                    print("=" * 60)

                # last_seen[device_id] = {

                #     "time":ts,

                #     "payload":parsed,
                #     "battery": battery,

                #     "rssi": rssi,

                # }
                current = last_seen.get(device_id)

                if current is None or ts > current["time"]:
                    last_seen[device_id] = {
                        "time": ts,
                        "payload": parsed,
                        "battery": battery,
                        "rssi": rssi,
                    }


            except Exception as e:

                print(e)


    return last_seen
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
    

# views.py

def get_device_telemetry_logs(token, device_id, limit=200):

    url = (
        f"{TB_URL}/api/plugins/telemetry/DEVICE/"
        f"{device_id}/values/timeseries"
        f"?limit={limit}"
    )


    headers = {
        "X-Authorization": f"Bearer {token}"
    }


    response = requests.get(
        url,
        headers=headers,
        timeout=10
    )


    if response.status_code != 200:
        return []


    data = response.json()


    logs = []


    for parameter, values in data.items():

        for v in values:

            logs.append({

                "parameter": parameter,

                "value": v["value"],

                "timestamp": datetime.fromtimestamp(
                    v["ts"]/1000
                )
            })


    return logs

def device_status_dashboard(request):

    token = get_tb_token()

    devices = get_tb_devices(token)

    tb_device_id = None

    for d in devices:

        if "SAMBHAV" in d.get("name", "").upper():

            tb_device_id = d["id"]["id"]

            break

    # -----------------------------------
    # Get latest timestamp of each device
    # -----------------------------------

    last_seen = {}

    if tb_device_id:

        last_seen = get_device_latest_status(
            token,
            tb_device_id
        )

    # -----------------------------------
    # Build table
    # -----------------------------------

        # -----------------------------------
    # Build tables
    # -----------------------------------

    rows = []       # weather
    water = []      # water level

    now = timezone.now()

    for d in DeviceMetadata.objects.all().order_by("device_id"):

        device = d.device_id.replace("SAMBHAV_", "").strip()

        if device == "00100028":
            print("DB device_id :", repr(d.device_id))
            print("Lookup key   :", repr(device))
            print("Found        :", device in last_seen)
            print("Latest data  :", last_seen.get(device))
        latest_data = last_seen.get(device)

        if latest_data is None:

            latest = None
            status = "No Data"
            age = "-"

        else:

            latest = latest_data["time"]

            diff = now - latest_data["time"]

            age = str(diff).split(".")[0]


            if diff <= timedelta(minutes=60):

                status = "Online"

            elif diff <= timedelta(hours=24):

                status = "Offline"

            else:

                status = "Inactive"


        device_row = {

            "device_id": d.device_id,

            "device_name": d.device_name,

            "village": d.village,

            "district": d.district,

            "category": d.category,

            "last_seen": latest,

            "age": age,

            "status": status

        }

        if device == "00100028":
            print("Latest :", latest)
            print("Diff   :", diff)
            print("Status :", status)
        # Separate devices

        if d.category == "water_level":

            water.append(device_row)


        elif d.category == "weather_station":

            rows.append(device_row)

    return render(

    request,

    "ingestion/device_status.html",

    {

        "rows": rows,          # weather devices

        "water": water,        # water devices

        "total_count": len(rows)+len(water),

        "water_count": len(water),

        "weather_count": len(rows),

        "online_count": sum(
            1 for d in rows+water 
            if d["status"]=="Online"
        ),

        "offline_count": sum(
            1 for d in rows+water 
            if d["status"]=="Offline"
        ),
        "inactive_count": sum(
            1 for d in rows+water 
            if d["status"]=="Inactive"
        ),
        # Add this
        "status_note": [
            "🟢 Online: Telemetry received within the last 60 minutes.",
            "🔴 Offline: No telemetry received for 60 minutes to 24 hours.",
            "🟠 Inactive: No telemetry received for more than 24 hours.",
            # "ℹ️ Event Log Severity: STATUS (≤60 min), WARNING (60–90 min), ERROR (>90 min)."
        ]
    }

)
    
# def device_status(request):

#     statuses = (
#         DeviceStatus.objects
#         .select_related("device")
#         .order_by("device__category", "device__village")
#     )

#     water = []
#     weather = []

#     online_count = 0

#     for s in statuses:

#         online = is_device_online(s.last_seen)

#         if online:
#             online_count += 1

#         row = {
#             "device_id": s.device.device_id,
#             "device_name": s.device.device_name,
#             "village": s.device.village,
#             "district": s.device.district,
#             "category": s.device.category,
#             "last_seen": s.last_seen,
#             "online": online,
#             "battery": s.battery,
#             "rssi": s.rssi,
#         }

#         if s.device.category == "water_level":
#             water.append(row)

#         elif s.device.category == "weather_station":
#             weather.append(row)

#     return render(
#         request,
#         "ingestion/device_status.html",
#         {
#             "water": water,
#             "weather": weather,
#             "water_count": len(water),
#             "weather_count": len(weather),
#             "total_count": len(water) + len(weather),
#             "online_count": online_count,
#             "offline_count": len(water) + len(weather) - online_count,
#         },
#     )


def device_logs(request):

    logs = DeviceEventLog.objects.all()[:200]

    data = [
        {
            # "time": l.timestamp.strftime("%H:%M:%S"),
            "date": l.timestamp.strftime("%d %b %Y"),
            "time": l.timestamp.strftime("%H:%M:%S"),
            "device": l.device_id,
            "event": l.event_type,
            "message": l.message,
        }
        for l in logs
    ]

    return JsonResponse(data, safe=False)

# def device_logs_page(request):

#     logs = DeviceEventLog.objects.all().order_by("-timestamp")[:200]

#     print("LOG COUNT:", logs.count())

#     return render(
#         request,
#         "ingestion/device_logs.html",
#         {"logs": logs}
#     )


def device_logs_page(request):

    token = get_tb_token()

    devices = get_tb_devices(token)

    now = timezone.localtime(timezone.now())


    # Find SAMBHAV ThingsBoard device
    tb_device_id = None

    for d in devices:

        if "SAMBHAV" in d.get("name", "").upper():

            tb_device_id = d["id"]["id"]
            break


    if not tb_device_id:

        return render(
            request,
            "ingestion/device_logs.html",
            {
                "logs": [],
                "page_obj": None
            }
        )


    # Get latest telemetry status
    latest_status = get_device_latest_status(
        token,
        tb_device_id
    )


    # Device metadata dictionary
    metadata = {
        d.device_id.replace("SAMBHAV_", "").strip(): d
        for d in DeviceMetadata.objects.all()
    }


    # -----------------------------------
    # Generate new events
    # -----------------------------------

    for device_id, data in latest_status.items():
        status_count = 0
        warning_count = 0
        error_count = 00
        if device_id not in metadata:
            continue


        device = metadata[device_id]


        last_seen = data["time"]

        battery = data.get("battery")

        rssi = data.get("rssi")


        # Convert pandas timestamp
        if hasattr(last_seen, "to_pydatetime"):

            last_seen = last_seen.to_pydatetime()


        # Make timezone aware
        if timezone.is_naive(last_seen):

            last_seen = timezone.make_aware(last_seen)


        last_seen = timezone.localtime(last_seen)


        diff = now - last_seen



        # -------------------------------
        # Decide event
        # -------------------------------

        if diff <= timedelta(minutes=60):

            event_type = "STATUS"

            message = "Device online"

            cause = ""
            status_count += 1

        elif diff <= timedelta(minutes=90):

            event_type = "WARNING"

            message = (
                f"Telemetry delayed by "
                f"{int(diff.total_seconds()/60)} minutes"
            )

            cause = ""
            warning_count += 1

        else:

            if battery is not None and battery < 11.5:

                cause = "Possible low battery"


            elif rssi is not None and rssi < 10:

                cause = "Possible weak GSM signal"


            else:

                cause = (
                    "Power loss / Network issue "
                    "(cannot determine exactly)"
                )


            event_type = "ERROR"

            message = (
                f"No telemetry received for "
                f"{int(diff.total_seconds()/3600)} hours[ ({int(diff.total_seconds()/60)} minutes)]"
            )

            error_count += 1
        # -------------------------------
        # Check last event
        # -------------------------------

        latest_event = (
            DeviceEventLog.objects
            .filter(
                device_id=device.device_id
            )
            .order_by("-timestamp")
            .first()
        )


        new_event_key = (
            f"{event_type}_{message}"
        )


        old_event_key = None


        if latest_event:

            old_event_key = (
                f"{latest_event.event_type}_"
                f"{latest_event.message}"
            )



        # Save only changed events

        if new_event_key != old_event_key:


            DeviceEventLog.objects.create(

                device_id=device.device_id,

                event_type=event_type,

                message=message,

                extra={

                    "cause": cause,

                    "battery": battery,

                    "rssi": rssi,

                    "last_seen": str(last_seen),

                    "device_name": device.device_name,

                    "village": device.village,

                    "district": device.district,

                    "category": device.category

                }

            )



    # -----------------------------------
    # Fetch logs from database
    # -----------------------------------

    event_logs = (
        DeviceEventLog.objects
        .all()
        .order_by("-timestamp")[:200]
    )


    # -----------------------------------
    # Add metadata for display
    # -----------------------------------

    logs = []


    metadata_full = {
        d.device_id: d
        for d in DeviceMetadata.objects.all()
    }


    for log in event_logs:


        device = metadata_full.get(
            log.device_id
        )


        extra = log.extra or {}


        logs.append({

            "timestamp": log.timestamp,

            "device_id": log.device_id,


            "device_name": (
                device.device_name
                if device
                else extra.get(
                    "device_name",
                    "-"
                )
            ),


            "village": (
                device.village
                if device
                else extra.get(
                    "village",
                    "-"
                )
            ),


            "district": (
                device.district
                if device
                else extra.get(
                    "district",
                    "-"
                )
            ),


            "category": (
                device.category
                if device
                else extra.get(
                    "category",
                    "-"
                )
            ),


            "event_type": log.event_type,


            "message": log.message,


            "cause": extra.get(
                "cause",
                ""
            )

        })



    # -----------------------------------
    # Pagination
    # -----------------------------------

    paginator = Paginator(
        logs,
        50
    )


    page_obj = paginator.get_page(
        request.GET.get("page")
    )

    today = timezone.localdate()

    today_events = DeviceEventLog.objects.filter(
        timestamp__date=today
    )


    status_count = today_events.filter(
        event_type="STATUS"
    ).count()


    warning_count = today_events.filter(
        event_type="WARNING"
    ).count()


    error_count = today_events.filter(
        event_type="ERROR"
    ).count()

    return render(
        request,
        "ingestion/device_logs.html",
        {
            "logs": page_obj,
            "page_obj": page_obj,
            "status_count": status_count,
            "warning_count": warning_count,
            "error_count": error_count,
            # Add this
        "event_legend": [
            {
                "class": "status",
                "label": "STATUS",
                "description": "Telemetry received within the last 60 minutes."
            },
            {
                "class": "warning",
                "label": "WARNING",
                "description": "Telemetry delayed (60–90 minutes)."
            },
            {
                "class": "error",
                "label": "ERROR",
                "description": "No telemetry received for more than 90 minutes."
            }
        ]
        }
    )

        