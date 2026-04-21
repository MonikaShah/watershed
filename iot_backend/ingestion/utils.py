import json
import pandas as pd
# from django.db import connection

# QUERY = """
# SELECT d.id AS device_id,
#        d.name AS device_name,
#        kd.key AS telemetry_key,
#        to_timestamp(t.ts / 1000) AS time,
#        t.ts,
#        COALESCE(
#            t.dbl_v::text,
#            t.long_v::text,
#            t.bool_v::text,
#            t.str_v,
#            t.json_v::text
#        ) AS value
# FROM ts_kv t
# JOIN key_dictionary kd ON t.key = kd.key_id
# JOIN device d ON t.entity_id = d.id
# WHERE d.id = %s
# AND t.ts BETWEEN %s AND %s
# ORDER BY t.ts ASC;
# """
# QUERY = """
# SELECT 
#     to_timestamp(t.ts / 1000) AS time,
#     kd.key AS telemetry_key,
#     COALESCE(
#         t.dbl_v::text,
#         t.long_v::text,
#         t.bool_v::text,
#         t.str_v,
#         t.json_v::text
#     ) AS value
# FROM public.ts_kv t
# JOIN public.key_dictionary kd ON t.key = kd.key_id
# WHERE t.entity_id = %s
# AND t.ts BETWEEN %s AND %s
# ORDER BY t.ts ASC;
# """

# def fetch_device_data(device_id, from_ts, to_ts):
#     with connection.cursor() as cursor:
#         cursor.execute(QUERY, [device_id, from_ts, to_ts])
#         rows = cursor.fetchall()

#     df = pd.DataFrame(rows, columns=[
#         'device_id', 'device_name', 'telemetry_key', 'time', 'ts', 'value'
#     ])

#     if df.empty:
#         return df

#     expanded_rows = []
#     for _, row in df.iterrows():
#         try:
#             parsed = json.loads(row['value'])
#             if isinstance(parsed, dict):
#                 for k, v in parsed.items():
#                     expanded_rows.append({
#                         'time': row['time'],
#                         'telemetry_key': f"{row['telemetry_key']}_{k}",
#                         'value': v,
#                     })
#                 continue
#         except Exception:
#             pass

#         expanded_rows.append({
#             'time': row['time'],
#             'telemetry_key': row['telemetry_key'],
#             'value': row['value'],
#         })

#     final_df = pd.DataFrame(expanded_rows)
#     return final_df.pivot_table(
#         index='time',
#         columns='telemetry_key',
#         values='value',
#         aggfunc='first'
#     ).reset_index()

# def get_telemetry(device_id, from_ts, to_ts):
#     with connection.cursor() as cursor:
#         cursor.execute(QUERY, [device_id, from_ts, to_ts])
#         rows = cursor.fetchall()

#     df = pd.DataFrame(rows, columns=[
#         'device_id', 'device_name', 'telemetry_key', 'time', 'ts', 'value'
#     ])

#     if df.empty:
#         return df

#     expanded_rows = []
#     for _, row in df.iterrows():
#         try:
#             parsed = json.loads(row['value'])
#             if isinstance(parsed, dict):
#                 for k, v in parsed.items():
#                     expanded_rows.append({
#                         'time': row['time'],
#                         'telemetry_key': f"{row['telemetry_key']}_{k}",
#                         'value': v,
#                     })
#                 continue
#         except Exception:
#             pass

#         expanded_rows.append({
#             'time': row['time'],
#             'telemetry_key': row['telemetry_key'],
#             'value': row['value'],
#         })

#     final_df = pd.DataFrame(expanded_rows)
#     return final_df.pivot_table(
#         index='time',
#         columns='telemetry_key',
#         values='value',
#         aggfunc='first'
#     ).reset_index()