from django.urls import path
from . import views


urlpatterns = [
    # path("", views.main_portal, name="main_portal"),
    path("portal/", views.main_portal, name="main_portal"),
    path('ingest/', views.ingest_data, name='ingest_data'),
    path('ota/<str:device_type>/', views.ota, name='ota'),
    path('iot-dashboard/', views.dashboard_v5, name='dashboard'),
    path('iot-dashboard/export-csv/', views.export_csv, name='export_csv'),
    # path('db/', views.db_tables, name='db_tables'),
    # path('db/<str:table_name>/', views.db_table_view, name='db_table_view'),
    # path(
    #     'iot-dashboard/compare/',
    #     views.dashboard_compare,
    #     name='dashboard_compare'
    # ),
    # path( "device-status/", views.device_status, name="device_status",),
    path("device-logs/", views.device_logs, name="device_logs"),
    path("device-logs-ui/", views.device_logs_page, name="device_logs_page"),
    path("device-status-ui/", views.device_status_dashboard, name="device_status_dashboard"),
    # NEW API
    # path(
    #     'api/device-comparison/',
    #     views.device_comparison_api,
    #     name='device_comparison_api'
    # ),
    
]
