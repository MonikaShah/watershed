from django.urls import path
from . import views

urlpatterns = [
    path('ingest/', views.ingest_data, name='ingest_data'),
    path('ota/<str:device_type>/', views.ota, name='ota'),
    path('tb-dashboard/', views.dashboard, name='dashboard'),
    path('tb-dashboard/export-csv/', views.export_csv, name='export_csv'),
    path('db/', views.db_tables, name='db_tables'),
    path('db/<str:table_name>/', views.db_table_view, name='db_table_view'),

    
]