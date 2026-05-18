from django.contrib import admin
from .models import DeviceMetadata


@admin.register(DeviceMetadata)
class DeviceMetadataAdmin(admin.ModelAdmin):

    list_display = (

        "device_id",

        "device_name",

        "category",

        "village",

        "taluka",

        "district",

        "latitude",

        "longitude",

        "created_at"

    )

    search_fields = (

        "device_id",

        "device_name",

        "village",

        "district"

    )

    list_filter = (

        "category",

        "district"

    )