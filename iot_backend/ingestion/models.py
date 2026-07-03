from django.db import models

# Create your models here.
class Device(models.Model):
    id = models.UUIDField(primary_key=True)
    name = models.CharField(max_length=255)
    label = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'device'


class DeviceMetadata(models.Model):

    DEVICE_TYPES = [

        ("flow_meter", "Flow Meter"),

        ("weather_station", "Weather Station"),

        ("water_level", "Water Level Detector"),

    ]

    device_id = models.CharField(
        max_length=100,
        unique=True
    )
    device_name = models.CharField(
        max_length=100,
        
    )

    village = models.CharField(
        max_length=100,
        blank=True,
        null=True
    )

    taluka = models.CharField(
        max_length=100,
        blank=True,
        null=True
    )

    district = models.CharField(
        max_length=100,
        blank=True,
        null=True
    )

    latitude = models.FloatField(
        blank=True,
        null=True
    )

    longitude = models.FloatField(
        blank=True,
        null=True
    )

    category = models.CharField(
        max_length=50,
        choices=DEVICE_TYPES
    )

    installation_site = models.CharField(
        max_length=200,
        blank=True,
        null=True
    )

    created_at = models.DateTimeField(
        auto_now_add=True
    )

    def __str__(self):

        if self.device_name:
            return f"{self.device_name} ({self.device_id})"

        return self.device_id
    

class DeviceStatus(models.Model):

    device = models.OneToOneField(
        DeviceMetadata,
        on_delete=models.CASCADE,
        related_name="status"
    )

    last_seen = models.DateTimeField(
        null=True,
        blank=True
    )

    online = models.BooleanField(
        default=False
    )

    battery = models.FloatField(
        null=True,
        blank=True
    )

    rssi = models.IntegerField(
        null=True,
        blank=True
    )

    updated_at = models.DateTimeField(
        auto_now=True
    )

    def __str__(self):
        return self.device.device_id
    

class DeviceEventLog(models.Model):

    EVENT_TYPES = (
        ("DATA", "Data"),
        ("STATUS", "Status"),
        ("OTA", "OTA"),
        ("ERROR", "Error"),
    )

    device_id = models.CharField(max_length=50)
    event_type = models.CharField(max_length=20, choices=EVENT_TYPES)

    message = models.TextField()

    timestamp = models.DateTimeField(auto_now_add=True)

    extra = models.JSONField(null=True, blank=True)

    class Meta:
        ordering = ["-timestamp"]

    def __str__(self):
        return f"{self.device_id} - {self.event_type}"