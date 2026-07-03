from ingestion.models import DeviceEventLog
from django.utils import timezone


def log_event(device_id, event_type, message):

    DeviceEventLog.objects.create(
        device_id=device_id,
        event_type=event_type,
        message=message,
        timestamp=timezone.now()
    )