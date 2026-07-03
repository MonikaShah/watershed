from ingestion.models import DeviceEventLog
from django.utils import timezone

from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer


def push_log_ws(data):

    channel_layer = get_channel_layer()

    if channel_layer:
        async_to_sync(channel_layer.group_send)(
            "logs",
            {
                "type": "send_log",
                "data": data,
            },
        )


def log_event(device_id, event_type, message, extra=None):

    DeviceEventLog.objects.create(
        device_id=device_id,
        event_type=event_type,
        message=message,
        timestamp=timezone.now(),
        extra=extra or {}
    )

    push_log_ws({
        "time": timezone.now().strftime("%H:%M:%S"),
        "device": device_id,
        "event": event_type,
        "message": message,
    })