from datetime import timedelta
from django.utils import timezone
from .models import DeviceEventLog,DeviceCurrentStatus

ONLINE_TIMEOUT = timedelta(minutes=30)


def is_device_online(last_seen):

    if last_seen is None:
        return False

    return (
        timezone.now() - last_seen
    ) <= ONLINE_TIMEOUT

def create_device_event(
        device_id,
        event_type,
        message,
        extra=None
):

    DeviceEventLog.objects.create(

        device_id=device_id,

        event_type=event_type,

        message=message,

        extra=extra

    )

def update_device_status(
        device_id,
        new_status,
        message
):

    old = DeviceCurrentStatus.objects.filter(
        device_id=device_id
    ).first()


    if old:

        if old.status != new_status:

            DeviceEventLog.objects.create(

                device_id=device_id,

                event_type=new_status,

                message=message,

                extra={
                    "old_status":old.status,
                    "new_status":new_status
                }

            )

            old.status=new_status
            old.save()


    else:

        DeviceCurrentStatus.objects.create(

            device_id=device_id,

            status=new_status

        )


        DeviceEventLog.objects.create(

            device_id=device_id,

            event_type=new_status,

            message=message

        )