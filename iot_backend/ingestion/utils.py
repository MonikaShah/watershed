from datetime import timedelta
from django.utils import timezone

ONLINE_TIMEOUT = timedelta(minutes=30)


def is_device_online(last_seen):

    if last_seen is None:
        return False

    return (
        timezone.now() - last_seen
    ) <= ONLINE_TIMEOUT