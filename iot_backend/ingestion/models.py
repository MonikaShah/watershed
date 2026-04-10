from django.db import models

# Create your models here.
class Device(models.Model):
    id = models.UUIDField(primary_key=True)
    name = models.CharField(max_length=255)
    label = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'device'