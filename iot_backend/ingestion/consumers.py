import json
from channels.generic.websocket import AsyncWebsocketConsumer


class LogConsumer(AsyncWebsocketConsumer):

    async def connect(self):
        # 👇 ADD THIS LINE (join group)
        await self.channel_layer.group_add(
            "logs",
            self.channel_name
        )

        await self.accept()

    async def disconnect(self, close_code):
        # 👇 IMPORTANT cleanup
        await self.channel_layer.group_discard(
            "logs",
            self.channel_name
        )

    async def send_log(self, event):
        await self.send(text_data=json.dumps(event["data"]))