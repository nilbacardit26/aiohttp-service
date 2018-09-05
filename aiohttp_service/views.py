import pika
import json
import asyncio
import aioamqp
import functools
from aiohttp import web
from selenium import webdriver
from upload import UploadTUS
from selenium.webdriver.chrome.options import Options

import os

CHUNK_SIZE = 1024 * 1024 * 5


class Payload:
    def __init__(self, payload):
        self.payload = json.loads(payload)

    @property
    def title(self):
        return self.payload.get('title', 'Title unknown')

    @property
    def token(self):
        return self.payload.get('token')

    @property
    def url(self):
        return self.payload.get('url')

    @property
    def url_screenshot(self):
        return self.payload.get('url_screenshot')

    @property
    def dumps(self):
        return json.dumps(self.payload)


class ChromeHeadless:
    def __init__(self):
        self.options = Options()
        self.options.add_argument("--headless")
        self.options.add_argument("--window-size=1920x1080")


class RabbitmqConsumer:
    async def consumer(self):
        try:
            transport, protocol = await aioamqp.connect('localhost', 5672)
        except aioamqp.AmqpClosedConnection:
            print("closed connections")
            return

        channel = await protocol.channel()

        await channel.queue(queue_name='screenshot-task', durable=True)
        await channel.basic_qos(prefetch_count=1, prefetch_size=0, connection_global=False)
        await channel.basic_consume(self.callback, queue_name='screenshot-task')

    async def generator(self, file_as_base64):
        """
        Iterator for upload file with tus protocol using base spyder library.
        """
        start, end = 0, CHUNK_SIZE
        total_size = len(file_as_base64)

        while start < total_size:
            yield file_as_base64[start:end]
            start += CHUNK_SIZE
            end += CHUNK_SIZE
        pass

    async def callback(self, channel, body, envelope, properties):
        await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)
        chrome = ChromeHeadless()
        options = chrome.options
        DRIVER = 'chromedriver'
        driver = webdriver.Chrome(
            chrome_options=options, executable_path=DRIVER)
        payload = Payload(body.decode())
        driver.get(payload.url_screenshot)
        screenshot_file = driver.get_screenshot_as_base64()
        tus_upload = UploadTUS(payload.token)
        await tus_upload.upload(self.generator(screenshot_file), payload.title, payload.url)
        driver.quit()
        await channel.basic_client_ack(delivery_tag=True)


async def index(request):
    rabbit = RabbitmqConsumer()
    await rabbit.consumer()
    return web.Response(text=f"The screenshot will be process")
