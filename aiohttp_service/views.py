import pika
from aiohttp import web
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

import os


class ChromeHeadless:
    def __init__(self):
        self.options = Options()
        self.options.add_argument("--headless")
        self.options.add_argument("--window-size=1920x1080")


class RabbitmqConsumer:
    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='hello')

    def consumer(self):
        self.channel.basic_consume(self.callback,
                                   queue='hello',
                                   no_ack=True)
        print(' [*] Waiting for messages. To exit press CTRL+C')
        self.channel.start_consuming()

    def callback(self, ch, method, properties, body):
        chrome = ChromeHeadless()
        options = chrome.options
        DRIVER = 'chromedriver'
        driver = webdriver.Chrome(
            chrome_options=options, executable_path=DRIVER)
        driver.get(body.decode())
        dir_path = os.path.dirname(os.path.realpath(__file__))

        screenshot = driver.save_screenshot(
            f"{dir_path}/screenshots/my_screenshot.png")
        driver.quit()
        return body


async def index(request):
    rabbit = RabbitmqConsumer()
    rabbit.consumer()
    return web.Response(text=f"my_screenshot.png saved in screenshots")
