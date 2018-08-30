from aiohttp import web
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

import os


class ChromeHeadless:
    def __init__(self):
        self.options = Options()
        self.options.add_argument("--headless")
        self.options.add_argument("--window-size=1920x1080")


async def index(request):
    chrome = ChromeHeadless()
    options = chrome.options
    DRIVER = 'chromedriver'
    driver = webdriver.Chrome(chrome_options=options, executable_path=DRIVER)
    driver.get('https://www.google.com')
    dir_path = os.path.dirname(os.path.realpath(__file__))
    screenshot = driver.save_screenshot(
        f"{dir_path}/screenshots/my_screenshot.png")
    driver.quit()
    return web.Response(text=f"my_screenshot.png saved in screenshots")
