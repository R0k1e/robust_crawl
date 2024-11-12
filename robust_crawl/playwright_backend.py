import logging
import json
import os
import tempfile
import shutil

from gevent.fileobject import FileObject

from playwright.sync_api import sync_playwright, Page

class PlaywrightBackend:
    def __init__(self, task_queue, page_index, proxies):
        self.task_queue = task_queue
        self.playwright = sync_playwright().start()
        self.device = self.playwright.devices["Desktop Chrome"]
        self.preference_path = self._create_preference(page_index)
        self.page = self.create_page(proxies)

    def start(self):
        while self.is_start:
            future, task_func, args, kwargs = self.task_queue.get()
            try:
                result = task_func(self.page, *args, **kwargs)
                future.set_result(result)
            except Exception as e:
                future.set_exception(e)

    def end(self):
        self.is_start = False
        self.page.close()
        self.playwright.stop()
        shutil.rmtree(self.preference_path)

    def create_page(self, proxies):
        proxy = {
            "server": proxies["http"],
        }
        preference_path = self._create_preference()
        context = self.playwright.chromium.launch_persistent_context(
            channel="chrome",
            user_data_dir=preference_path,
            headless=False,
            downloads_path=self.download_path,
            proxy=proxy,
            locale=proxies.get("locale", None),
            timezone_id=proxies.get("timezone_id", None),
            geolocation=proxies.get("geolocation", None),
            color_scheme="dark",
            accept_downloads=True,
            bypass_csp=True,
        )
        page = context.create_page()
        return page

    def delete_page(self, page):
        context = page.context
        context.close()

    def _create_preference(self, index):
        preference_path = tempfile.mkdtemp()
        file_path = os.path.join(preference_path, "Default", "Preferences")
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with FileObject(file_path, "w") as f:
            default_preferences = {"plugins": {"always_open_pdf_externally": True}}
            json.dump(default_preferences, f)
        return preference_path