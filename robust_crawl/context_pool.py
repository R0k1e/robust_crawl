import logging
import json
import math
import random
import functools
import time
import os
from concurrent.futures import Future
import threading
from tqdm import tqdm

import gevent
from gevent import sleep, spawn
from gevent.queue import Queue
from gevent.fileobject import FileObject
from gevent.lock import Semaphore

from playwright.sync_api import sync_playwright, Page

from tenacity import (
    retry,
    stop_after_attempt,
    wait_random_exponential,
)

from .token_bucket import TokenBucket
from .proxy_manager import ProxyManager
from .singleton_meta import SingletonMeta

logger = logging.getLogger(__name__)


class ContextPool(metaclass=SingletonMeta):
    class ContextInfo:

        def __init__(self, context, proxies, lifetime, index):
            self.context = context
            self.proxies = proxies
            self.lifetime = lifetime
            self.index = index
            self.page = self.context.new_page()

        def get_page(self):
            if self.page is None:
                self.page = self.context.new_page()
            while not self._check_page_available(self.page):
                self.page = self.context.new_page()
            return self.page

        def _check_page_available(self, page: Page):
            try:
                page.goto("about:blank")
                # 检查页面的加载状态
                if page.url == "about:blank":
                    return True
                else:
                    return False
            except Exception as e:
                logger.error(f"Page is not available: {e}")
                return False

    def __init__(self, config=None):
        self.num_contexts = config["num_contexts"]
        self.work_contexts = config["work_contexts"]
        self.context_lifetime = config["context_lifetime"]
        self.context_cooling_time = config["context_cooling_time"]
        self.duplicate_proxies = config["duplicate_proxies"]
        self.ensure_none_proxies = config["ensure_none_proxies"]
        self.have_proxy = config["have_proxy"]

        self.token_bucket = TokenBucket()
        self.proxy_manager = ProxyManager()

        self.download_path = config["downloads_path"]
        self.playwright = sync_playwright().start()
        self.device = self.playwright.devices["Desktop Chrome"]
        self.preference_path = config["preference_path"]
        self.preference_path = os.path.abspath(self.preference_path)

        if self.have_proxy:
            logger.info("Starting proxy manager")
            self.proxies = self.proxy_manager.get_proxies()
            self.proxy_available_semaphore = Semaphore(len(self.proxies))
            if len(self.proxies) < self.num_contexts:
                logger.warning(
                    f"Not enough proxies available. Required: {self.num_contexts}, Available: {len(self.proxies)}"
                )
                if self.duplicate_proxies:
                    if len(self.proxies) > 0:
                        multiplier = math.ceil(self.num_contexts / len(self.proxies))
                        self.proxies = self.proxies * multiplier
                    else:
                        raise Exception("No proxies available for duplication")
        else:
            self.proxies = []

        self.waiting_lock = Semaphore(1)
        self.cooling_lock = Semaphore(1)
        self.waiting_proxies = self.proxies
        self.cooling_proxies = []

        self.avail_context_infos = []
        self.avail_context_lock = Semaphore(1)
        # context that available to use
        self.avail_context_empty = Semaphore(self.num_contexts)
        self.avail_context_full = Semaphore(self.num_contexts)
        for _ in range(self.num_contexts):
            self.avail_context_full.acquire()
        # context that totally created
        self.using_context_semaphore = Semaphore(self.num_contexts)
        self.context_index_semaphore = Semaphore(1)
        self.context_index = [str(i) for i in range(self.num_contexts)]
        # control parallel working context num
        self.working_context_semaphore = Semaphore(self.work_contexts)

        for _ in tqdm(range(self.num_contexts), desc="Creating contexts"):
            try:
                self._create_context_info(timeout=10)
            except Exception:
                logger.error("Failed to create context info")
                pass
        logger.info(f"Successfully created {len(self.avail_context_infos)} contexts")

        self.is_start = True
        self.task = [spawn(self._reallocate_source), spawn(self._assign_task_to_thread)]

        self.task_queue = Queue(maxsize=self.work_contexts)

    def __del__(self):
        self.is_start = False
        gevent.joinall(self.task)
        for context_info in self.avail_context_infos:
            context = context_info.context
            context.close()

    @retry(
        stop=stop_after_attempt(10),
        wait=wait_random_exponential(multiplier=1, max=5),
    )
    def assign_task(self, task_func, *args, **kwargs):
        @functools.wraps(task_func)
        def _error_decorator(task_func):
            def _wrapper(*args, **kwargs):
                context_info = None
                page = None
                try:
                    context_info = self._consume_context_info()
                    page = context_info.page
                    self.token_bucket.get_tokens(
                        context_info.proxies, task_func.__qualname__
                    )
                    return task_func(page, *args, **kwargs)
                except Exception as e:
                    logger.warning(
                        f"Error in context dealing with {task_func.__qualname__}, proxies: {context_info.proxies}: {e}"
                    )
                    if context_info:
                        context_info.lifetime = 0
                    raise
                finally:
                    if context_info:
                        self._recycle_context_info(context_info)

            return _wrapper

        task_func = _error_decorator(task_func)
        future = Future()
        self.working_context_semaphore.acquire()
        self.task_queue.put((future, task_func, args, kwargs))
        result = future.result()
        self.working_context_semaphore.release()
        return result

    def _assign_task_to_thread(self):
        while self.is_start:
            future, task_func, args, kwargs = self.task_queue.get()
            thread = threading.Thread(
                target=self._run_task, args=(future, task_func, args, kwargs)
            )
            thread.start()

    def _run_task(self, future, task_func, args, kwargs):
        try:
            result = task_func(*args, **kwargs)
            self._set_future_result(future, result)
        except Exception as e:
            self._set_future_exception(future, e)

    def _reallocate_source(self):
        while self.is_start:
            try:
                self._check_cooling_proxies()
                self._create_context_info(timeout=1)
            except Exception:
                pass
            finally:
                sleep(10)

    def _create_context_info(self, timeout=None):
        proxies = None
        context = None
        ret1 = None
        ret2 = None
        try:
            ret1 = self.using_context_semaphore.acquire(timeout=timeout)
            if not ret1:
                raise TimeoutError(
                    "using context semaphore timeout in _create_context_info"
                )
            ret2 = self.avail_context_empty.acquire(timeout=timeout)
            if not ret2:
                raise TimeoutError(
                    "avail context empty semaphore timeout in _create_context_info"
                )

            if self.have_proxy:
                proxies = self._get_proxies(
                    ensure_none_proxies=self.ensure_none_proxies, timeout=timeout
                )
                proxy = {
                    "server": proxies["http"],
                }
            else:
                proxies = None
                proxy = None
            with self.context_index_semaphore:
                index = self.context_index.pop(0)
            preference_path = os.path.join(self.preference_path, index)
            self._create_preference(preference_path)
            context = self.playwright.chromium.launch_persistent_context(
                channel="chrome",
                user_data_dir=preference_path,
                headless=False,
                downloads_path=self.download_path,
                proxy=proxy,
                locale=proxies["locale"],
                timezone_id=proxies["timezone_id"],
                geolocation=proxies["geolocation"],
                color_scheme="dark",
                accept_downloads=True,
                bypass_csp=True,
            )
            context_info = self.ContextInfo(
                context, proxies, self.context_lifetime, index
            )
            with self.avail_context_lock:
                self.avail_context_infos.append(context_info)

            self.avail_context_full.release()
            # logger.info(f"Created new context with proxies: {proxies}")
        except Exception as e:
            if context:
                context.close()
            if proxies:
                self._release_proxies(proxies)
            if ret1:
                self.using_context_semaphore.release()
            if ret2:
                self.avail_context_empty.release()
            raise e

    def _consume_context_info(self, timeout=None):
        ret = None
        context_info = None
        try:
            ret = self.avail_context_full.acquire(timeout=timeout)
            if not ret:
                raise TimeoutError(
                    "avail context full semaphore timeout in _consume_context_info"
                )
            with self.avail_context_lock:
                context_info = self.avail_context_infos.pop(0)
            return context_info
        except Exception:
            if context_info:
                self.delete_context_info(context_info)
            raise
        finally:
            self.avail_context_empty.release()

    def delete_context_info(self, context_info):
        try:
            self._release_proxies(context_info.proxies)
            with self.context_index_semaphore:
                self.context_index.append(context_info.index)
            context = context_info.context
            context.close()
        finally:
            self.using_context_semaphore.release()
        try:
            self._create_context_info(timeout=1)
        except:
            pass

    def _recycle_context_info(self, context_info: ContextInfo):
        ret1 = None
        if context_info.lifetime <= 0:
            self.delete_context_info(context_info)
        else:
            # recycle to use
            try:
                ret1 = self.avail_context_empty.acquire()
                with self.avail_context_lock:
                    self.avail_context_infos.append(context_info)
                self.avail_context_full.release()
            except Exception:
                if ret1:
                    self.avail_context_empty.release()
                with self.avail_context_lock:
                    if context_info in self.avail_context_infos:
                        self.avail_context_infos.remove(context_info)
                self.delete_context_info(context_info)

    def _get_proxies(self, ensure_none_proxies=True, timeout=None):
        def _get_proxy_from_waiting():
            with self.waiting_lock:
                index = random.randrange(len(self.waiting_proxies))
                proxies = self.waiting_proxies.pop(index)
            return proxies

        ret = None
        proxies = None
        if not ensure_none_proxies:
            try:
                ret = self.proxy_available_semaphore.acquire(timeout=timeout)
                if not ret:
                    raise TimeoutError(
                        "proxy available semaphore timeout in get_proxies()"
                    )
                proxies = _get_proxy_from_waiting()
            except Exception:
                if ret:
                    self.proxy_available_semaphore.release()
                if proxies:
                    self._release_proxies(proxies)
                raise
        else:
            try:
                proxies = _get_proxy_from_waiting()
            except ValueError:
                proxies = None

        return proxies

    def _release_proxies(self, proxies):
        if proxies:
            with self.cooling_lock:
                future = {"proxies": proxies, "start_time": time.time()}
                self.cooling_proxies.append(future)

    def _check_cooling_proxies(self):
        completed_futures = []
        current_time = time.time()
        with self.cooling_lock:
            for future in self.cooling_proxies:
                if current_time - future["start_time"] > self.context_cooling_time * 60:
                    completed_futures.append(future)
            for future in completed_futures:
                self.cooling_proxies.remove(future)

        for future in completed_futures:
            proxies = future["proxies"]
            with self.waiting_lock:
                assert (
                    proxies not in self.waiting_proxies
                ), f"Proxy already in waiting list, proxies: {proxies}"
                self.waiting_proxies.append(proxies)
            self.proxy_available_semaphore.release()

    def _create_preference(self, file_path):
        file_path = os.path.join(file_path, "Default", "Preferences")
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with FileObject(file_path, "w") as f:
            default_preferences = {"plugins": {"always_open_pdf_externally": True}}
            json.dump(default_preferences, f)
