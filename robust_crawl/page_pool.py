import logging
import json
import math
import random
import functools
import time
import os
import queue
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

from .playwright_backend import PlaywrightSyncBackend
from .proxy_manager import ProxyManager
from .token_bucket import TokenBucket
from .singleton_meta import SingletonMeta

logger = logging.getLogger(__name__)


class PagePool(metaclass=SingletonMeta):
    class PageInfo:

        def __init__(self, thread, instance, task_queue, page_index, proxies, lifetime):
            self.thread = thread
            self.instance = instance
            self.task_queue = task_queue
            self.page_index = page_index
            self.proxies = proxies
            self.lifetime = lifetime

    def __init__(self, config=None):
        self.num_pages = config.get("num_pages", 1)
        self.work_pages = config.get("work_pages", 1)
        self.page_lifetime = config.get("page_lifetime", 10)
        self.page_cooling_time = config.get("page_cooling_time", 1)
        self.duplicate_proxies = config.get("duplicate_proxies", False)
        self.ensure_none_proxies = config.get("ensure_none_proxies", True)
        self.have_proxy = config.get("have_proxy", True)
        self.download_path = config.get("downloads_path", "./output/browser_downloads")

        self.token_bucket = TokenBucket()
        self.backend = PlaywrightSyncBackend
        self.proxy_manager = ProxyManager(cooling_time=self.page_cooling_time)
        if self.proxy_manager.proxies_number < self.num_pages:
            logger.warning(
                f"Not enough proxies available. Required: {self.num_pages}, Available: {self.proxy_manager.proxies_number}"
            )
            
        self.page_index_semaphore = Semaphore(1)
        self.page_index = [str(i) for i in range(self.num_pages)]

        self.avail_page_infos = []
        self.avail_page_lock = Semaphore(1)
        # page that available to use
        self.avail_page_empty = Semaphore(self.num_pages)
        self.avail_page_full = Semaphore(self.num_pages)
        for _ in range(self.num_pages):
            self.avail_page_full.acquire()
        # page that totally created
        self.using_page_semaphore = Semaphore(self.num_pages)
        # control parallel working page num
        self.working_page_semaphore = Semaphore(self.work_pages)

        for _ in tqdm(range(self.num_pages), desc="Creating pages"):
            try:
                self._create_page_info(timeout=10)
            except Exception as e:
                logger.error(f"Failed to create page info: {e}")
                pass
        logger.info(f"Successfully created {len(self.avail_page_infos)} pages")

        self.is_start = True
        self.task_queue = queue.Queue(maxsize=self.work_pages)
        self.task = [spawn(self._reallocate_source)]

    def __del__(self):
        self.is_start = False
        gevent.joinall(self.task)
        for page_info in self.avail_page_infos:
            page = page_info.page
            page.close()

    @retry(
        stop=stop_after_attempt(10),
        wait=wait_random_exponential(multiplier=1, max=5),
    )
    def assign_task(self, task_func, *args, **kwargs):
        page_info = None
        ret1 = None
        future = Future()
        try:
            ret1 = self.working_page_semaphore.acquire()
            page_info = self._consume_page_info()
            self.token_bucket.get_tokens(page_info.proxies, task_func.__qualname__)
            page_info.task_queue.put((future, task_func, args, kwargs))
            result = future.result()
            return result
        except Exception as e:
            logger.warning(
                f"Error in page dealing with {task_func.__qualname__}, proxies: {page_info.proxies}: {e}"
            )
            if page_info:
                page_info.lifetime = 0
            raise
        finally:
            if ret1:
                self.working_page_semaphore.release()
            if page_info:
                page_info.lifetime -= 1
                self._recycle_page_info(page_info)

    def _reallocate_source(self):
        while self.is_start:
            try:
                self._create_page_info(timeout=1)
            except Exception:
                pass
            finally:
                sleep(10)

    def _create_page_info(self, timeout=None):
        def release_resource():
            if thread:
                thread.join(0)
            if proxies:
                self.proxy_manager.release_proxies(proxies)
            if ret1:
                self.using_page_semaphore.release()
            if ret2:
                self.avail_page_empty.release()

        proxies = {}
        thread = None
        ret1 = None
        ret2 = None
        try:
            ret1 = self.using_page_semaphore.acquire(timeout=timeout)
            if not ret1:
                raise TimeoutError("using page semaphore timeout in _create_page_info")
            ret2 = self.avail_page_empty.acquire(timeout=timeout)
            if not ret2:
                raise TimeoutError(
                    "avail page empty semaphore timeout in _create_page_info"
                )

            if self.have_proxy:
                proxies = self.proxy_manager.get_proxies(
                    ensure_none_proxies=self.ensure_none_proxies, timeout=timeout
                )
            else:
                proxies = {}

            page_index = self.page_index.pop(0)
            page_queue = queue.Queue(maxsize=1)
            instance = self.backend(page_queue, page_index, proxies, self.download_path)
            thread = threading.Thread(
                target=instance.start, name=f"PageThread-{page_index}"
            )
            page_info = self.PageInfo(
                thread, instance, page_queue, page_index, proxies, self.page_lifetime
            )
            with self.avail_page_lock:
                self.avail_page_infos.append(page_info)

            self.avail_page_full.release()
            # logger.info(f"Created new page with proxies: {proxies}")
        except TimeoutError as e:
            release_resource()
            raise e
        except Exception as e:
            logger.error(f"Error in creating page: {e}")
            release_resource()
            raise e

    def _delete_page_info(self, page_info):
        self.page_index.append(page_info.page_index)
        self.proxy_manager.release_proxies(page_info.proxies)
        page_info.instance.end()
        page_info.thread.join(0)
        self.using_page_semaphore.release()
        try:
            self._create_page_info(timeout=1)
        except:
            pass

    def _consume_page_info(self, timeout=None):
        ret = None
        page_info = None
        try:
            ret = self.avail_page_full.acquire(timeout=timeout)
            if not ret:
                raise TimeoutError(
                    "avail page full semaphore timeout in _consume_page_info"
                )
            with self.avail_page_lock:
                page_info = self.avail_page_infos.pop(0)
            return page_info
        except Exception:
            if page_info:
                self._delete_page_info(page_info)
            raise
        finally:
            self.avail_page_empty.release()

    def _recycle_page_info(self, page_info: PageInfo):
        ret1 = None
        if page_info.lifetime <= 0:
            self._delete_page_info(page_info)
        else:
            # recycle to use
            try:
                ret1 = self.avail_page_empty.acquire()
                with self.avail_page_lock:
                    self.avail_page_infos.append(page_info)
                self.avail_page_full.release()
            except Exception:
                if ret1:
                    self.avail_page_empty.release()
                with self.avail_page_lock:
                    if page_info in self.avail_page_infos:
                        self.avail_page_infos.remove(page_info)
                self._delete_page_info(page_info)
