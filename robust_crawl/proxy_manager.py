import random
from time import time
from gevent import spawn, sleep
from gevent.lock import Semaphore

from .proxy_creator import ProxyCreator


class ProxyManager:

    def __init__(self, cooling_time):
        self.proxy_creator = ProxyCreator()
        self.proxies = self.proxy_creator.get_proxies()
        self.proxy_available_semaphore = Semaphore(len(self.proxies))
        self.cooling_time = cooling_time

        self.waiting_lock = Semaphore(1)
        self.cooling_lock = Semaphore(1)

        self.waiting_proxies = self.proxies
        self.cooling_proxies = []
        self.task = spawn(self._check_cooling_proxies)

    @property
    def proxies_number(self):
        return len(self.proxies)

    def get_proxies(self, ensure_none_proxies=True, timeout=None):
        def _get_proxy_from_waiting():
            with self.waiting_lock:
                index = random.randrange(len(self.waiting_proxies))
                proxy = self.waiting_proxies.pop(index)
            return proxy

        ret = None
        proxies = {}
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
                    self.release_proxies(proxies)
                raise
        else:
            try:
                proxies = _get_proxy_from_waiting()
            except ValueError:
                proxies = {}

        return proxies

    def release_proxies(self, proxies):
        if proxies:
            with self.cooling_lock:
                future = {"proxies": proxies, "start_time": time()}
                self.cooling_proxies.append(future)

    def _check_cooling_proxies(self):
        while True:
            completed_futures = []
            current_time = time()
            with self.cooling_lock:
                for future in self.cooling_proxies:
                    if current_time - future["start_time"] > self.cooling_time * 60:
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
            sleep(10)
