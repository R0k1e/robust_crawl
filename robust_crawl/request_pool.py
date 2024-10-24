import itertools
import logging
import os
import random
import traceback
from typing import List
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from gevent import sleep, Timeout
from gevent.lock import Semaphore
from openai import OpenAI
from requests.exceptions import (
    ConnectionError,
    HTTPError,
    ProxyError,
    SSLError as RequestsSSLError,
)

from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
    before_sleep_log,
)

from . import ROBUST_CRAWL_CONFIG as glb_pipeline_cfg
from .exceptions import AntiCrawlingDetectedException
from .token_bucket import TokenBucket
from .proxy_manager import ProxyManager
from .singleton_meta import SingletonMeta
from .context_pool import ContextPool


logger = logging.getLogger(__name__)


class RequestPool(metaclass=SingletonMeta):
    def __init__(self):
        self.config = glb_pipeline_cfg["RequestPool"]

        self.keys = [
            os.environ.get("OPENAI_API_KEY"),
        ]
        self.keys_iter = itertools.cycle(self.keys)
        self.model = self.config["GPT"]["model_type"]
        self.clients = []
        for k in self.keys:
            client = OpenAI(
                api_key=k,
                base_url=os.environ.get("OPENAI_API_BASE"),
            )
            self.clients.append(client)
        self.clients_iter = itertools.cycle(self.clients)

        self.wait_sec = self.config["wait_sec"]
        self.semaphore = Semaphore(self.config["max_concurrent_requests"])
        self.token_bucket = TokenBucket(self.config["TokenBucket"])

        self.ua = UserAgent()
        self.ua_list = [
            self.ua.chrome,
            self.ua.firefox,
            self.ua.edge,
        ]
        self.proxy_iterators = {}
        self.proxy_manager = ProxyManager(self.config["Proxy"])
        self.proxy_manager.start_proxies()

        self.context_pool = ContextPool(self.config["ContextPool"])

    def llm_request(self, messages, model=None, have_system_message=False):
        if model is None:
            model = self.model
        client = next(self.clients_iter)
        self.token_bucket.get_tokens(model)
        if isinstance(messages, list):
            messages = self._list2messages(messages, have_system_message)
        elif isinstance(messages, str):
            messages = self._list2messages([messages], have_system_message)
        response = self._completion_with_backoff(messages, model, client)
        return response

    def url_request(self, url, method="GET", **kwargs):
        try:
            return self._fetch_url(url, method, **kwargs)
        except Exception as e:
            logger.error(f"Error fetching URL: {url}\n{e}")
            raise

    def browser_task(self, task_func, *args, **kwargs):
        return self.context_pool.assign_task(task_func, *args, **kwargs)

    def browser_request(self, url, method="GET", **kwargs):
        def fetch_url(page, url, method, **kwargs):
            match method.upper():
                case "GET":
                    response = page.request.get(url, **kwargs)
                case "POST":
                    response = page.request.post(url, **kwargs)
                case "PUT":
                    response = page.request.put(url, **kwargs)
                case "DELETE":
                    response = page.request.delete(url, **kwargs)
                case _:
                    raise ValueError(f"Unsupported HTTP method: {method}")
            return response

        return self.context_pool.assign_task(fetch_url, url, method, **kwargs)

    def browser_download(self):
        return
        # TODO 浏览器当前不会渲染pdf链接，但是捕获到download save的时候会 RuntimeError: cannot schedule new futures after interpreter shutdown
        with page.expect_download() as download_info:
            pdf_link_tag.click()
        download = download_info.value
        suggest_name = download.suggested_filename
        path = f"./download/{os.path.splitext(suggest_name)[0]}/{suggest_name}"
        download.save_as(path)

    def _get_proxy_for_url(self, base_url):
        if self.proxy_iterators is None or base_url not in self.proxy_iterators.keys():
            proxies = self.proxy_manager.get_proxies()
            self.proxy_iterators[base_url] = proxies

        if self.proxy_iterators[base_url]:
            proxies = random.choice(self.proxy_iterators[base_url])
        else:
            proxies = None
        return proxies

    @retry(
        wait=wait_random_exponential(multiplier=2, max=5),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(
            (
                HTTPError,
                ConnectionError,
                AntiCrawlingDetectedException,
                ProxyError,
                RequestsSSLError,
                Timeout,
            )
        ),
        # before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def _fetch_url(self, url, method, **kwargs):
        with self.semaphore:
            proxies = kwargs.pop("proxies", {})
            base_url = urlparse(url).netloc
            if not proxies:
                proxies = self._get_proxy_for_url(base_url)
                if proxies:
                    kwargs["proxies"] = proxies

            self.token_bucket.get_tokens(base_url, proxies)

            headers = kwargs.pop("headers", {})
            if not headers:
                kwargs["headers"] = {
                    "User-Agent": random.choice(self.ua_list),
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                    "Accept-Language": f"en-US,en;q=0.9",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Upgrade-Insecure-Requests": "1",
                    "Connection": "keep-alive",
                }

            session = requests.Session()
            session.max_redirects = 100

            if "http" not in url:
                url = f"https://{url}"

            try:
                timeout = Timeout(60)
                timeout.start()
                response = session.request(
                    method,
                    url,
                    verify=False,
                    timeout=60,
                    **kwargs,
                )

                self._detect_anti_crawling(response)
                response.raise_for_status()
            except AntiCrawlingDetectedException as e:
                logger.warning(f"Anti-crawling detected of url {url}:\n{e}")
                raise
            except HTTPError as e:
                logger.warning(f"HTTP {e.response.status_code} of url {url}:\n{e}")
                if e.response.status_code in [404, 405]:
                    return e.response
                elif e.response.status_code >= 500 and e.response.status_code < 600:
                    raise
                else:
                    raise
            except (RequestsSSLError, ConnectionError, HTTPError, ProxyError) as e:
                logger.debug(f"SSL error of url {url}:\n{e}")
                kwargs["proxies"] = proxies = self._get_proxy_for_url(url)
                self.token_bucket.get_tokens(base_url, proxies)
                raise
            except Exception as e:
                logger.error(f"Unknown error of url: {url}:\n{e}")
                logger.error(traceback.format_exc())
                raise
            finally:
                timeout.close()
                session.close()

            sleep(random.uniform(1, self.wait_sec))
            return response

    def _detect_anti_crawling(self, response):
        """
        Detects various anti-crawling techniques in the response.

        Args:
            response (requests.Response): The response object to check.

        Raises:
            AntiCrawlingDetectedException: If anti-crawling technique is detected.
        """
        # Check for rate limiting
        if response.status_code == 429:
            raise AntiCrawlingDetectedException(
                f"Rate limit detected because of {response.status_code}, url: {response.url}"
            )

        if "X-RateLimit-Remaining" in response.headers:
            remaining = int(response.headers["X-RateLimit-Remaining"])
            if remaining <= 1:
                logger.warning(f"Rate limit almost reached. Remaining: {remaining}")

        # Content type checks
        if "text/html" in response.headers.get("Content-Type", ""):
            soup = BeautifulSoup(response.text, "html.parser")

            # Check for reCAPTCHA
            if soup.find("div", class_="g-recaptcha") or "reCAPTCHA" in response.text:
                raise AntiCrawlingDetectedException("reCAPTCHA detected")

            # Check for access denied
            if (
                soup.find("title", text="Access Denied")
                or "blocked" in response.text.lower()
            ):
                raise AntiCrawlingDetectedException("Access possibly denied")

    @retry(
        wait=wait_random_exponential(multiplier=2),
        stop=stop_after_attempt(5),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def _completion_with_backoff(self, messages, model, client):
        try:
            response = client.chat.completions.create(
                model=model,
                messages=messages,
            )
            answer = response.choices[0].message.content
        except Exception as e:
            logger.error(f"Error in completion: {e}")
            raise
        return answer

    def _list2messages(self, contents: List, is_system: bool = False):
        """
        Convert a list of contents into a list of messages.

        Args:
            contents (List): The list of contents to convert into messages.
            is_system (bool, optional): Indicates whether the contents are system messages. Defaults to False.

        Returns:
            List: The list of messages, where each message is a dictionary with 'role' and 'content' keys.
        """
        content_len = len(contents)
        contents = iter(contents)
        messages = []

        if is_system:
            assert (
                content_len % 2 == 0
            ), "The contents sent to LLM should end with a user message. With system messages, the length should be even. Current length: {}".format(
                content_len
            )
            messages.append({"role": "system", "content": next(contents)})
        else:
            assert (
                content_len % 2 == 1
            ), "The contents sent to LLM should end with a user message. Without system messages, the length should be odd. Current length: {}".format(
                content_len
            )

        is_usr = True
        for content in contents:
            if is_usr:
                messages.append({"role": "user", "content": content})
            else:
                messages.append({"role": "assistant", "content": content})
            is_usr = not is_usr

        return messages
