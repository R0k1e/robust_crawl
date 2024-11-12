import os
import glob
import concurrent.futures
import socket
import subprocess
import requests
import yaml
import time
import tempfile
import logging
import copy
from tqdm import tqdm
from gevent.fileobject import FileObject

# import geoip2.database #TODO add MMDB to get location
from tenacity import (
    retry,
    stop_after_attempt,
    before_sleep_log,
)

from .singleton_meta import SingletonMeta

logger = logging.getLogger(__name__)


class ProxyCreator(metaclass=SingletonMeta):
    def __init__(self, config=None):
        self.config = config
        self.is_enabled = config.get("is_enabled", False)
        self.proxies_dir = config.get("proxies_dir", None)
        if not self.proxies_dir:
            logger.warning("No proxies provided for starting up proxies, skipping...")
        self.start_port = config.get("start_port", 33333)
        self.location_dict = config.get("locations", {})

        self.port_mapping = {}
        self.proxy_list = []
        self.processes = []
        self.current_proxy_index = 0

    def __del__(self):
        for process in self.processes:
            process.kill()

    def start_proxies(self):
        if not self.is_enabled:
            logger.info("Proxy manager is disabled.")
            self.port_mapping = []
            return

        futures = {}
        port_gen = self._get_avail_port()
        self.port_mapping = self._create_mihomo_proxy(port_gen=port_gen)

        self.port_mapping = self._check_port_mapping_availability(self.port_mapping)

        logger.info(f"{len(self.port_mapping)} port mapping: {self.port_mapping}")
        proxy_pool = []
        for data in self.port_mapping.items():
            data = {
                "name": data[0],
                "http": f"socks5://127.0.0.1:{data[1]}",
                "https": f"socks5://127.0.0.1:{data[1]}",
                "locale": None,  # TODO {latitude, longitude, accuracy}
                "timezone_id": None,  # TODO
                "geolocation": None,  # TODO
            }
            proxy_pool.append(data)
        self.proxy_list.extend(proxy_pool)

    def terminate_processes(self):
        for process in self.processes:
            process.terminate()

    def get_proxies(self):
        return copy.deepcopy(self.proxy_list)

    def _create_mihomo_proxy(self, port_gen):
        def _create_mihomo_listener(proxy, port):
            listener = {
                "name": proxy["name"],
                "type": "mixed",
                "listen": "0.0.0.0",
                "port": port,
                "proxy": proxy["name"],
                "udp": True,
            }
            return listener

        def _update_dns_config(dns, new_dns):
            def _update_list(old_list, new_list):
                for item in new_list:
                    if item not in old_list:
                        old_list.append(item)

            _update_list(
                dns["default-nameserver"], new_dns.get("default-nameserver", [])
            )
            _update_list(dns["nameserver"], new_dns.get("nameserver", []))
            _update_list(dns["fake-ip-filter"], new_dns.get("fake-ip-filter", []))
            _update_list(dns["fallback"], new_dns.get("fallback", []))
            dns["fallback-filter"].update(new_dns.get("fallback-filter", {}))
            return dns

        overall_config = {
            "port": next(port_gen),
            "socks-port": next(port_gen),
            "mixed-port": next(port_gen),
            "mode": "rule",
            "log-level": "warning",
        }

        dns = {
            "enable": True,
            "ipv6": False,
            "default-nameserver": ["223.5.5.5"],
            "enhanced-mode": "fake-ip",
            "nameserver": [],
            "fake-ip-filter": [],
            "fallback": [],
            "fallback-filter": {},
        }
        listeners = []
        proxies = []
        port_mapping = {}
        created_server = []
        for config in self._get_config(self.proxies_dir):
            for proxy in tqdm(config["proxies"], desc=f"Processing proxies"):
                port = next(port_gen)
                if (proxy["server"], proxy["port"]) not in created_server:
                    proxies.append(proxy)
                    listener = _create_mihomo_listener(proxy, port)
                    listeners.append(listener)
                    port_mapping[proxy["name"]] = port
                    created_server.append((proxy["server"], proxy["port"]))
            dns = _update_dns_config(dns, config["dns"])

        proxy_groups = [
            {
                "name": "all_proxies",
                "type": "select",
                "proxies": [proxy["name"] for proxy in proxies],
                "url": "http://www.gstatic.com/generate_204",
                "interval": 300,
                "disable-udp": False,
            }
        ]

        overall_config["dns"] = dns
        overall_config["proxies"] = proxies
        overall_config["proxy-groups"] = proxy_groups
        overall_config["listeners"] = listeners

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml") as temp_config_file:
            temp_config_path = temp_config_file.name
            yaml.dump(
                overall_config,
                temp_config_file,
                default_flow_style=False,
                allow_unicode=True,
            )
            self._start_process(
                [
                    "mihomo",
                    "-f",
                    temp_config_path,
                ]
            )

        time.sleep(1)
        return port_mapping

    def _get_config(self, proxies_dir):
        for config_path in glob.glob(os.path.join(proxies_dir, "*.yml")):
            with FileObject(config_path, "r") as f:
                config = yaml.safe_load(f)
                logger.info(f"Loaded config from {config_path}")
                yield config

    def _check_port_mapping_availability(self, port_mapping):
        futures = {}
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for name, port in tqdm(
                port_mapping.items(), desc="Start checking proxy availability"
            ):
                future = executor.submit(self._check_proxy_availability, name, port)
                futures[future] = name
            logger.info("Waiting for proxy availability check to finish...")
            # 统一检查代理是否可用
            for future in tqdm(
                concurrent.futures.as_completed(futures),
                desc="Get proxy availability check result",
            ):
                proxy_name = futures[future]
                available = future.result()
                if not available:
                    port_mapping.pop(proxy_name)

        return port_mapping

    def _get_avail_port(self):
        port = self.start_port
        while port < 65535:
            while not self._is_port_available(port):
                port += 1
            yield port
            port += 1

    def _is_port_available(self, port):
        try:
            assert isinstance(port, int)
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                return s.connect_ex(("localhost", port)) != 0
        except Exception as e:
            logger.error(f"Error checking port availability: {e}")
            return False

    def _check_proxy_availability(self, proxy_name, port):
        try:
            if self._is_socks_proxy_working(port):
                return True
        except Exception as e:
            logger.error(f"Proxy {proxy_name} is not available: {str(e)}")
            return False

    def _is_socks_proxy_working(self, port):
        proxies = {
            "http": f"socks5://127.0.0.1:{port}",
            "https": f"socks5://127.0.0.1:{port}",
        }
        response = requests.get("https://www.google.com", proxies=proxies, timeout=10)
        if response.status_code == 200:
            return True
        else:
            raise ValueError(f"Status code: {response.status_code}")

    def _start_process(self, cmd):
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.processes.append(process)
        time.sleep(1)
