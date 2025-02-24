import gzip
import http
import http.server
import logging
import urllib.parse
from threading import Event, Thread
from typing import Any, Callable, Dict, List, Optional

import prometheus_client
import requests

from datahub_executor.common.identity.base import DATAHUB_EXECUTOR_IDENTITY
from datahub_executor.config import (
    DATAHUB_EXECUTOR_INTERNAL_WORKER,
    DATAHUB_EXECUTOR_METRICS_INTERVAL,
    DATAHUB_EXECUTOR_METRICS_PATH,
    DATAHUB_EXECUTOR_METRICS_PORT,
    DATAHUB_EXECUTOR_METRICS_PUSH_DISABLED,
    DATAHUB_EXECUTOR_MODE,
    DATAHUB_GMS_TOKEN,
    DATAHUB_GMS_URL,
)

from .process_collector import DatahubProcessCollector
from .registry import REGISTRY

logger = logging.getLogger(__name__)


class DummyMetric:
    def __init__(self) -> None:
        pass

    def catchall(self, *args: Any, **kwargs: Any) -> None:
        return None

    def time(self) -> Callable:
        def outer(fn: Callable) -> Callable:
            def inner(*args: Any, **kwargs: Any) -> Any:
                return fn(*args, **kwargs)

            return inner

        return outer

    def __getattr__(self, attr: str) -> Callable:
        return self.catchall


class DatahubExecutorMetrics:
    def __init__(
        self,
        namespace: Optional[str] = None,
        common_labels: Optional[Dict[str, str]] = None,
        collectors: Optional[List[Callable]] = None,
    ) -> None:
        self.stop_event = Event()
        self.stop_flag = False

        # Legacy datahub-actions package independently exposes prometheus metrics
        # into the default registry. To avoid loosing those metrics we have to also
        # use default registry. Once the use of datahub actions package is deprecated
        # it might be better to create RE-specific registry as follows:
        #   self.registry = prometheus_client.CollectorRegistry()
        self.registry = prometheus_client.REGISTRY

        self.namespace = namespace if namespace else "datahub_executor"
        self.registered_metrics: Dict[str, Any] = {}

        self.loop: Optional[Thread] = None
        self.httpd: Optional[http.server.HTTPServer] = None
        self.httpdt: Optional[Thread] = None

        self.common_labels = common_labels if common_labels is not None else {}

        self.telemetry_url: Optional[str] = None
        self.telemetry_base_url = DATAHUB_GMS_URL
        self.telemetry_token = DATAHUB_GMS_TOKEN

        # Add collectors
        self.add_collectors(collectors)

    def register_metrics(self, metrics: Dict[str, List[Any]]) -> None:
        for name, params in metrics.items():
            full_name = self.namespace + "_" + name.lower()
            labels = list(self.common_labels.keys())
            if len(params) > 2:
                labels += list(params[2])
            self.registered_metrics[name] = params[0](
                full_name, params[1], labelnames=labels, registry=self.registry
            )

    def get(self, name: str, **kwargs: Any) -> Any:
        metric = self.registered_metrics.get(name, None)
        if metric is not None:
            try:
                return metric.labels(**{**self.common_labels, **kwargs})
            except Exception:
                logger.exception(
                    f"Monitoring: failed to get metric {name} from the registry"
                )
        return None

    def _build_telemetry_url(self) -> str:
        url = urllib.parse.urlparse(self.telemetry_base_url)
        identity = urllib.parse.quote(DATAHUB_EXECUTOR_IDENTITY, safe="")
        path = f"{DATAHUB_EXECUTOR_METRICS_PATH}/push/{identity}"
        return url._replace(path=path, fragment="", query="", params="").geturl()

    def add_collectors(self, collectors: Optional[List[Callable]]) -> None:
        if collectors is not None:
            for collector in collectors:
                collector(registry=self.registry, namespace=self.namespace)

    def _push_metrics(self) -> None:
        if self.telemetry_url is None:
            return

        with self.get("MONITORING_PUSH_REQUESTS").time():
            try:
                data = prometheus_client.exposition.generate_latest(self.registry)
                data_compressed = gzip.compress(data)
                headers = {
                    "Content-Encoding": "gzip",
                    "Content-Type": "text/plain",
                }
                if self.telemetry_token is not None:
                    headers["Authorization"] = f"Bearer {self.telemetry_token}"
                requests.post(self.telemetry_url, data=data_compressed, headers=headers)
                payload_size = len(data_compressed)

                self.get("MONITORING_PUSH_PAYLOAD_SIZE").set(payload_size)
                logger.info(
                    f"Monitoring: pushing metrics to the backend; payload len = {payload_size}"
                )
            except Exception:
                self.get("MONITORING_PUSH_ERRORS").inc()
                logger.exception("Monitoring: failed to push metrics to the backend")

    def _loop_handler(self) -> None:
        while not self.stop_flag:
            try:
                self.stop_event.clear()
                self.stop_event.wait(timeout=DATAHUB_EXECUTOR_METRICS_INTERVAL)
                if not self.stop_flag:
                    self._push_metrics()
            except Exception as e:
                logger.error(f"Monitoring: error in monitoring loop: {e}")

    def start(self) -> None:
        logger.info("Monitoring: starting")

        handler = prometheus_client.exposition.MetricsHandler.factory(self.registry)
        listen = ("0.0.0.0", DATAHUB_EXECUTOR_METRICS_PORT)

        if self.httpd is None:
            self.httpd = http.server.HTTPServer(listen, handler)
            self.httpdt = Thread(target=self.httpd.serve_forever)
            self.httpdt.daemon = True
            self.httpdt.start()

        # In internal deployments, metrics are scraped by prometheus directly from the pod.
        if (
            not DATAHUB_EXECUTOR_METRICS_PUSH_DISABLED
            and not DATAHUB_EXECUTOR_INTERNAL_WORKER
            and self.telemetry_base_url is not None
        ):
            self.telemetry_url = self._build_telemetry_url()
            if self.loop is None:
                self.loop = Thread(target=self._loop_handler)
                self.loop.start()

    def stop(self) -> None:
        logger.info("Monitoring: shutting down")

        if self.stop_flag:
            return

        self.stop_flag = True
        self.stop_event.set()

        if self.loop is not None:
            self.loop.join()
            self.loop = None

        if self.httpd is not None:
            self.httpd.shutdown()
            self.httpd.server_close()
            self.httpd = None

        if self.httpdt is not None:
            self.httpdt.join()
            self.httpdt = None


collectors: List[Callable] = [DatahubProcessCollector]
common_labels = {
    "executor_internal": str(DATAHUB_EXECUTOR_INTERNAL_WORKER),
    "executor_mode": DATAHUB_EXECUTOR_MODE,
}

METRICS = DatahubExecutorMetrics(
    common_labels=common_labels,
    collectors=collectors,
)
METRICS.register_metrics(REGISTRY)


# Example METRIC('metrics_name', label1: 'val1', label2: 'val2', ...).inc()
def METRIC(name: str, **kwargs: Any) -> Any:
    global METRICS
    metric = None
    if METRICS is not None:
        metric = METRICS.get(name, **kwargs)
    if metric is None:
        logger.error(f"Monitoring: metric {name} is not available in the registry.")
        return DummyMetric()
    return metric


def monitoring_start() -> None:
    global METRICS
    if METRICS is not None:
        METRICS.start()


def monitoring_stop() -> None:
    global METRICS
    if METRICS is not None:
        METRICS.stop()
