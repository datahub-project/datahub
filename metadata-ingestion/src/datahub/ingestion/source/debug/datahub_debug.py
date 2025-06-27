import logging
import socket
import time
from typing import Iterable, Optional
from urllib.parse import urlparse

import dns.exception
import dns.resolver
import requests

from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit

logger = logging.getLogger(__name__)


class DataHubDebugSourceConfig(ConfigModel):
    dns_probe_url: Optional[str] = None


@platform_name("DataHubDebug")
@config_class(DataHubDebugSourceConfig)
@support_status(SupportStatus.TESTING)
class DataHubDebugSource(Source):
    """
    DataHubDebugSource is helper to debug things in executor where ingestion is running.

    This source can perform the following tasks:
    1. Network probe of a URL. Different from test connection in sources as that is after source starts.

    """

    def __init__(self, ctx: PipelineContext, config: DataHubDebugSourceConfig):
        self.ctx = ctx
        self.config = config
        self.report = SourceReport()
        self.report.event_not_produced_warn = False

    @classmethod
    def create(cls, config_dict, ctx):
        config = DataHubDebugSourceConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def perform_dns_probe(self, url: str) -> None:
        """
        Perform comprehensive DNS probe and network connectivity tests.
        Logs detailed information to help diagnose network issues.
        """
        logger.info(f"Starting DNS probe for URL: {url}")
        logger.info("=" * 60)
        logger.info(f"DNS PROBE REPORT FOR: {url}")
        logger.info("=" * 60)

        try:
            # Parse the URL to extract hostname
            parsed_url = urlparse(
                url if url.startswith(("http://", "https://")) else f"http://{url}"
            )
            hostname = parsed_url.hostname or parsed_url.netloc
            port = parsed_url.port or (443 if parsed_url.scheme == "https" else 80)

            logger.info(f"Parsed hostname: {hostname}")
            logger.info(f"Target port: {port}")
            logger.info(f"URL scheme: {parsed_url.scheme}")
            logger.info("-" * 60)

            # Test 1: Enhanced DNS resolution with dnspython if available
            logger.info("1. DNS RESOLUTION TEST")
            self._dns_probe_with_dnspython(hostname)

            logger.info("-" * 60)

            # Test 2: HTTP/HTTPS connectivity test with requests if available
            logger.info("2. HTTP CONNECTIVITY TEST")
            self._http_probe_with_requests(url)

            logger.info("-" * 60)

            # Test 3: System network information
            logger.info("3. SYSTEM NETWORK INFORMATION")
            self._log_system_network_info()

        except Exception as e:
            logger.error(f"DNS probe failed with unexpected error: {e}", exc_info=True)

        logger.info("=" * 60)
        logger.info("DNS PROBE COMPLETED")
        logger.info("=" * 60)

    def _dns_probe_with_dnspython(self, hostname: str) -> None:
        """Enhanced DNS probing using dnspython library"""
        try:
            # Test different record types
            record_types = ["A", "AAAA", "CNAME", "MX"]

            for record_type in record_types:
                try:
                    start_time = time.time()
                    answers = dns.resolver.resolve(hostname, record_type)
                    dns_time = time.time() - start_time

                    logger.info(
                        f"✓ {record_type} record resolution successful ({dns_time:.3f}s)"
                    )
                    for answer in answers:
                        logger.info(f"  - {record_type}: {answer}")

                except dns.resolver.NXDOMAIN:
                    logger.info(f"✗ {record_type} record: Domain does not exist")
                except dns.resolver.NoAnswer:
                    logger.info(
                        f"- {record_type} record: No answer (record type not available)"
                    )
                except dns.exception.Timeout:
                    logger.error(f"✗ {record_type} record: DNS query timed out")
                except Exception as e:
                    logger.error(f"✗ {record_type} record query failed: {e}")

            # Test different DNS servers
            logger.info("Testing with different DNS servers:")
            dns_servers = ["8.8.8.8", "1.1.1.1", "208.67.222.222"]

            for dns_server in dns_servers:
                try:
                    resolver = dns.resolver.Resolver()
                    resolver.nameservers = [dns_server]
                    resolver.timeout = 5

                    start_time = time.time()
                    answers = resolver.resolve(hostname, "A")
                    dns_time = time.time() - start_time

                    logger.info(
                        f"✓ DNS server {dns_server} responded ({dns_time:.3f}s)"
                    )
                    for answer in answers:
                        logger.info(f"  - A: {answer}")

                except Exception as e:
                    logger.error(f"✗ DNS server {dns_server} failed: {e}")

        except Exception as e:
            logger.error(f"Enhanced DNS probe failed: {e}", exc_info=True)

    def _http_probe_with_requests(self, url: str) -> None:
        """HTTP connectivity test using requests library"""
        try:
            # Test with different timeouts and methods
            timeout = 10
            allow_redirects_head = True
            allow_redirects_get = False

            # Test HEAD request
            try:
                logger.info(f"Testing HEAD request with timeout {timeout}s")
                start_time = time.time()

                response = requests.head(
                    url, timeout=timeout, allow_redirects=allow_redirects_head
                )

                request_time = time.time() - start_time

                logger.info(f"✓ HEAD request successful ({request_time:.3f}s)")
                logger.info(f"  Status code: {response.status_code}")
                logger.info(
                    f"  Response headers: {dict(list(response.headers.items())[:5])}"
                )

                if hasattr(response, "url") and response.url != url:
                    logger.info(f"  Final URL after redirects: {response.url}")

            except requests.exceptions.Timeout:
                logger.error(f"✗ HEAD request timed out after {timeout}s")
            except requests.exceptions.ConnectionError as e:
                logger.error(f"✗ HEAD connection error: {e}")
            except requests.exceptions.RequestException as e:
                logger.error(f"✗ HEAD request failed: {e}")
            except Exception as e:
                logger.error(f"✗ HEAD unexpected error: {e}")

            # Test GET request
            try:
                logger.info(f"Testing GET request with timeout {timeout}s")
                start_time = time.time()

                response = requests.get(
                    url, timeout=timeout, allow_redirects=allow_redirects_get
                )

                request_time = time.time() - start_time

                logger.info(f"✓ GET request successful ({request_time:.3f}s)")
                logger.info(f"  Status code: {response.status_code}")
                logger.info(
                    f"  Response headers: {dict(list(response.headers.items())[:5])}"
                )

                if hasattr(response, "url") and response.url != url:
                    logger.info(f"  Final URL after redirects: {response.url}")

            except requests.exceptions.Timeout:
                logger.error(f"✗ GET request timed out after {timeout}s")
            except requests.exceptions.ConnectionError as e:
                logger.error(f"✗ GET connection error: {e}")
            except requests.exceptions.RequestException as e:
                logger.error(f"✗ GET request failed: {e}")
            except Exception as e:
                logger.error(f"✗ GET unexpected error: {e}")

        except Exception as e:
            logger.error(f"HTTP probe failed: {e}", exc_info=True)

    def _log_dns_troubleshooting(self) -> None:
        """Log DNS troubleshooting information"""
        logger.info("DNS TROUBLESHOOTING SUGGESTIONS:")
        logger.info("- Check if the hostname is correct")
        logger.info("- Verify DNS server configuration")
        logger.info("- Check network connectivity")
        logger.info("- Try using a different DNS server (8.8.8.8, 1.1.1.1)")
        logger.info("- Check if there are firewall restrictions")

    def _log_system_network_info(self) -> None:
        """Log system network configuration information"""
        try:
            local_hostname = socket.gethostname()
            logger.info(f"Local hostname: {local_hostname}")

            try:
                local_ips = socket.getaddrinfo(local_hostname, None)
                logger.info("Local IP addresses:")
                for addr_info in local_ips:
                    if addr_info[0] in [socket.AF_INET, socket.AF_INET6]:
                        family = "IPv4" if addr_info[0] == socket.AF_INET else "IPv6"
                        logger.info(f"  - {addr_info[4][0]} ({family})")
            except Exception as e:
                logger.warning(f"Could not retrieve local IP addresses: {e}")

            logger.info("DNS Server Connectivity:")
            dns_servers = ["8.8.8.8", "1.1.1.1", "208.67.222.222"]
            for dns_server in dns_servers:
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(5)
                    result = sock.connect_ex((dns_server, 53))
                    if result == 0:
                        logger.info(f"  ✓ Can reach {dns_server}:53")
                    else:
                        logger.error(f"  ✗ Cannot reach {dns_server}:53")
                    sock.close()
                except Exception as e:
                    logger.error(f"  ✗ Error testing {dns_server}:53 - {e}")

        except Exception as e:
            logger.warning(f"Could not gather system network info: {e}")

    def _test_alternative_dns(self, hostname: str) -> None:
        """Test hostname resolution using alternative methods"""
        try:
            families = [(socket.AF_INET, "IPv4"), (socket.AF_INET6, "IPv6")]

            for family, family_name in families:
                try:
                    result = socket.getaddrinfo(hostname, None, family)
                    if result:
                        logger.info(f"✓ {family_name} resolution successful:")
                        for addr_info in result[:3]:
                            logger.info(f"  - {addr_info[4][0]}")
                    else:
                        logger.warning(
                            f"✗ {family_name} resolution returned no results"
                        )
                except socket.gaierror:
                    logger.error(f"✗ {family_name} resolution failed")
                except Exception as e:
                    logger.error(f"✗ {family_name} resolution error: {e}")

        except Exception as e:
            logger.error(f"Alternative DNS test failed: {e}")

    def get_workunits_internal(
        self,
    ) -> Iterable[MetadataWorkUnit]:
        if self.config.dns_probe_url is not None:
            # Perform DNS probe
            logger.info(f"Performing DNS probe for: {self.config.dns_probe_url}")
            self.perform_dns_probe(self.config.dns_probe_url)

        yield from []

    def get_report(self) -> SourceReport:
        return self.report
