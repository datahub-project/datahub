"""
Kubernetes kubectl Manager - Reusable module for kubectl operations.

This module provides a clean Python interface for common kubectl operations:
- Get contexts, namespaces, and pods
- Start/stop port forwarding
- Check connection health
- Manage kubectl process lifecycle

Can be used by any tool that needs to interact with Kubernetes clusters.
"""

import json
import os
import subprocess
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import List, Optional

import httpx
from loguru import logger


class PodStatus(str, Enum):
    """Pod status enum."""

    RUNNING = "Running"
    PENDING = "Pending"
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"
    UNKNOWN = "Unknown"


class ConnectionStatus(str, Enum):
    """Connection status enum."""

    CONNECTED = "connected"
    CONNECTING = "connecting"
    DISCONNECTED = "disconnected"
    ERROR = "error"


@dataclass
class PodInfo:
    """Information about a Kubernetes pod."""

    name: str
    namespace: str
    status: PodStatus
    labels: dict[str, str]
    ready: bool
    age: str


@dataclass
class PortForwardProcess:
    """Information about an active port-forward process."""

    process: subprocess.Popen
    pod_name: str
    namespace: str
    local_port: int
    remote_port: int
    pid: int
    started_at: float


@dataclass
class HealthCheckResult:
    """Result of a connection health check."""

    status: ConnectionStatus
    url: str
    response_time_ms: Optional[float] = None
    error_message: Optional[str] = None


class KubectlManager:
    """Manager for kubectl operations."""

    # Common label selectors for DataHub integrations service
    DEFAULT_LABEL_SELECTORS = [
        "app=datahub-integrations",
        "app.kubernetes.io/name=datahub-integrations",
        "app.kubernetes.io/name=acryl-datahub-integrations",  # Acryl-specific variant
        "component=integrations-service",
    ]

    def __init__(self, kubectl_path: str = "kubectl"):
        """
        Initialize kubectl manager.

        Args:
            kubectl_path: Path to kubectl binary (default: "kubectl" from PATH)
        """
        self.kubectl_path = kubectl_path
        self._verify_kubectl()

    def _verify_kubectl(self) -> None:
        """Verify kubectl is installed and accessible."""
        try:
            result = subprocess.run(
                [self.kubectl_path, "version", "--client", "--output=json"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode != 0:
                raise RuntimeError("kubectl not found or not working")
            logger.debug("kubectl verified successfully")
        except (subprocess.TimeoutExpired, FileNotFoundError) as e:
            raise RuntimeError(
                f"kubectl not found. Please install kubectl: https://kubernetes.io/docs/tasks/tools/"
            ) from e

    def get_contexts(self) -> List[str]:
        """
        Get list of available kubectl contexts.

        Returns:
            List of context names

        Raises:
            RuntimeError: If kubectl command fails
        """
        try:
            result = subprocess.run(
                [self.kubectl_path, "config", "get-contexts", "-o", "name"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode != 0:
                raise RuntimeError(f"Failed to get contexts: {result.stderr}")

            contexts = [line.strip() for line in result.stdout.split("\n") if line.strip()]
            logger.debug(f"Found {len(contexts)} kubectl contexts")
            return contexts
        except subprocess.TimeoutExpired as e:
            raise RuntimeError(
                "kubectl get-contexts timed out - This usually means you're not connected to VPN. "
                "Please connect to VPN and try again."
            ) from e

    def get_current_context(self) -> Optional[str]:
        """
        Get the current kubectl context.

        Returns:
            Current context name or None if not set
        """
        try:
            result = subprocess.run(
                [self.kubectl_path, "config", "current-context"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0:
                context = result.stdout.strip()
                logger.debug(f"Current context: {context}")
                return context
            return None
        except (subprocess.TimeoutExpired, Exception) as e:
            logger.warning(f"Failed to get current context: {e}")
            return None

    def get_namespaces(self, context: Optional[str] = None) -> List[str]:
        """
        Get list of namespaces in the cluster.

        Args:
            context: Optional context to use (default: current context)

        Returns:
            List of namespace names

        Raises:
            RuntimeError: If kubectl command fails
        """
        cmd = [self.kubectl_path, "get", "namespaces", "-o", "jsonpath={.items[*].metadata.name}"]
        if context:
            cmd.extend(["--context", context])

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            if result.returncode != 0:
                raise RuntimeError(f"Failed to get namespaces: {result.stderr}")

            namespaces = [ns.strip() for ns in result.stdout.split() if ns.strip()]
            logger.debug(f"Found {len(namespaces)} namespaces")
            return namespaces
        except subprocess.TimeoutExpired as e:
            raise RuntimeError(
                "kubectl get namespaces timed out - This usually means you're not connected to VPN. "
                "Please connect to VPN and try again."
            ) from e

    def get_pods(
        self,
        namespace: str,
        label_selector: Optional[str] = None,
        context: Optional[str] = None,
    ) -> List[PodInfo]:
        """
        Get list of pods in a namespace.

        Args:
            namespace: Namespace to query
            label_selector: Optional label selector (e.g., "app=myapp")
            context: Optional context to use

        Returns:
            List of PodInfo objects

        Raises:
            RuntimeError: If kubectl command fails
        """
        cmd = [self.kubectl_path, "get", "pods", "-n", namespace, "-o", "json"]
        if label_selector:
            cmd.extend(["--selector", label_selector])
        if context:
            cmd.extend(["--context", context])

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            if result.returncode != 0:
                raise RuntimeError(f"Failed to get pods: {result.stderr}")

            data = json.loads(result.stdout)
            pods = []

            for item in data.get("items", []):
                metadata = item.get("metadata", {})
                status = item.get("status", {})

                # Check if pod is ready
                conditions = status.get("conditions", [])
                ready = any(c.get("type") == "Ready" and c.get("status") == "True" for c in conditions)

                pod_info = PodInfo(
                    name=metadata.get("name", ""),
                    namespace=metadata.get("namespace", namespace),
                    status=PodStatus(status.get("phase", "Unknown")),
                    labels=metadata.get("labels", {}),
                    ready=ready,
                    age=metadata.get("creationTimestamp", ""),
                )
                pods.append(pod_info)

            logger.debug(f"Found {len(pods)} pods in namespace {namespace}")
            return pods

        except subprocess.TimeoutExpired as e:
            raise RuntimeError(
                "kubectl get pods timed out - This usually means you're not connected to VPN. "
                "Please connect to VPN and try again."
            ) from e
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Failed to parse pod list: {e}") from e

    def find_integrations_service_pods(
        self, namespace: str, context: Optional[str] = None
    ) -> List[PodInfo]:
        """
        Find DataHub integrations service pods using common label patterns.

        Args:
            namespace: Namespace to search
            context: Optional context to use

        Returns:
            List of matching PodInfo objects
        """
        all_pods = []

        for selector in self.DEFAULT_LABEL_SELECTORS:
            try:
                pods = self.get_pods(namespace, label_selector=selector, context=context)
                # Only include Running and Ready pods
                running_pods = [p for p in pods if p.status == PodStatus.RUNNING and p.ready]
                all_pods.extend(running_pods)
            except Exception as e:
                logger.debug(f"No pods found with selector '{selector}': {e}")

        # Deduplicate by name
        seen = set()
        unique_pods = []
        for pod in all_pods:
            if pod.name not in seen:
                seen.add(pod.name)
                unique_pods.append(pod)

        logger.info(f"Found {len(unique_pods)} integrations service pods in {namespace}")
        return unique_pods

    def start_port_forward(
        self,
        pod_name: str,
        namespace: str,
        local_port: int,
        remote_port: int,
        context: Optional[str] = None,
    ) -> PortForwardProcess:
        """
        Start port forwarding to a pod.

        Args:
            pod_name: Name of the pod
            namespace: Namespace of the pod
            local_port: Local port to forward to
            remote_port: Remote port on the pod
            context: Optional context to use

        Returns:
            PortForwardProcess object

        Raises:
            RuntimeError: If port-forward fails to start
        """
        cmd = [
            self.kubectl_path,
            "port-forward",
            "-n",
            namespace,
            pod_name,
            f"{local_port}:{remote_port}",
        ]
        if context:
            cmd.extend(["--context", context])

        try:
            # Start port-forward in background
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )

            # Give it a moment to start
            time.sleep(2)

            # Check if process is still running
            if process.poll() is not None:
                _, stderr = process.communicate()
                raise RuntimeError(f"Port-forward failed to start: {stderr}")

            port_forward = PortForwardProcess(
                process=process,
                pod_name=pod_name,
                namespace=namespace,
                local_port=local_port,
                remote_port=remote_port,
                pid=process.pid,
                started_at=time.time(),
            )

            logger.info(
                f"Port-forward started: localhost:{local_port} -> {pod_name}:{remote_port} (PID: {process.pid})"
            )
            return port_forward

        except Exception as e:
            raise RuntimeError(f"Failed to start port-forward: {e}") from e

    def stop_port_forward(self, port_forward: PortForwardProcess, timeout: int = 5) -> bool:
        """
        Stop a port-forward process.

        Args:
            port_forward: PortForwardProcess to stop
            timeout: Timeout in seconds for graceful shutdown

        Returns:
            True if stopped successfully, False otherwise
        """
        try:
            if port_forward.process.poll() is not None:
                logger.debug("Port-forward process already terminated")
                return True

            # Try graceful shutdown first
            logger.debug(f"Sending SIGTERM to port-forward process (PID: {port_forward.pid})")
            port_forward.process.terminate()

            try:
                port_forward.process.wait(timeout=timeout)
                logger.info(f"Port-forward stopped gracefully (PID: {port_forward.pid})")
                return True
            except subprocess.TimeoutExpired:
                logger.warning(f"Port-forward didn't stop gracefully, sending SIGKILL")
                port_forward.process.kill()
                port_forward.process.wait(timeout=2)
                logger.info(f"Port-forward killed (PID: {port_forward.pid})")
                return True

        except Exception as e:
            logger.error(f"Failed to stop port-forward: {e}")
            return False

    def check_connection(
        self, url: str, timeout: int = 5, verify_ssl: bool = True
    ) -> HealthCheckResult:
        """
        Check if a service is reachable at the given URL.

        Args:
            url: URL to check (e.g., "http://localhost:9003")
            timeout: Request timeout in seconds
            verify_ssl: Whether to verify SSL certificates

        Returns:
            HealthCheckResult with status and timing info
        """
        try:
            start_time = time.time()

            with httpx.Client(verify=verify_ssl, timeout=timeout) as client:
                # Try a simple GET to the root or health endpoint
                response = client.get(url)

            elapsed_ms = (time.time() - start_time) * 1000

            if response.status_code < 500:
                return HealthCheckResult(
                    status=ConnectionStatus.CONNECTED,
                    url=url,
                    response_time_ms=elapsed_ms,
                )
            else:
                return HealthCheckResult(
                    status=ConnectionStatus.ERROR,
                    url=url,
                    response_time_ms=elapsed_ms,
                    error_message=f"Server error: HTTP {response.status_code}",
                )

        except httpx.TimeoutException:
            return HealthCheckResult(
                status=ConnectionStatus.ERROR,
                url=url,
                error_message="Connection timeout",
            )
        except httpx.ConnectError as e:
            return HealthCheckResult(
                status=ConnectionStatus.DISCONNECTED,
                url=url,
                error_message=f"Connection refused: {e}",
            )
        except Exception as e:
            return HealthCheckResult(
                status=ConnectionStatus.ERROR,
                url=url,
                error_message=f"Unexpected error: {e}",
            )

    def is_port_in_use(self, port: int) -> bool:
        """
        Check if a local port is already in use.

        Args:
            port: Port number to check

        Returns:
            True if port is in use, False otherwise
        """
        import socket

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("localhost", port))
                return False
        except OSError:
            return True

    def get_gms_url_from_namespace(
        self, namespace: str, context: Optional[str] = None
    ) -> Optional[str]:
        """
        Auto-discover GMS URL from namespace by checking ingresses and services.

        Args:
            namespace: Namespace to search
            context: Optional context to use

        Returns:
            GMS URL if found, None otherwise
        """
        cmd = [
            self.kubectl_path,
            "get",
            "ingress",
            "-n",
            namespace,
            "-o",
            "json",
        ]
        if context:
            cmd.extend(["--context", context])

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                data = json.loads(result.stdout)
                items = data.get("items", [])

                if items:
                    # Take the first ingress with a host (usually the main one)
                    for item in items:
                        spec = item.get("spec", {})
                        rules = spec.get("rules", [])
                        if rules:
                            host = rules[0].get("host", "")
                            if host:
                                # Use https for ingress
                                gms_url = f"https://{host}/api/gms"
                                logger.info(f"Discovered GMS URL from ingress: {gms_url}")
                                return gms_url
        except Exception as e:
            logger.debug(f"Failed to get ingress: {e}")

        # Fallback: check for LoadBalancer services
        try:
            cmd = [
                self.kubectl_path,
                "get",
                "svc",
                "-n",
                namespace,
                "-o",
                "json",
            ]
            if context:
                cmd.extend(["--context", context])

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                data = json.loads(result.stdout)
                for item in data.get("items", []):
                    metadata = item.get("metadata", {})
                    name = metadata.get("name", "")
                    if "datahub-frontend" in name.lower():
                        spec = item.get("spec", {})
                        if spec.get("type") == "LoadBalancer":
                            status = item.get("status", {})
                            lb = status.get("loadBalancer", {})
                            ingress_list = lb.get("ingress", [])
                            if ingress_list:
                                hostname = ingress_list[0].get("hostname") or ingress_list[0].get("ip")
                                if hostname:
                                    gms_url = f"https://{hostname}/api/gms"
                                    logger.info(f"Discovered GMS URL from LoadBalancer: {gms_url}")
                                    return gms_url
        except Exception as e:
            logger.debug(f"Failed to get services: {e}")

        return None

    def parse_cluster_info_from_context(self, context: str) -> dict[str, str]:
        """
        Parse AWS region, cluster name, and account from kubectl context ARN.

        Args:
            context: Kubectl context (e.g., "arn:aws:eks:us-west-2:...:cluster/usw2-saas-01-staging")

        Returns:
            Dict with 'region', 'cluster', 'cluster_full', and 'account' keys
        """
        # Example: arn:aws:eks:us-west-2:795586375822:cluster/usw2-saas-01-staging
        if context.startswith("arn:aws:eks:"):
            parts = context.split(":")
            if len(parts) >= 6:
                region = parts[3]  # us-west-2
                account = parts[4]  # 795586375822 or 243536687406
                cluster_part = parts[5]  # cluster/usw2-saas-01-staging
                if "/" in cluster_part:
                    cluster_name = cluster_part.split("/")[1]  # usw2-saas-01-staging

                    # Normalize cluster name (e.g., usw2-saas-01-staging -> usw2-staging)
                    # Pattern: region-type-##-env -> region-env
                    cluster_normalized = cluster_name
                    if "-saas-" in cluster_name:
                        # usw2-saas-01-staging -> usw2-staging
                        region_prefix = cluster_name.split("-")[0]  # usw2
                        env = cluster_name.split("-")[-1]  # staging or prod
                        cluster_normalized = f"{region_prefix}-{env}"
                    elif "-trials-" in cluster_name:
                        # Trial clusters: usw2-trials-01-dmz, euc1-trials-01-dmz
                        # No normalization needed - use full name for Parameter Store paths
                        cluster_normalized = cluster_name

                    return {
                        "region": region,
                        "cluster": cluster_normalized,
                        "cluster_full": cluster_name,
                        "account": account,
                    }

        # Fallback defaults
        return {"region": "us-west-2", "cluster": "usw2-staging", "cluster_full": "unknown", "account": "unknown"}

    def detect_aws_profile_for_account(self, account_id: str) -> Optional[str]:
        """
        Automatically detect which AWS profile to use based on account ID.

        Dynamically discovers all AWS profiles and finds one matching the account.

        Args:
            account_id: AWS account ID (e.g., "795586375822" or "243536687406")

        Returns:
            Profile name if found, None otherwise
        """
        # First, try AWS_PROFILE environment variable
        aws_profile = os.environ.get("AWS_PROFILE")
        if aws_profile:
            try:
                result = subprocess.run(
                    ["aws", "sts", "get-caller-identity", "--profile", aws_profile],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                if result.returncode == 0:
                    import json
                    identity = json.loads(result.stdout)
                    if identity["Account"] == account_id:
                        logger.debug(f"Using AWS_PROFILE={aws_profile} for account {account_id}")
                        return aws_profile
            except Exception:
                pass

        # Discover all AWS profiles from config
        try:
            config_path = Path.home() / ".aws" / "config"
            if not config_path.exists():
                logger.debug("No AWS config file found")
                return None

            profiles = []
            with open(config_path) as f:
                for line in f:
                    if line.startswith("[profile "):
                        profile_name = line.strip()[9:-1]  # Extract name from "[profile NAME]"
                        profiles.append(profile_name)

            logger.debug(f"Found {len(profiles)} AWS profiles in config")

            # Try each profile and check if it matches the account
            for profile in profiles:
                try:
                    result = subprocess.run(
                        ["aws", "sts", "get-caller-identity", "--profile", profile],
                        capture_output=True,
                        text=True,
                        timeout=5,
                    )
                    if result.returncode == 0:
                        import json
                        identity = json.loads(result.stdout)
                        if identity["Account"] == account_id:
                            logger.info(f"Auto-detected AWS profile '{profile}' for account {account_id}")
                            return profile
                except Exception as e:
                    logger.debug(f"Profile '{profile}' not authenticated or failed: {e}")
                    continue

        except Exception as e:
            logger.error(f"Failed to discover AWS profiles: {e}")

        logger.warning(f"Could not find authenticated AWS profile for account {account_id}")
        return None

    def get_datahub_token(
        self,
        namespace: str,
        frontend_url: str,
        context: Optional[str] = None,
        profile: Optional[str] = None,
        force_regenerate: bool = False,
    ) -> Optional[str]:
        """
        Get DataHub token by generating it via API (retrieves password from AWS Parameter Store).

        Args:
            namespace: Namespace to get token for
            frontend_url: DataHub frontend URL
            context: Optional context to use
            profile: Optional AWS profile name (uses AWS_PROFILE env or default if not provided)
            force_regenerate: Force generation of a new token even if cached token exists

        Returns:
            Token if successful, None otherwise
        """
        try:
            # Parse cluster info from context
            if not context:
                context = self.get_current_context()

            if not context:
                logger.error("Could not determine kubectl context")
                return None

            cluster_info = self.parse_cluster_info_from_context(context)
            region = cluster_info["region"]
            cluster_full = cluster_info["cluster_full"]
            cluster_short = cluster_info["cluster"]
            account_id = cluster_info["account"]

            logger.info(f"Generating token for namespace={namespace}, region={region}, cluster={cluster_full} (short={cluster_short}), account={account_id}")

            # Auto-detect AWS profile if not provided
            if not profile:
                profile = self.detect_aws_profile_for_account(account_id)
                if not profile:
                    logger.error(f"Could not find AWS profile for account {account_id}. Trial clusters require DMZ account access.")
                    return None

            # Import and use token generator
            from token_generator import TokenGenerator, TokenValidity

            generator = TokenGenerator()

            # Try with full cluster name first
            token = generator.generate_token(
                frontend_url=frontend_url,
                namespace=namespace,
                region=region,
                cluster=cluster_full,
                validity=TokenValidity.ONE_DAY,
                profile=profile,
                force_regenerate=force_regenerate,
            )

            # If that fails, try with short cluster name
            if not token and cluster_short != cluster_full:
                logger.info(f"Retrying token generation with short cluster name: {cluster_short}")
                token = generator.generate_token(
                    frontend_url=frontend_url,
                    namespace=namespace,
                    region=region,
                    cluster=cluster_short,
                    validity=TokenValidity.ONE_DAY,
                    profile=profile,
                    force_regenerate=force_regenerate,
                )

            if token:
                logger.info(f"✓ Generated DataHub token for namespace {namespace}")
                return token
            else:
                logger.error("Failed to generate token")
                return None

        except ImportError:
            logger.error("token_generator module not found")
            return None
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Failed to get token: {e}")

            # Re-raise SSO token expiry errors so UI can handle them
            if "Token has expired" in error_msg or (
                "sso" in error_msg.lower() and "refresh failed" in error_msg.lower()
            ):
                raise

            return None
