"""
Local Integrations Service Manager - Run local integrations service connected to remote GMS.

This module allows you to:
- Start a local integrations service connected to remote GMS
- Manage the service lifecycle (start/stop/restart)
- Check service health and status
- Configure GMS connection details

This is useful when you want to:
- Debug integrations service locally while using production/staging GMS
- Test changes to integrations service without deploying
- Avoid kubectl port-forwarding complexities
"""

import os
import signal
import subprocess
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Optional

import httpx
from loguru import logger


class ServiceStatus(str, Enum):
    """Integration service status."""

    RUNNING = "running"
    STOPPED = "stopped"
    STARTING = "starting"
    ERROR = "error"


@dataclass
class IntegrationsServiceConfig:
    """Configuration for local integrations service."""

    gms_url: str
    gms_token: str
    service_port: int = 9003
    log_level: str = "INFO"
    aws_region: Optional[str] = None
    aws_profile: Optional[str] = None

    def to_env_dict(self) -> dict[str, str]:
        """Convert config to environment variables for the service."""
        env = os.environ.copy()

        # Use the GMS URL as-is - DataHub client expects the full GMS URL
        # For Acryl Cloud: https://instance.acryl.io/api/gms
        # For OSS: http://localhost:8080/gms
        env.update(
            {
                "DATAHUB_GMS_URL": self.gms_url,
                "DATAHUB_GMS_API_TOKEN": self.gms_token,
                "LOG_LEVEL": self.log_level,
                # Required for Kafka/actions system
                "KAFKA_AUTOMATIONS_CONSUMER_GROUP_PREFIX": "local-dev",
            }
        )

        if self.aws_region:
            env["AWS_REGION"] = self.aws_region
        if self.aws_profile:
            env["AWS_PROFILE"] = self.aws_profile

        return env


@dataclass
class ServiceProcess:
    """Information about running service process."""

    process: subprocess.Popen
    pid: int
    port: int
    config: IntegrationsServiceConfig
    started_at: float
    log_file: Optional[Path] = None


class LocalIntegrationsManager:
    """Manager for local integrations service."""

    def __init__(
        self,
        service_root: Optional[Path] = None,
        venv_path: Optional[Path] = None,
    ):
        """
        Initialize local integrations manager.

        Args:
            service_root: Root directory of integrations service (default: auto-detect)
            venv_path: Path to Python venv (default: auto-detect)
        """
        self.service_root = service_root or self._find_service_root()
        self.venv_path = venv_path or self._find_venv()
        self._verify_setup()

    def _find_service_root(self) -> Path:
        """Auto-detect integrations service root directory."""
        # Start from current file and walk up
        current = Path(__file__).resolve()
        for parent in current.parents:
            if (parent / "src" / "datahub_integrations" / "server.py").exists():
                return parent

        raise RuntimeError(
            "Could not find integrations service root. "
            "Please run from within datahub-integrations-service directory."
        )

    def _find_venv(self) -> Path:
        """Auto-detect Python venv."""
        # Check common venv locations
        candidates = [
            self.service_root / "venv",
            self.service_root / ".venv",
            self.service_root / ".venv-experimental",
        ]

        for candidate in candidates:
            if (candidate / "bin" / "python").exists():
                return candidate

        raise RuntimeError(
            "Could not find Python venv. Please run setup script first:\n"
            f"  cd {self.service_root}\n"
            "  ../gradlew installDev"
        )

    def _verify_setup(self) -> None:
        """Verify service setup is correct."""
        if not self.service_root.exists():
            raise RuntimeError(f"Service root not found: {self.service_root}")

        if not self.venv_path.exists():
            raise RuntimeError(f"Venv not found: {self.venv_path}")

        server_path = self.service_root / "src" / "datahub_integrations" / "server.py"
        if not server_path.exists():
            raise RuntimeError(f"Server file not found: {server_path}")

        logger.debug(f"Service root: {self.service_root}")
        logger.debug(f"Venv path: {self.venv_path}")

    def start_service(
        self,
        config: IntegrationsServiceConfig,
        log_file: Optional[Path] = None,
        wait_for_ready: bool = True,
    ) -> ServiceProcess:
        """
        Start local integrations service.

        Args:
            config: Service configuration
            log_file: Optional path to log file (default: temp file)
            wait_for_ready: Wait for service to be ready before returning

        Returns:
            ServiceProcess object

        Raises:
            RuntimeError: If service fails to start
        """
        # Check if port is already in use
        if self._is_port_in_use(config.service_port):
            raise RuntimeError(
                f"Port {config.service_port} is already in use. "
                "Stop existing service first or use a different port."
            )

        # Setup log file
        if log_file is None:
            log_file = Path("/tmp") / f"integrations-service-{config.service_port}.log"

        log_file.parent.mkdir(parents=True, exist_ok=True)

        # Prepare environment
        env = config.to_env_dict()

        # Build uvicorn command
        python_bin = self.venv_path / "bin" / "python"
        cmd = [
            str(python_bin),
            "-m",
            "uvicorn",
            "datahub_integrations.server:app",
            "--host",
            "0.0.0.0",
            "--port",
            str(config.service_port),
            "--log-level",
            config.log_level.lower(),
        ]

        logger.info(f"Starting integrations service on port {config.service_port}")
        logger.debug(f"Command: {' '.join(cmd)}")
        logger.debug(f"GMS: {config.gms_url}")
        logger.debug(f"Log file: {log_file}")

        # Debug: Show what env vars will be set
        env_dict = config.to_env_dict()
        logger.debug(f"DATAHUB_GMS_URL env var: {env_dict.get('DATAHUB_GMS_URL')}")
        logger.debug(f"Token present: {bool(env_dict.get('DATAHUB_GMS_API_TOKEN'))}")
        logger.debug(f"Token length: {len(env_dict.get('DATAHUB_GMS_API_TOKEN', ''))}")

        # Start process
        try:
            with open(log_file, "w") as log_fh:
                process = subprocess.Popen(
                    cmd,
                    cwd=self.service_root,
                    env=env,
                    stdout=log_fh,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,  # Detach from parent
                )

            service_process = ServiceProcess(
                process=process,
                pid=process.pid,
                port=config.service_port,
                config=config,
                started_at=time.time(),
                log_file=log_file,
            )

            # Wait for service to be ready
            if wait_for_ready:
                logger.info("Waiting for service to be ready...")
                if not self._wait_for_ready(config.service_port, timeout=30):
                    self.stop_service(service_process)
                    raise RuntimeError(
                        f"Service failed to start within timeout. Check logs: {log_file}"
                    )

            logger.success(f"✓ Service started (PID: {process.pid})")
            logger.info(f"  URL: http://localhost:{config.service_port}")
            logger.info(f"  Logs: {log_file}")

            return service_process

        except Exception as e:
            logger.error(f"Failed to start service: {e}")
            raise

    def stop_service(
        self, service_process: ServiceProcess, timeout: int = 10
    ) -> bool:
        """
        Stop integrations service.

        Args:
            service_process: ServiceProcess to stop
            timeout: Timeout for graceful shutdown

        Returns:
            True if stopped successfully
        """
        try:
            if service_process.process.poll() is not None:
                logger.debug("Service already stopped")
                return True

            logger.info(f"Stopping service (PID: {service_process.pid})")

            # Send SIGTERM to the process group
            try:
                os.killpg(os.getpgid(service_process.process.pid), signal.SIGTERM)
            except ProcessLookupError:
                # Process already dead
                return True

            # Wait for graceful shutdown
            try:
                service_process.process.wait(timeout=timeout)
                logger.success("✓ Service stopped gracefully")
                return True
            except subprocess.TimeoutExpired:
                logger.warning("Service didn't stop gracefully, force killing...")
                try:
                    os.killpg(os.getpgid(service_process.process.pid), signal.SIGKILL)
                    service_process.process.wait(timeout=2)
                    logger.success("✓ Service force stopped")
                    return True
                except Exception as e:
                    logger.error(f"Failed to force stop service: {e}")
                    return False

        except Exception as e:
            logger.error(f"Error stopping service: {e}")
            return False

    def restart_service(self, service_process: ServiceProcess) -> ServiceProcess:
        """
        Restart integrations service.

        Args:
            service_process: Existing ServiceProcess

        Returns:
            New ServiceProcess
        """
        logger.info("Restarting service...")
        self.stop_service(service_process)
        time.sleep(2)
        return self.start_service(
            service_process.config, log_file=service_process.log_file
        )

    def get_service_status(self, port: int) -> ServiceStatus:
        """
        Get status of service running on port.

        Args:
            port: Service port

        Returns:
            ServiceStatus
        """
        if not self._is_port_in_use(port):
            return ServiceStatus.STOPPED

        # Check if service is responsive
        try:
            with httpx.Client(timeout=2) as client:
                response = client.get(f"http://localhost:{port}/ping")
                if response.status_code == 200:
                    return ServiceStatus.RUNNING
                else:
                    return ServiceStatus.ERROR
        except httpx.ConnectError:
            return ServiceStatus.STARTING
        except Exception:
            return ServiceStatus.ERROR

    def _wait_for_ready(self, port: int, timeout: int = 30) -> bool:
        """
        Wait for service to be ready.

        Args:
            port: Service port
            timeout: Max wait time in seconds

        Returns:
            True if service became ready, False otherwise
        """
        start_time = time.time()
        last_error = None

        while time.time() - start_time < timeout:
            try:
                with httpx.Client(timeout=2) as client:
                    response = client.get(f"http://localhost:{port}/ping")
                    if response.status_code == 200:
                        elapsed = time.time() - start_time
                        logger.debug(f"Service ready after {elapsed:.1f}s")
                        return True
            except Exception as e:
                last_error = str(e)

            time.sleep(1)

        logger.error(f"Service not ready after {timeout}s. Last error: {last_error}")
        return False

    def _is_port_in_use(self, port: int) -> bool:
        """Check if port is in use."""
        import socket

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("localhost", port))
                return False
        except OSError:
            return True

    def check_service_health(self, port: int) -> dict:
        """
        Check service health and get info.

        Args:
            port: Service port

        Returns:
            Health info dict
        """
        try:
            with httpx.Client(timeout=5) as client:
                response = client.get(f"http://localhost:{port}/ping")
                if response.status_code == 200:
                    return {
                        "status": "healthy",
                        "response_time_ms": response.elapsed.total_seconds() * 1000,
                        "data": response.json(),
                    }
                else:
                    return {
                        "status": "unhealthy",
                        "error": f"HTTP {response.status_code}",
                    }
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def find_running_service(self) -> Optional[int]:
        """
        Find if any integrations service is running locally.

        Returns:
            Port number if found, None otherwise
        """
        # Check common ports
        for port in [9003, 9004, 9005]:
            status = self.get_service_status(port)
            if status == ServiceStatus.RUNNING:
                return port
        return None
