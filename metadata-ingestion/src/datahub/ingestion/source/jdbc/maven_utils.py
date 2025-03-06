import logging
import os
import re
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from packaging import version

logger = logging.getLogger(__name__)


class MavenManager:
    is_maven_installed: bool

    def __init__(self):
        self.is_maven_installed = self._check_maven_installed()
        self.maven_home = str(Path.home() / "maven")

    def setup_environment(self):
        """Setup the environment including Maven if needed"""
        if not self._check_maven_installed():
            self._setup_maven()

    def _check_maven_installed(self):
        """Check if Maven is already installed and accessible"""
        try:
            result = subprocess.run(
                args=["mvn", "--version"],
                capture_output=True,
                text=True,
            )

            if result.returncode == 0:
                version_info = result.stdout.split("\n")[0]
                logger.info(f"Maven is already installed: {version_info}")
                return True
            return False

        except FileNotFoundError:
            logger.info("Maven is not installed or not in PATH")
            return False
        except Exception as e:
            logger.error(f"Error checking Maven installation: {e}")
            return False

    def _get_java_version(self):
        """Get the installed Java version"""
        try:
            result = subprocess.run(
                args=["java", "-version"],
                capture_output=True,
                text=True,
                stderr=subprocess.STDOUT,
            )

            if result.returncode == 0:
                version_pattern = r'version "(.*?)"'
                match = re.search(version_pattern, result.stdout)
                if match:
                    version_str = match.group(1)
                    major_version = int(version_str.split(".")[0])
                    if "1.8" in version_str:
                        major_version = 8
                    return major_version
            return None
        except Exception:
            return None

    def _get_latest_maven_url(self):
        """Get the latest Maven version download URL"""
        try:
            base_url = "https://maven.apache.org/download.cgi"
            response = requests.get(base_url)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")
            download_links = soup.find_all(
                name="a", href=re.compile(r".*apache-maven-[\d.]+-bin\.zip$")
            )

            if not download_links:
                raise Exception("No Maven download links found")

            versions = []
            for link in download_links:
                href = link.get("href")
                version_match = re.search(
                    pattern=r"apache-maven-([\d.]+)-bin\.zip", string=href
                )
                if version_match:
                    versions.append((version.parse(version_match.group(1)), href))

            versions.sort(key=lambda x: x[0], reverse=True)
            latest_version_url = versions[0][1]

            if not latest_version_url.startswith("http"):
                if latest_version_url.startswith("//"):
                    latest_version_url = "https:" + latest_version_url
                else:
                    latest_version_url = (
                        "https://dlcdn.apache.org/maven/"
                        + latest_version_url.lstrip("/")
                    )

            return latest_version_url

        except Exception as e:
            logger.error(f"Error finding latest Maven version: {e}")
            return "https://dlcdn.apache.org/maven/maven-3/3.9.6/binaries/apache-maven-3.9.6-bin.zip"

    def _setup_maven(self):
        """Install the latest version of Maven"""
        if not self._check_java_version():
            raise EnvironmentError("Java 11 or higher is required")

        maven_url = self._get_latest_maven_url()
        if not maven_url:
            raise Exception("Failed to determine Maven download URL")

        version_match = re.search(
            pattern=r"apache-maven-([\d.]+)-bin\.zip", string=maven_url
        )
        maven_version = version_match.group(1) if version_match else "unknown"

        try:
            logger.info(f"Downloading Maven {maven_version}...")
            response = requests.get(maven_url, stream=True)
            response.raise_for_status()

            home = str(Path.home())
            zip_path = os.path.join(home, "maven.zip")

            with open(file=zip_path, mode="wb") as f:
                shutil.copyfileobj(response.raw, f)

            logger.info("Extracting Maven...")
            with zipfile.ZipFile(file=zip_path, mode="r") as zip_ref:
                zip_ref.extractall(home)

            extracted_dir = os.path.join(home, f"apache-maven-{maven_version}")
            if os.path.exists(self.maven_home):
                shutil.rmtree(self.maven_home)
            shutil.move(extracted_dir, self.maven_home)

            os.remove(zip_path)

            self._setup_maven_path()

            logger.info(
                f"\nMaven {maven_version} installed successfully in {self.maven_home}"
            )
            return True

        except Exception as e:
            logger.error(f"Error during Maven installation: {e}")
            return False

    def _setup_maven_path(self):
        """Setup Maven PATH based on OS"""
        maven_bin = os.path.join(self.maven_home, "bin")
        if sys.platform == "win32":
            current_path = os.environ.get("PATH", "")
            if maven_bin not in current_path:
                os.system(f'setx PATH "{current_path};{maven_bin}"')
                os.environ["PATH"] = f"{current_path};{maven_bin}"
        else:
            # For Unix-like systems
            bashrc_path = os.path.expanduser("~/.bashrc")
            maven_config = (
                f"\nexport M2_HOME={self.maven_home}\nexport PATH=$M2_HOME/bin:$PATH\n"
            )

            with open(bashrc_path, "a") as f:
                f.write(maven_config)

            # Also set for current session
            os.environ["M2_HOME"] = self.maven_home
            os.environ["PATH"] = f"{self.maven_home}/bin:{os.environ.get('PATH', '')}"

    def _check_java_version(self):
        """Check if Java version is compatible"""
        java_version = self._get_java_version()
        if java_version and java_version >= 11:
            return True
        raise Exception("Java 11 or higher is required")
