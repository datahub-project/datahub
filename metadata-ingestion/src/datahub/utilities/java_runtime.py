import logging
import os
import re
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)

# Debian/Ubuntu (ca-certificates-java) and Wolfi (java-cacerts) both materialize a
# Java-format trust store derived from the OS CA bundle at this path.
SYSTEM_JAVA_TRUST_STORE = Path("/etc/ssl/certs/java/cacerts")

# JPype can no longer start pre-11 JVMs, but its finder only validates the
# architecture of a candidate libjvm, not its version. A system JVM older than
# this must not win over jdk4py (a modern JDK), or startJVM fails on hosts that
# previously worked through the jdk4py fallback.
MINIMUM_SYSTEM_JAVA = 11


def _jvm_major_version(libjvm_path: Path) -> Optional[int]:
    """Read the Java major version from the ``release`` file of the JVM home.

    Every JDK/JRE since 9 ships ``<java home>/release`` with a JAVA_VERSION
    line; the home is a few directories above libjvm (the exact depth varies by
    platform and vendor layout). Returns None when no parseable release file is
    found — e.g. for Java 8 JREs, which is exactly the vintage that must not be
    treated as usable.
    """
    for parent in list(libjvm_path.parents)[:5]:
        release = parent / "release"
        if release.is_file():
            try:
                content = release.read_text()
            except OSError:
                return None
            match = re.search(r'^JAVA_VERSION="(?:1\.)?(\d+)', content, re.MULTILINE)
            if match:
                return int(match.group(1))
    return None


def _usable_system_jvm_discoverable() -> bool:
    import jpype  # type: ignore[import-untyped]

    try:
        jvm_path = Path(jpype.getDefaultJVMPath())
    except Exception:
        return False
    version = _jvm_major_version(jvm_path)
    if version is None or version < MINIMUM_SYSTEM_JAVA:
        logger.info(
            "Ignoring system JVM at %s (version %s, need >= %s)",
            jvm_path,
            version,
            MINIMUM_SYSTEM_JAVA,
        )
        return False
    return True


def _jvm_is_jdk4py() -> bool:
    try:
        from jdk4py import JAVA_HOME as JDK4PY_JAVA_HOME
    except ImportError:
        return False

    import jpype  # type: ignore[import-untyped]

    try:
        jvm_path = Path(jpype.getDefaultJVMPath()).resolve()
    except Exception:
        return False
    return Path(str(JDK4PY_JAVA_HOME)).resolve() in jvm_path.parents


def prepare_jvm_settings() -> List[str]:
    """Make a JVM discoverable for JPype, preferring a system JVM over jdk4py.

    jdk4py bundles its own ``cacerts`` trust store, which only changes when a new
    jdk4py release ships. No release so far has dropped the emSign/eMudhra roots
    whose intermediate CA keys were compromised in October 2026 (the incident
    behind certifi's GHSA-6q48-6c7w-9gw2), so a JVM running off jdk4py trusts
    certificate chains the rest of the environment rejects. A system JVM's trust
    store is kept current by OS package updates, so it must win whenever a
    JPype-startable one (Java >= 11) is discoverable; jdk4py remains the
    fallback for hosts with no usable Java installed.

    Returns extra JVM arguments to pass to ``jpype.startJVM``. When jdk4py's
    runtime is the JVM in use and the OS provides a Java-format trust store
    derived from the system CA bundle, the returned arguments point TLS trust
    at that store instead of jdk4py's bundled one.
    """
    if "JAVA_HOME" not in os.environ and not _usable_system_jvm_discoverable():
        try:
            from jdk4py import JAVA_HOME as JDK4PY_JAVA_HOME
        except ImportError:
            # No system JVM and no jdk4py: let jpype.startJVM raise its own
            # descriptive JVMNotFoundException.
            return []
        os.environ["JAVA_HOME"] = str(JDK4PY_JAVA_HOME)

    if _jvm_is_jdk4py():
        if SYSTEM_JAVA_TRUST_STORE.is_file():
            logger.info(
                "Using jdk4py's bundled JVM with the system Java trust store %s",
                SYSTEM_JAVA_TRUST_STORE,
            )
            return [f"-Djavax.net.ssl.trustStore={SYSTEM_JAVA_TRUST_STORE}"]
        logger.warning(
            "No usable system JVM found; using jdk4py's bundled JVM and trust "
            "store. Its CA set is frozen at the jdk4py release and may trust "
            "roots that have since been distrusted. Install a system JDK/JRE "
            "or set JAVA_HOME to use an OS-managed trust store."
        )
    return []
