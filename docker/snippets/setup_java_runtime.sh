#!/bin/sh
# Shared apk-based Java runtime setup for DataHub service images:
#   datahub-gms, datahub-mce-consumer, datahub-mae-consumer, datahub-upgrade, datahub-frontend-react.
# Container JRE major (apk openjdk-*-jre). Bump JAVA_MAJOR below; datahub-actions parses this line too.
# Environment:
#   INSTALL_JATTACH — set to 0 to skip jattach (e.g. datahub-upgrade base)
#   OTEL_JMX_DIR    — directory for OTEL + JMX agent JARs (default /)
#   GITHUB_REPO_URL, MAVEN_CENTRAL_REPO_URL, JMX_VERSION

set -eu

# Must stay on its own line as JAVA_MAJOR=<digits> for datahub-actions Dockerfile parsing.
JAVA_MAJOR=25

retry_command() {
  label="$1"
  shift

  max_retries=3
  retry_num=0

  while :; do
    if "$@"; then
      return 0
    fi

    retry_num=$((retry_num + 1))
    if [ "$retry_num" -gt "$max_retries" ]; then
      echo "setup_java_runtime.sh: ${label} failed after $((max_retries + 1)) attempts" >&2
      return 1
    fi

    sleep_seconds=$((retry_num * 5))
    echo "setup_java_runtime.sh: ${label} failed with a transient error; retry ${retry_num}/${max_retries} in ${sleep_seconds}s" >&2
    sleep "$sleep_seconds"
  done
}

mkdir -p /usr/local/bin

INSTALL_JATTACH="${INSTALL_JATTACH:-1}"
OTEL_JMX_DIR="${OTEL_JMX_DIR:-/}"
GITHUB_REPO_URL="${GITHUB_REPO_URL:-https://github.com}"
MAVEN_CENTRAL_REPO_URL="${MAVEN_CENTRAL_REPO_URL:-https://repo1.maven.org/maven2}"
JMX_VERSION="${JMX_VERSION:-1.0.1}"

JATTACH_TAG=v2.2

case "$JAVA_MAJOR" in '' | *[!0-9]*)
  echo "setup_java_runtime.sh: invalid JAVA_MAJOR=${JAVA_MAJOR}" >&2
  exit 1
  ;;
esac

JAVA_JRE_PKG="openjdk-${JAVA_MAJOR}-jre"
JAVA_VM_HOME="/usr/lib/jvm/java-${JAVA_MAJOR}-openjdk"

retry_command "apk add" apk add --no-cache \
  bash \
  coreutils \
  curl \
  wget \
  ca-certificates \
  sqlite \
  snappy \
  unzip \
  "$JAVA_JRE_PKG"

# apk openjdk-*-jre here does not ship /usr/bin/java; all service start.sh entrypoints invoke `java`.
if [ ! -x /usr/bin/java ] && [ -x "${JAVA_VM_HOME}/bin/java" ]; then
  ln -sf "${JAVA_VM_HOME}/bin/java" /usr/bin/java
fi

# Drop setuid/setgid on unix_chkpwd if present (scanner hygiene; same idea as datahub-actions).
for f in /usr/bin/unix_chkpwd /usr/sbin/unix_chkpwd; do
  if [ -f "$f" ]; then chmod u-s,g-s "$f"; fi
done

if [ "$INSTALL_JATTACH" = "1" ]; then
  ARCH=$(uname -m)
  case "$ARCH" in
    x86_64) JA_ARCH=x64 ;;
    aarch64) JA_ARCH=arm64 ;;
    *)
      echo "setup_java_runtime.sh: unsupported architecture for jattach: $ARCH" >&2
      exit 1
      ;;
  esac
  TGZ="jattach-linux-${JA_ARCH}.tgz"
  retry_command "download jattach" \
    wget --no-verbose "${GITHUB_REPO_URL}/jattach/jattach/releases/download/${JATTACH_TAG}/${TGZ}" -O "/tmp/${TGZ}"
  tar -xzf "/tmp/${TGZ}" -C /usr/local/bin jattach
  chmod 755 /usr/local/bin/jattach
  rm -f "/tmp/${TGZ}"
fi

mkdir -p "$OTEL_JMX_DIR"

retry_command "download opentelemetry javaagent" \
  wget --no-verbose "${GITHUB_REPO_URL}/open-telemetry/opentelemetry-java-instrumentation/releases/download/v2.27.0/opentelemetry-javaagent.jar" \
  -O "${OTEL_JMX_DIR}/opentelemetry-javaagent.jar"

retry_command "download jmx prometheus javaagent" \
  wget --no-verbose \
  "${MAVEN_CENTRAL_REPO_URL}/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_VERSION}/jmx_prometheus_javaagent-${JMX_VERSION}.jar" \
  -O "${OTEL_JMX_DIR}/jmx_prometheus_javaagent.jar"

JAVA_BIN=$(readlink -f "${JAVA_VM_HOME}/bin/java")
JAVA_HOME=$(dirname "$(dirname "$JAVA_BIN")")
cp "${JAVA_HOME}/lib/security/cacerts" /tmp/kafka.client.truststore.jks
