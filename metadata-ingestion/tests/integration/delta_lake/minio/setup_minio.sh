INTEGRATIONS_DIR="./tests/integrations/minio"
#ref https://hub.docker.com/r/minio/minio/
if [[ $(uname -m) == 'arm64' && $(uname) == 'Darwin' ]]; then
  if ! command -v minio &> /dev/null; then
    brew install minio/stable/minio
  fi
  mkdir -p "${INTEGRATIONS_DIR}"
  MINIO_CMD="minio"
else
  wget https://dl.min.io/server/minio/release/linux-amd64/minio -P "${INTEGRATIONS_DIR}"
  chmod +x "${INTEGRATIONS_DIR}/minio"
  MINIO_CMD="${INTEGRATIONS_DIR}/minio"
fi
nohup ${MINIO_CMD} server "${INTEGRATIONS_DIR}/data" > "${INTEGRATIONS_DIR}/temp.log" 2>&1 &
echo $! > "${INTEGRATIONS_DIR}/minio_pid.txt"
