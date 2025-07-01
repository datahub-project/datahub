TARGET_DIR="${TARGET_DIR:=docker_logs}"
TEST_STRATEGY="${TEST_STRATEGY:=}"

mkdir -p "$TARGET_DIR"
for name in `docker ps -a --format '{{.Names}}'`;
do
    docker logs "$name" >& "${TARGET_DIR}/${name}${TEST_STRATEGY}.log" || true
done