#ref https://hub.docker.com/r/minio/minio/

wget https://dl.min.io/server/minio/release/linux-amd64/minio -P ./tests/integrations/delta_lake/minio/
chmod +x ./tests/integrations/delta_lake/minio/minio
nohup ./tests/integrations/delta_lake/minio/minio server ./tests/integrations/delta_lake/minio/data > temp.log 2>&1 &
echo $! > ./tests/integrations/delta_lake/minio/minio_pid.txt
