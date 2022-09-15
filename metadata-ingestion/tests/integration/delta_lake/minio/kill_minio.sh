kill -9 `cat minio_pid.txt`
rm ./tests/integrations/delta_lake/minio/minio_pid.txt
rm -rf ./tests/integrations/delta_lake/minio/data