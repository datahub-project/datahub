echo "Stopping container in case it is already running"
source stop_sql_server.sh
echo "Starting container"
source start_sql_server.sh
echo "Loading data / metadata"
source create_metadata.sh
echo "Running ingestion"
source run_ingest.sh
echo "Stopping container"
source stop_sql_server.sh
