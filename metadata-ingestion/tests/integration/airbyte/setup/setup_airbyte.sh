#!/bin/bash
# We'll start with set -e but then disable it for specific sections
set -e

# Add a trap to catch errors
trap 'echo "[ERROR] Error occurred at line $LINENO" >&2' ERR

# Add debug markers
echo "[DEBUG] Script started"

# Script to set up Airbyte Open Source for integration testing
# This script creates sources, destinations, and connections in Airbyte

# Static IDs (GUIDs)
STATIC_WORKSPACE_ID="12345678-1234-1234-1234-123456789012"  # Replace with your desired GUID
STATIC_POSTGRES_SOURCE_ID="11111111-1111-1111-1111-111111111111"
STATIC_MYSQL_SOURCE_ID="22222222-2222-2222-2222-222222222222"
STATIC_MYSQL_METAGALAXY_SOURCE_ID="33333333-3333-3333-3333-333333333333"
STATIC_POSTGRES_DEST_ID="44444444-4444-4444-4444-444444444444"
STATIC_PG_TO_PG_CONNECTION_ID="55555555-5555-5555-5555-555555555555"
STATIC_MYSQL_TO_PG_CONNECTION_ID="66666666-6666-6666-6666-666666666666"
STATIC_METAGALAXY_TO_PG_CONNECTION_ID="77777777-7777-7777-7777-777777777777"

# Try multiple API endpoints to find one that works
API_ENDPOINTS=(
  "http://localhost:8000/api/v1"
  "http://localhost:8001/api/v1"
  "http://localhost:8000/api/public/v1"
  "http://localhost:8001/api/public/v1"
  "http://airbyte-server:8000/api/v1"
  "http://airbyte-server:8001/api/v1"
  "http://airbyte-server:8000/api/public/v1"
  "http://airbyte-server:8001/api/public/v1"
  "http://host.docker.internal:8000/api/v1"
  "http://host.docker.internal:8001/api/v1"
)

HEADER="Content-Type: application/json"
AUTH="airbyte:password"
AUTH_HEADER="Authorization: Basic $(echo -n "$AUTH" | base64)"

# Find a working API endpoint
echo "Finding a working Airbyte API endpoint..."
AIRBYTE_API=""
for endpoint in "${API_ENDPOINTS[@]}"; do
  echo "Trying $endpoint..."
  if curl -s -f -m 5 -u "$AUTH" "$endpoint/health" > /dev/null 2>&1; then
    AIRBYTE_API="$endpoint"
    echo "✅ Found working API at $AIRBYTE_API"
    break
  fi
done

if [ -z "$AIRBYTE_API" ]; then
  echo "❌ Could not find a working API endpoint"
  echo "Available endpoints:"
  for endpoint in "${API_ENDPOINTS[@]}"; do
    echo "Testing $endpoint..."
    curl -s -m 5 -v -u "$AUTH" "$endpoint/health" || echo "Failed to connect to $endpoint"
  done
  
  # Checking Docker connectivity
  echo "Checking Docker networking..."
  docker ps || echo "Docker not available"
  
  # Check if airbyte containers are running
  echo "Checking Airbyte containers..."
  docker ps | grep airbyte || echo "No Airbyte containers found"
  
  # Try ping to check basic connectivity
  echo "Trying ping to check connectivity..."
  ping -c 1 localhost || echo "Cannot ping localhost"
  
  exit 1
fi

# Complete onboarding if needed
echo "Trying to complete onboarding..."
curl -s -u "$AUTH" "$AIRBYTE_API/instance_configuration/setup" -H "$HEADER" -d '{
  "email": "test@example.com",
  "anonymousDataCollection": false,
  "news": false,
  "securityUpdates": false
}' || true

# Wait for the API to be fully ready
echo "Waiting for Airbyte API to be ready..."
max_attempts=30
attempt=0
while true; do
  attempt=$((attempt + 1))
  echo "Attempt $attempt of $max_attempts"

  response=$(curl -s -m 10 -u "$AUTH" "$AIRBYTE_API/workspaces/list" -H "$HEADER" -d '{}')
  if echo "$response" | grep -q "workspaces"; then
    echo "Airbyte API is ready!"
    break
  else
    echo "API not ready yet. Response: ${response:0:100}..."
  fi

  if [ $attempt -eq $max_attempts ]; then
    echo "Airbyte API did not become ready in time."
    echo "Last response: $response"
    echo "Checking Airbyte container status:"
    docker ps --filter "name=airbyte" || echo "Could not check docker containers"
    echo "Checking container logs:"
    docker logs $(docker ps -q --filter "name=airbyte-server") --tail 20 || echo "Could not get airbyte-server logs"
    exit 1
  fi

  sleep 10
done

# Get workspace ID
echo "Getting workspace ID..."
WORKSPACE_RESPONSE=$(curl -s -u "$AUTH" "$AIRBYTE_API/workspaces/list" -H "$HEADER" -d '{}')
WORKSPACE_ID=$(echo "$WORKSPACE_RESPONSE" | jq -r '.workspaces[0].workspaceId')

if [ -z "$WORKSPACE_ID" ] || [ "$WORKSPACE_ID" == "null" ]; then
  echo "No workspace found. Creating one..."
  WORKSPACE_RESPONSE=$(curl -s -u "$AUTH" "$AIRBYTE_API/workspaces/create" -H "$HEADER" -d '{"name": "Default Workspace"}')
  WORKSPACE_ID=$(echo "$WORKSPACE_RESPONSE" | jq -r '.workspaceId')

  if [ -z "$WORKSPACE_ID" ] || [ "$WORKSPACE_ID" == "null" ]; then
    echo "Failed to create workspace!"
    exit 1
  fi
fi

echo "Using workspace ID: $WORKSPACE_ID"

# Setup variables for the databases
AIRBYTE_DB_HOST="airbyte-db"
AIRBYTE_DB_USER="docker"
AIRBYTE_DB_NAME="airbyte"
AIRBYTE_DB_PASSWORD="docker"

TEST_DB_HOST="test-postgres-db"
TEST_DB_USER="test"
TEST_DB_NAME="test"
TEST_DB_PASSWORD="test"

# Test Airbyte database connection
echo "Testing Airbyte database connection..."
if ! docker exec $AIRBYTE_DB_HOST psql -U $AIRBYTE_DB_USER -d $AIRBYTE_DB_NAME -c "SELECT 1;" > /dev/null 2>&1; then
  echo "❌ Cannot connect to the Airbyte database. Trying alternative hosts..."
  
  # Try alternative host names
  for alt_host in "airbyte-db" "localhost" "postgres" "airbyte-postgres"; do
    echo "Trying database host: $alt_host"
    if docker exec $alt_host psql -U $AIRBYTE_DB_USER -d $AIRBYTE_DB_NAME -c "SELECT 1;" > /dev/null 2>&1; then
      echo "✅ Connected to Airbyte database using host: $alt_host"
      AIRBYTE_DB_HOST=$alt_host
      break
    fi
  done
  
  # Final check if we found a working connection
  if ! docker exec $AIRBYTE_DB_HOST psql -U $AIRBYTE_DB_USER -d $AIRBYTE_DB_NAME -c "SELECT 1;" > /dev/null 2>&1; then
    echo "❌ Failed to connect to the Airbyte database after trying alternative hosts."
    echo "Please verify that the Airbyte database container is running and accessible."
    echo "Available docker containers:"
    docker ps || echo "Docker not available"
    exit 1
  fi
fi
echo "✅ Airbyte database connection successful to $AIRBYTE_DB_HOST"

# Test test database connection
echo "Testing test database connection..."
if ! docker exec $TEST_DB_HOST psql -U $TEST_DB_USER -d $TEST_DB_NAME -c "SELECT 1;" > /dev/null 2>&1; then
  echo "❌ Cannot connect to the test database. Trying alternative hosts..."
  
  # Try alternative host names
  for alt_host in "test-postgres" "localhost" "postgres" "test-postgres-db"; do
    echo "Trying database host: $alt_host"
    if docker exec $alt_host psql -U $TEST_DB_USER -d $TEST_DB_NAME -c "SELECT 1;" > /dev/null 2>&1; then
      echo "✅ Connected to test database using host: $alt_host"
      TEST_DB_HOST=$alt_host
      break
    fi
  done
  
  # Final check if we found a working connection
  if ! docker exec $TEST_DB_HOST psql -U $TEST_DB_USER -d $TEST_DB_NAME -c "SELECT 1;" > /dev/null 2>&1; then
    echo "❌ Failed to connect to the test database after trying alternative hosts."
    echo "Please verify that the test database container is running and accessible."
    echo "Available docker containers:"
    docker ps || echo "Docker not available"
    exit 1
  fi
fi
echo "✅ Test database connection successful to $TEST_DB_HOST"

# Function to check if a table exists and update ID in Airbyte database
check_and_update_airbyte_table() {
  local table_name=$1
  local new_id=$2
  local old_id=$3
  
  # Check if table exists in Airbyte database
  table_exists=$(docker exec $AIRBYTE_DB_HOST psql -U $AIRBYTE_DB_USER -d $AIRBYTE_DB_NAME -t -c "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '$table_name');")
  
  if [[ $table_exists == *t* ]]; then
    echo "Updating $table_name table with static ID in Airbyte database..."
    docker exec $AIRBYTE_DB_HOST psql -U $AIRBYTE_DB_USER -d $AIRBYTE_DB_NAME -c "UPDATE $table_name SET id = '$new_id' WHERE id = '$old_id';"
    return 0
  else
    echo "Table $table_name does not exist in Airbyte database, skipping update."
    return 1
  fi
}

# Force static workspace ID using direct database update in Airbyte database
echo "Updating workspace ID in the Airbyte database to use static ID..."
DB_UPDATE_RESULT=$(docker exec $AIRBYTE_DB_HOST psql -U $AIRBYTE_DB_USER -d $AIRBYTE_DB_NAME -c "UPDATE workspace SET id = '$STATIC_WORKSPACE_ID' WHERE id = '$WORKSPACE_ID' RETURNING id;")
echo "Database update result: $DB_UPDATE_RESULT"

# Use the static ID for all subsequent operations
WORKSPACE_ID="$STATIC_WORKSPACE_ID"
echo "Now using static workspace ID: $WORKSPACE_ID"

# Create Postgres source
echo "[DEBUG] About to create Postgres source"
set +e
echo "Creating Postgres source..."
POSTGRES_SOURCE_DEF_RESPONSE=$(curl -s -u "$AUTH" "$AIRBYTE_API/source_definitions/list" -H "$HEADER" -d '{}')
echo "Source definitions response (first 300 chars): ${POSTGRES_SOURCE_DEF_RESPONSE:0:300}..."
POSTGRES_SOURCE_DEF_ID=$(echo "$POSTGRES_SOURCE_DEF_RESPONSE" | jq -r '.sourceDefinitions[] | select(.name=="Postgres") | .sourceDefinitionId')
echo "Postgres source definition ID: $POSTGRES_SOURCE_DEF_ID"

if [ -z "$POSTGRES_SOURCE_DEF_ID" ] || [ "$POSTGRES_SOURCE_DEF_ID" == "null" ]; then
  echo "❌ Could not find Postgres source definition!"
  echo "Available source definitions:"
  echo "$POSTGRES_SOURCE_DEF_RESPONSE" | jq -r '.sourceDefinitions[] | .name'
  # Try alternative name
  POSTGRES_SOURCE_DEF_ID=$(echo "$POSTGRES_SOURCE_DEF_RESPONSE" | jq -r '.sourceDefinitions[] | select(.name=="PostgreSQL") | .sourceDefinitionId')
  echo "Trying alternative name 'PostgreSQL', ID: $POSTGRES_SOURCE_DEF_ID"
fi

POSTGRES_CONFIG='{
  "name": "Test Postgres Source",
  "sourceDefinitionId": "'"$POSTGRES_SOURCE_DEF_ID"'",
  "workspaceId": "'"$WORKSPACE_ID"'",
  "connectionConfiguration": {
    "host": "host.docker.internal",
    "port": 5433,
    "database": "test",
    "schema": "public",
    "username": "test",
    "password": "test",
    "ssl": false,
    "tunnel_method": {
      "tunnel_method": "NO_TUNNEL"
    }
  }
}'

echo "Creating Postgres source with config: $POSTGRES_CONFIG"
POSTGRES_SOURCE_RESPONSE=$(curl -s -u "$AUTH" "$AIRBYTE_API/sources/create" -H "$HEADER" -d "$POSTGRES_CONFIG")
echo "Postgres source creation response: $POSTGRES_SOURCE_RESPONSE"
POSTGRES_SOURCE_ID=$(echo "$POSTGRES_SOURCE_RESPONSE" | jq -r '.sourceId')

if [ -z "$POSTGRES_SOURCE_ID" ] || [ "$POSTGRES_SOURCE_ID" == "null" ]; then
  echo "❌ Failed to create Postgres source!"
  echo "Response: $POSTGRES_SOURCE_RESPONSE"
  # Continue anyway
else
  echo "Created Postgres source with ID: $POSTGRES_SOURCE_ID"
  
  # Force static source ID in database
  echo "Updating Postgres source ID in the database to use static ID..."
  # Update all potential tables that could store source IDs
  check_and_update_airbyte_table "actor" "$STATIC_POSTGRES_SOURCE_ID" "$POSTGRES_SOURCE_ID"
  check_and_update_airbyte_table "source" "$STATIC_POSTGRES_SOURCE_ID" "$POSTGRES_SOURCE_ID" 
  check_and_update_airbyte_table "actor_definition_workspace_grant" "$STATIC_POSTGRES_SOURCE_ID" "$POSTGRES_SOURCE_ID"
  check_and_update_airbyte_table "actor_catalog" "$STATIC_POSTGRES_SOURCE_ID" "$POSTGRES_SOURCE_ID"
  check_and_update_airbyte_table "actor_catalog_fetch_event" "$STATIC_POSTGRES_SOURCE_ID" "$POSTGRES_SOURCE_ID"
  check_and_update_airbyte_table "actor_oauth_parameter" "$STATIC_POSTGRES_SOURCE_ID" "$POSTGRES_SOURCE_ID"
  
  echo "Now using static Postgres source ID: $STATIC_POSTGRES_SOURCE_ID"
  POSTGRES_SOURCE_ID="$STATIC_POSTGRES_SOURCE_ID"
fi
set -e

# Create MySQL source
echo "[DEBUG] About to create MySQL source"
set +e
echo "Creating MySQL source..."

# Test MySQL connectivity
echo "Testing MySQL connectivity from script host..."
if ! docker exec test-mysql mysqladmin ping -h localhost -u test -ptest > /dev/null 2>&1; then
  echo "❌ Cannot connect to MySQL from script host"
  echo "Checking MySQL container status..."
  docker ps | grep mysql || echo "No MySQL containers found"
  echo "Trying to start MySQL container if not running..."
  docker-compose up -d test-mysql || echo "Failed to start MySQL container"
  sleep 5
fi

# Wait for MySQL to be ready
echo "Waiting for MySQL to be ready..."
max_attempts=30
attempt=0
while true; do
  attempt=$((attempt + 1))
  echo "Attempt $attempt of $max_attempts"
  
  if docker exec test-mysql mysqladmin ping -h localhost -u test -ptest > /dev/null 2>&1; then
    echo "✅ MySQL is ready!"
    break
  fi
  
  if [ $attempt -eq $max_attempts ]; then
    echo "❌ MySQL did not become ready in time"
    echo "Checking MySQL logs:"
    docker logs test-mysql --tail 20
    exit 1
  fi
  
  sleep 2
done

MYSQL_SOURCE_DEF_RESPONSE=$(curl -s -u "$AUTH" "$AIRBYTE_API/source_definitions/list" -H "$HEADER" -d '{}')
echo "MySQL source definition response (first 300 chars): ${MYSQL_SOURCE_DEF_RESPONSE:0:300}..."

# Try to find MySQL source definition with different possible names
MYSQL_SOURCE_DEF_ID=$(echo "$MYSQL_SOURCE_DEF_RESPONSE" | jq -r '.sourceDefinitions[] | select(.name=="MySQL") | .sourceDefinitionId')
echo "MySQL source definition ID (looking for 'MySQL'): $MYSQL_SOURCE_DEF_ID"

if [ -z "$MYSQL_SOURCE_DEF_ID" ] || [ "$MYSQL_SOURCE_DEF_ID" == "null" ]; then
  echo "Trying alternative source definition names..."
  echo "Available source definitions:"
  echo "$MYSQL_SOURCE_DEF_RESPONSE" | jq -r '.sourceDefinitions[] | .name'
  
  # Try alternative names that might represent MySQL
  for name in "MySql" "My SQL" "MYSQL" "mysql" "my-sql"; do
    echo "Checking for source with name: $name"
    alt_id=$(echo "$MYSQL_SOURCE_DEF_RESPONSE" | jq -r '.sourceDefinitions[] | select(.name=="'"$name"'") | .sourceDefinitionId')
    if [ -n "$alt_id" ] && [ "$alt_id" != "null" ]; then
      echo "Found source definition with name '$name', ID: $alt_id"
      MYSQL_SOURCE_DEF_ID=$alt_id
      break
    fi
  done
fi

if [ -z "$MYSQL_SOURCE_DEF_ID" ] || [ "$MYSQL_SOURCE_DEF_ID" == "null" ]; then
  echo "❌ Could not find MySQL source definition after all attempts!"
  echo "Skipping MySQL source creation"
else
  echo "Found MySQL source definition with ID: $MYSQL_SOURCE_DEF_ID"
  
  # Test MySQL connectivity from Airbyte container
  echo "Testing MySQL connectivity from Airbyte container..."
  if ! docker exec $(docker ps -q --filter name=airbyte-server) bash -c "command -v mysql >/dev/null 2>&1 && mysql -h host.docker.internal -P 30336 -u test -ptest -e 'SELECT 1;' >/dev/null 2>&1"; then
    echo "❌ Cannot connect to MySQL from Airbyte container"
    echo "This might cause issues with the MySQL source. Continuing anyway..."
  else
    echo "✅ MySQL is accessible from Airbyte container"
  fi
  
  MYSQL_CONFIG='{
    "name": "Test MySQL Source",
    "sourceDefinitionId": "'"$MYSQL_SOURCE_DEF_ID"'",
    "workspaceId": "'"$WORKSPACE_ID"'",
    "connectionConfiguration": {
      "host": "host.docker.internal",
      "port": 30306,
      "database": "test",
      "username": "test",
      "password": "test",
      "ssl": false,
      "tunnel_method": {
        "tunnel_method": "NO_TUNNEL"
      },
      "replication_method": {
        "method": "STANDARD",
        "cursor_field": "updated_at"
      }
    }
  }'

  echo "Creating MySQL source with config: $MYSQL_CONFIG"
  MYSQL_SOURCE_RESPONSE=$(curl -s -u "$AUTH" "$AIRBYTE_API/sources/create" -H "$HEADER" -d "$MYSQL_CONFIG")
  echo "MySQL source creation response: $MYSQL_SOURCE_RESPONSE"
  MYSQL_SOURCE_ID=$(echo "$MYSQL_SOURCE_RESPONSE" | jq -r '.sourceId')

  if [ -z "$MYSQL_SOURCE_ID" ] || [ "$MYSQL_SOURCE_ID" == "null" ]; then
    echo "❌ Failed to create MySQL source!"
    echo "Response: $MYSQL_SOURCE_RESPONSE"
    # Try to get more information about the failure
    echo "Checking MySQL container status:"
    docker ps | grep mysql
    echo "Checking MySQL logs:"
    docker logs test-mysql --tail 20
    echo "Checking Airbyte server logs:"
    docker logs $(docker ps -q --filter name=airbyte-server) --tail 20
    # Continue anyway
  else
    echo "Created MySQL source with ID: $MYSQL_SOURCE_ID"
    
    # Force static source ID in database
    echo "Updating MySQL source ID in the database to use static ID..."
    # Update all potential tables that could store source IDs
    check_and_update_airbyte_table "actor" "$STATIC_MYSQL_SOURCE_ID" "$MYSQL_SOURCE_ID"
    check_and_update_airbyte_table "source" "$STATIC_MYSQL_SOURCE_ID" "$MYSQL_SOURCE_ID"
    check_and_update_airbyte_table "actor_definition_workspace_grant" "$STATIC_MYSQL_SOURCE_ID" "$MYSQL_SOURCE_ID"
    check_and_update_airbyte_table "actor_catalog" "$STATIC_MYSQL_SOURCE_ID" "$MYSQL_SOURCE_ID"
    check_and_update_airbyte_table "actor_catalog_fetch_event" "$STATIC_MYSQL_SOURCE_ID" "$MYSQL_SOURCE_ID"
    check_and_update_airbyte_table "actor_oauth_parameter" "$STATIC_MYSQL_SOURCE_ID" "$MYSQL_SOURCE_ID"
    
    echo "Now using static MySQL source ID: $STATIC_MYSQL_SOURCE_ID"
    MYSQL_SOURCE_ID="$STATIC_MYSQL_SOURCE_ID"
  fi
  
  # Create MySQL Metagalaxy source
  echo "Creating MySQL Metagalaxy source..."
  MYSQL_METAGALAXY_CONFIG='{
    "name": "MySQL Metagalaxy Source",
    "sourceDefinitionId": "'"$MYSQL_SOURCE_DEF_ID"'",
    "workspaceId": "'"$WORKSPACE_ID"'",
    "connectionConfiguration": {
      "host": "host.docker.internal",
      "port": 30306,
      "database": "test",
      "username": "test",
      "password": "test",
      "ssl": false,
      "tunnel_method": {
        "tunnel_method": "NO_TUNNEL"
      },
      "replication_method": {
        "method": "STANDARD"
      }
    }
  }'

  echo "Creating MySQL Metagalaxy source with config: $MYSQL_METAGALAXY_CONFIG"
  MYSQL_METAGALAXY_SOURCE_RESPONSE=$(curl -s -u "$AUTH" "$AIRBYTE_API/sources/create" -H "$HEADER" -d "$MYSQL_METAGALAXY_CONFIG")
  echo "MySQL Metagalaxy source creation response: $MYSQL_METAGALAXY_SOURCE_RESPONSE"
  MYSQL_METAGALAXY_SOURCE_ID=$(echo "$MYSQL_METAGALAXY_SOURCE_RESPONSE" | jq -r '.sourceId')

  if [ -z "$MYSQL_METAGALAXY_SOURCE_ID" ] || [ "$MYSQL_METAGALAXY_SOURCE_ID" == "null" ]; then
    echo "❌ Failed to create MySQL Metagalaxy source!"
    echo "Response: $MYSQL_METAGALAXY_SOURCE_RESPONSE"
    # Continue anyway
  else
    echo "Created MySQL Metagalaxy source with ID: $MYSQL_METAGALAXY_SOURCE_ID"
    
    # Force static source ID in database
    echo "Updating MySQL Metagalaxy source ID in the database to use static ID..."
    # Update all potential tables that could store source IDs
    check_and_update_airbyte_table "actor" "$STATIC_MYSQL_METAGALAXY_SOURCE_ID" "$MYSQL_METAGALAXY_SOURCE_ID"
    check_and_update_airbyte_table "source" "$STATIC_MYSQL_METAGALAXY_SOURCE_ID" "$MYSQL_METAGALAXY_SOURCE_ID"
    check_and_update_airbyte_table "actor_definition_workspace_grant" "$STATIC_MYSQL_METAGALAXY_SOURCE_ID" "$MYSQL_METAGALAXY_SOURCE_ID"
    check_and_update_airbyte_table "actor_catalog" "$STATIC_MYSQL_METAGALAXY_SOURCE_ID" "$MYSQL_METAGALAXY_SOURCE_ID"
    check_and_update_airbyte_table "actor_catalog_fetch_event" "$STATIC_MYSQL_METAGALAXY_SOURCE_ID" "$MYSQL_METAGALAXY_SOURCE_ID"
    check_and_update_airbyte_table "actor_oauth_parameter" "$STATIC_MYSQL_METAGALAXY_SOURCE_ID" "$MYSQL_METAGALAXY_SOURCE_ID"
    
    echo "Now using static MySQL Metagalaxy source ID: $STATIC_MYSQL_METAGALAXY_SOURCE_ID"
    MYSQL_METAGALAXY_SOURCE_ID="$STATIC_MYSQL_METAGALAXY_SOURCE_ID"
  fi
fi
set -e

# Create Postgres destination
echo "[DEBUG] About to create Postgres destination"
set +e
echo "Creating Postgres destination..."
POSTGRES_DEST_DEF_RESPONSE=$(curl -s -u "$AUTH" "$AIRBYTE_API/destination_definitions/list" -H "$HEADER" -d '{}')
echo "Destination definitions response (first 300 chars): ${POSTGRES_DEST_DEF_RESPONSE:0:300}..."
POSTGRES_DEST_DEF_ID=$(echo "$POSTGRES_DEST_DEF_RESPONSE" | jq -r '.destinationDefinitions[] | select(.name=="Postgres") | .destinationDefinitionId')
echo "Postgres destination definition ID: $POSTGRES_DEST_DEF_ID"

if [ -z "$POSTGRES_DEST_DEF_ID" ] || [ "$POSTGRES_DEST_DEF_ID" == "null" ]; then
  echo "❌ Could not find Postgres destination definition!"
  echo "Available destination definitions:"
  echo "$POSTGRES_DEST_DEF_RESPONSE" | jq -r '.destinationDefinitions[] | .name'
  # Try alternative name
  POSTGRES_DEST_DEF_ID=$(echo "$POSTGRES_DEST_DEF_RESPONSE" | jq -r '.destinationDefinitions[] | select(.name=="PostgreSQL") | .destinationDefinitionId')
  echo "Trying alternative name 'PostgreSQL', ID: $POSTGRES_DEST_DEF_ID"
fi

POSTGRES_DEST_CONFIG='{
  "name": "Test Postgres Destination",
  "destinationDefinitionId": "'"$POSTGRES_DEST_DEF_ID"'",
  "workspaceId": "'"$WORKSPACE_ID"'",
  "connectionConfiguration": {
    "host": "host.docker.internal",
    "port": 5433,
    "database": "test",
    "schema": "destination_schema",
    "username": "test",
    "password": "test",
    "ssl": false,
    "tunnel_method": {
      "tunnel_method": "NO_TUNNEL"
    }
  }
}'

echo "Creating Postgres destination with config: $POSTGRES_DEST_CONFIG"
POSTGRES_DEST_RESPONSE=$(curl -s -u "$AUTH" "$AIRBYTE_API/destinations/create" -H "$HEADER" -d "$POSTGRES_DEST_CONFIG")
echo "Postgres destination creation response: $POSTGRES_DEST_RESPONSE"
POSTGRES_DEST_ID=$(echo "$POSTGRES_DEST_RESPONSE" | jq -r '.destinationId')

if [ -z "$POSTGRES_DEST_ID" ] || [ "$POSTGRES_DEST_ID" == "null" ]; then
  echo "❌ Failed to create Postgres destination!"
  echo "Response: $POSTGRES_DEST_RESPONSE"
  # Continue anyway
else
  echo "Created Postgres destination with ID: $POSTGRES_DEST_ID"
  
  # Force static destination ID in database
  echo "Updating Postgres destination ID in the database to use static ID..."
  # Update all potential tables that could store destination IDs
  check_and_update_airbyte_table "actor" "$STATIC_POSTGRES_DEST_ID" "$POSTGRES_DEST_ID"
  check_and_update_airbyte_table "destination" "$STATIC_POSTGRES_DEST_ID" "$POSTGRES_DEST_ID"
  check_and_update_airbyte_table "actor_definition_workspace_grant" "$STATIC_POSTGRES_DEST_ID" "$POSTGRES_DEST_ID"
  check_and_update_airbyte_table "actor_oauth_parameter" "$STATIC_POSTGRES_DEST_ID" "$POSTGRES_DEST_ID"
  
  echo "Now using static Postgres destination ID: $STATIC_POSTGRES_DEST_ID"
  POSTGRES_DEST_ID="$STATIC_POSTGRES_DEST_ID"
fi
set -e

# Create connections only if we have both source and destination IDs
echo "[DEBUG] About to create connections"
set +e
if [[ -n "$POSTGRES_SOURCE_ID" && "$POSTGRES_SOURCE_ID" != "null" && -n "$POSTGRES_DEST_ID" && "$POSTGRES_DEST_ID" != "null" ]]; then
  # Create PostgreSQL to PostgreSQL connection
  echo "[DEBUG] Creating PostgreSQL to PostgreSQL connection"
  PG_TO_PG_CONFIG='{
    "name": "Postgres to Postgres Connection",
    "sourceId": "'"$POSTGRES_SOURCE_ID"'",
    "destinationId": "'"$POSTGRES_DEST_ID"'",
    "status": "active",
    "syncCatalog": {
      "streams": [
        {
          "stream": {
            "name": "customers",
            "jsonSchema": {},
            "supportedSyncModes": ["full_refresh", "incremental"],
            "sourceDefinedPrimaryKey": [["customer_id"]]
          },
          "config": {
            "syncMode": "full_refresh",
            "destinationSyncMode": "overwrite",
            "selected": true
          }
        },
        {
          "stream": {
            "name": "orders",
            "jsonSchema": {},
            "supportedSyncModes": ["full_refresh", "incremental"],
            "sourceDefinedPrimaryKey": [["order_id"]]
          },
          "config": {
            "syncMode": "full_refresh",
            "destinationSyncMode": "overwrite",
            "selected": true
          }
        }
      ]
    },
    "scheduleType": "manual",
    "operationIds": []
  }'
  
  echo "Creating PG to PG connection with config: $PG_TO_PG_CONFIG"
  PG_TO_PG_CONNECTION_RESPONSE=$(curl -s -u "$AUTH" "$AIRBYTE_API/connections/create" -H "$HEADER" -d "$PG_TO_PG_CONFIG")
  echo "PG to PG connection creation response: $PG_TO_PG_CONNECTION_RESPONSE"
  PG_TO_PG_CONNECTION_ID=$(echo "$PG_TO_PG_CONNECTION_RESPONSE" | jq -r '.connectionId')
  
  if [ -z "$PG_TO_PG_CONNECTION_ID" ] || [ "$PG_TO_PG_CONNECTION_ID" == "null" ]; then
    echo "❌ Failed to create PostgreSQL to PostgreSQL connection!"
    echo "Response: $PG_TO_PG_CONNECTION_RESPONSE"
    # Continue anyway
  else
    echo "Created PostgreSQL to PostgreSQL connection with ID: $PG_TO_PG_CONNECTION_ID"
    
    # Function to check if a connection ID exists in the database
    check_connection_exists() {
      local connection_id=$1
      local exists=$(docker exec $AIRBYTE_DB_HOST psql -U $AIRBYTE_DB_USER -d $AIRBYTE_DB_NAME -t -c "SELECT EXISTS (SELECT 1 FROM connection WHERE id = '$connection_id');")
      if [[ $exists == *t* ]]; then
        return 0
      else
        return 1
      fi
    }

    # Function to update connection ID safely
    update_connection_id() {
      local old_id=$1
      local new_id=$2
      
      # First check if the new ID already exists
      if check_connection_exists "$new_id"; then
        echo "⚠️ Connection ID $new_id already exists, skipping update"
        return 1
      fi
      
      # Check if the old ID exists
      if ! check_connection_exists "$old_id"; then
        echo "⚠️ Connection ID $old_id does not exist, skipping update"
        return 1
      fi
      
      # Update the ID
      echo "Updating connection ID from $old_id to $new_id..."
      docker exec $AIRBYTE_DB_HOST psql -U $AIRBYTE_DB_USER -d $AIRBYTE_DB_NAME -c "UPDATE connection SET id = '$new_id' WHERE id = '$old_id';"
      return 0
    }

    # Update the connection ID update sections
    if [ -n "$PG_TO_PG_CONNECTION_ID" ] && [ "$PG_TO_PG_CONNECTION_ID" != "null" ]; then
      echo "Updating PostgreSQL to PostgreSQL connection ID in the database to use static ID..."
      if update_connection_id "$PG_TO_PG_CONNECTION_ID" "$STATIC_PG_TO_PG_CONNECTION_ID"; then
        echo "Now using static connection ID: $STATIC_PG_TO_PG_CONNECTION_ID"
        PG_TO_PG_CONNECTION_ID="$STATIC_PG_TO_PG_CONNECTION_ID"
      else
        echo "Using original connection ID: $PG_TO_PG_CONNECTION_ID"
      fi
    fi
  fi
else
  echo "[DEBUG] Skipping PostgreSQL to PostgreSQL connection creation"
  echo "⚠️ Skipping PostgreSQL to PostgreSQL connection creation because source or destination ID is missing."
  echo "Postgres Source ID: $POSTGRES_SOURCE_ID"
  echo "Postgres Destination ID: $POSTGRES_DEST_ID"
fi

# Check if databases are actually running
echo "Checking if Postgres is available from inside the Airbyte container..."
if ! docker exec $(docker ps -q --filter name=airbyte-server) bash -c "command -v psql >/dev/null 2>&1 && psql -h host.docker.internal -p 5433 -U test -d test -c 'SELECT 1;' >/dev/null 2>&1"; then
  echo "❌ Cannot connect to Postgres from Airbyte container"
  echo "Trying alternative host names..."
  
  # Try alternative host names
  for alt_host in "test-postgres-db" "test-postgres" "localhost"; do
    echo "Trying Postgres host: $alt_host"
    if docker exec $(docker ps -q --filter name=airbyte-server) bash -c "command -v psql >/dev/null 2>&1 && psql -h $alt_host -p 5433 -U test -d test -c 'SELECT 1;' >/dev/null 2>&1"; then
      echo "✅ Connected to Postgres using host: $alt_host"
      # Update the Postgres config with the working host
      POSTGRES_CONFIG=$(echo "$POSTGRES_CONFIG" | jq --arg host "$alt_host" '.connectionConfiguration.host = $host')
      POSTGRES_DEST_CONFIG=$(echo "$POSTGRES_DEST_CONFIG" | jq --arg host "$alt_host" '.connectionConfiguration.host = $host')
      break
    fi
  done
else
  echo "✅ Postgres is accessible from Airbyte container"
fi

echo "Checking if MySQL is available from inside the Airbyte container..."
if ! docker exec $(docker ps -q --filter name=airbyte-server) bash -c "command -v mysql >/dev/null 2>&1 && mysql -h host.docker.internal -P 30336 -u test -ptest -e 'SELECT 1;' >/dev/null 2>&1"; then
  echo "❌ Cannot connect to MySQL from Airbyte container"
  echo "This might cause issues with the MySQL source. Continuing anyway..."
else
  echo "✅ MySQL is accessible from Airbyte container"
fi

if [[ -n "$MYSQL_SOURCE_ID" && "$MYSQL_SOURCE_ID" != "null" && -n "$POSTGRES_DEST_ID" && "$POSTGRES_DEST_ID" != "null" ]]; then
  # Create MySQL to PostgreSQL connection
  echo "[DEBUG] Creating MySQL to PostgreSQL connection"
  set +e
  MYSQL_TO_PG_CONFIG='{
    "name": "MySQL to Postgres Connection",
    "sourceId": "'"$MYSQL_SOURCE_ID"'",
    "destinationId": "'"$POSTGRES_DEST_ID"'",
    "status": "active",
    "syncCatalog": {
      "streams": [
        {
          "stream": {
            "name": "customers",
            "namespace": "test",
            "jsonSchema": {
              "type": "object",
              "properties": {
                "customer_id": {"type": "integer"},
                "name": {"type": "string"},
                "email": {"type": "string"}
              }
            },
            "supportedSyncModes": ["full_refresh", "incremental"],
            "sourceDefinedCursor": false,
            "defaultCursorField": ["customer_id"]
          },
          "config": {
            "syncMode": "incremental",
            "cursorField": ["customer_id"],
            "destinationSyncMode": "append",
            "primaryKey": [["customer_id"]],
            "selected": true
          }
        },
        {
          "stream": {
            "name": "orders",
            "namespace": "test",
            "jsonSchema": {
              "type": "object",
              "properties": {
                "order_id": {"type": "integer"},
                "customer_id": {"type": "integer"},
                "order_date": {"type": "string", "format": "date-time"},
                "total_amount": {"type": "number"}
              }
            },
            "supportedSyncModes": ["full_refresh", "incremental"],
            "sourceDefinedCursor": false,
            "defaultCursorField": ["order_id"]
          },
          "config": {
            "syncMode": "incremental",
            "cursorField": ["order_id"],
            "destinationSyncMode": "append",
            "primaryKey": [["order_id"]],
            "selected": true
          }
        }
      ]
    },
    "scheduleType": "manual",
    "operationIds": []
  }'
  
  echo "Creating MySQL to PG connection with config: $MYSQL_TO_PG_CONFIG"
  MYSQL_TO_PG_CONNECTION_RESPONSE=$(curl -s -u "$AUTH" "$AIRBYTE_API/connections/create" -H "$HEADER" -d "$MYSQL_TO_PG_CONFIG")
  echo "MySQL to PG connection creation response: $MYSQL_TO_PG_CONNECTION_RESPONSE"
  MYSQL_TO_PG_CONNECTION_ID=$(echo "$MYSQL_TO_PG_CONNECTION_RESPONSE" | jq -r '.connectionId')
  
  if [ -z "$MYSQL_TO_PG_CONNECTION_ID" ] || [ "$MYSQL_TO_PG_CONNECTION_ID" == "null" ]; then
    echo "❌ Failed to create MySQL to PostgreSQL connection!"
    echo "Response: $MYSQL_TO_PG_CONNECTION_RESPONSE"
    # Continue anyway
  else
    echo "Created MySQL to PostgreSQL connection with ID: $MYSQL_TO_PG_CONNECTION_ID"
    
    # Function to check if a connection ID exists in the database
    check_connection_exists() {
      local connection_id=$1
      local exists=$(docker exec $AIRBYTE_DB_HOST psql -U $AIRBYTE_DB_USER -d $AIRBYTE_DB_NAME -t -c "SELECT EXISTS (SELECT 1 FROM connection WHERE id = '$connection_id');")
      if [[ $exists == *t* ]]; then
        return 0
      else
        return 1
      fi
    }

    # Function to update connection ID safely
    update_connection_id() {
      local old_id=$1
      local new_id=$2
      
      # First check if the new ID already exists
      if check_connection_exists "$new_id"; then
        echo "⚠️ Connection ID $new_id already exists, skipping update"
        return 1
      fi
      
      # Check if the old ID exists
      if ! check_connection_exists "$old_id"; then
        echo "⚠️ Connection ID $old_id does not exist, skipping update"
        return 1
      fi
      
      # Update the ID
      echo "Updating connection ID from $old_id to $new_id..."
      docker exec $AIRBYTE_DB_HOST psql -U $AIRBYTE_DB_USER -d $AIRBYTE_DB_NAME -c "UPDATE connection SET id = '$new_id' WHERE id = '$old_id';"
      return 0
    }

    # Update the connection ID update sections
    if [ -n "$MYSQL_TO_PG_CONNECTION_ID" ] && [ "$MYSQL_TO_PG_CONNECTION_ID" != "null" ]; then
      echo "Updating MySQL to PostgreSQL connection ID in the database to use static ID..."
      if update_connection_id "$MYSQL_TO_PG_CONNECTION_ID" "$STATIC_MYSQL_TO_PG_CONNECTION_ID"; then
        echo "Now using static connection ID: $STATIC_MYSQL_TO_PG_CONNECTION_ID"
        MYSQL_TO_PG_CONNECTION_ID="$STATIC_MYSQL_TO_PG_CONNECTION_ID"
      else
        echo "Using original connection ID: $MYSQL_TO_PG_CONNECTION_ID"
      fi
    fi
  fi
  set -e
else
  echo "[DEBUG] Skipping MySQL to PostgreSQL connection creation"
  echo "⚠️ Skipping MySQL to PostgreSQL connection creation because source or destination ID is missing."
  echo "MySQL Source ID: $MYSQL_SOURCE_ID"
  echo "Postgres Destination ID: $POSTGRES_DEST_ID"
fi

# Function to validate UUID format
validate_uuid() {
  local uuid=$1
  if [[ $uuid =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]]; then
    return 0
  else
    return 1
  fi
}

# Create Metagalaxy to PostgreSQL connection if both source and destination exist
if [ -n "$MYSQL_METAGALAXY_SOURCE_ID" ] && [ -n "$POSTGRES_DEST_ID" ]; then
  echo "[DEBUG] Creating MySQL Metagalaxy to PostgreSQL connection"
  echo "Creating MySQL Metagalaxy to PostgreSQL connection..."
  
  # Validate UUIDs
  if ! validate_uuid "$MYSQL_METAGALAXY_SOURCE_ID"; then
    echo "❌ Invalid MySQL Metagalaxy source ID format: $MYSQL_METAGALAXY_SOURCE_ID"
    echo "Expected format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
    echo "Skipping Metagalaxy to PostgreSQL connection creation"
  elif ! validate_uuid "$POSTGRES_DEST_ID"; then
    echo "❌ Invalid Postgres destination ID format: $POSTGRES_DEST_ID"
    echo "Expected format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
    echo "Skipping Metagalaxy to PostgreSQL connection creation"
  else
    # First check if the connection already exists
    echo "Checking for existing Metagalaxy to PostgreSQL connection..."
    EXISTING_CONNECTION=$(curl -s -u "$AUTH" "$AIRBYTE_API/connections/list" -H "$HEADER" -d "{\"workspaceId\": \"$WORKSPACE_ID\"}" | jq -r ".connections[] | select(.sourceId == \"$MYSQL_METAGALAXY_SOURCE_ID\" and .destinationId == \"$POSTGRES_DEST_ID\")")
    
    if [ -n "$EXISTING_CONNECTION" ]; then
      echo "✅ Metagalaxy to PostgreSQL connection already exists"
      METAGALAXY_TO_PG_CONNECTION_ID=$(echo "$EXISTING_CONNECTION" | jq -r '.connectionId')
      echo "Using existing connection ID: $METAGALAXY_TO_PG_CONNECTION_ID"
    else
      echo "Creating new Metagalaxy to PostgreSQL connection..."
      set +e
      METAGALAXY_TO_PG_CONFIG='{
        "name": "MySQL Metagalaxy to PostgreSQL",
        "sourceId": "'$MYSQL_METAGALAXY_SOURCE_ID'",
        "destinationId": "'$POSTGRES_DEST_ID'",
        "status": "active",
        "syncCatalog": {
          "streams": [
            {
              "stream": {
                "name": "customers",
                "jsonSchema": {
                  "type": "object",
                  "properties": {
                    "customer_id": {"type": "integer"},
                    "name": {"type": "string"},
                    "email": {"type": "string"}
                  }
                },
                "supportedSyncModes": ["full_refresh", "incremental"],
                "sourceDefinedCursor": false,
                "defaultCursorField": ["customer_id"]
              },
              "config": {
                "syncMode": "full_refresh",
                "cursorField": [],
                "destinationSyncMode": "append",
                "primaryKey": [["customer_id"]],
                "selected": true
              }
            },
            {
              "stream": {
                "name": "orders",
                "jsonSchema": {
                  "type": "object",
                  "properties": {
                    "order_id": {"type": "integer"},
                    "customer_id": {"type": "integer"},
                    "order_date": {"type": "string", "format": "date-time"},
                    "total_amount": {"type": "number"}
                  }
                },
                "supportedSyncModes": ["full_refresh", "incremental"],
                "sourceDefinedCursor": false,
                "defaultCursorField": ["order_id"]
              },
              "config": {
                "syncMode": "full_refresh",
                "cursorField": [],
                "destinationSyncMode": "append",
                "primaryKey": [["order_id"]],
                "selected": true
              }
            }
          ]
        },
        "scheduleType": "manual",
        "operationIds": []
      }'
      
      echo "Creating Metagalaxy to PG connection with config: $METAGALAXY_TO_PG_CONFIG"
      METAGALAXY_TO_PG_CONNECTION_RESPONSE=$(curl -s -u "$AUTH" "$AIRBYTE_API/connections/create" -H "$HEADER" -d "$METAGALAXY_TO_PG_CONFIG")
      echo "Metagalaxy to PG connection creation response: $METAGALAXY_TO_PG_CONNECTION_RESPONSE"
      METAGALAXY_TO_PG_CONNECTION_ID=$(echo "$METAGALAXY_TO_PG_CONNECTION_RESPONSE" | jq -r '.connectionId')
      
      if [ -z "$METAGALAXY_TO_PG_CONNECTION_ID" ] || [ "$METAGALAXY_TO_PG_CONNECTION_ID" == "null" ]; then
        echo "❌ Failed to create MySQL Metagalaxy to PostgreSQL connection!"
        echo "Response: $METAGALAXY_TO_PG_CONNECTION_RESPONSE"
        echo "Source ID: $MYSQL_METAGALAXY_SOURCE_ID"
        echo "Destination ID: $POSTGRES_DEST_ID"
        
        # Check if source and destination exist
        echo "Verifying source and destination..."
        SOURCE_CHECK=$(curl -s -u "$AUTH" "$AIRBYTE_API/sources/get" -H "$HEADER" -d "{\"sourceId\": \"$MYSQL_METAGALAXY_SOURCE_ID\"}")
        echo "Source check response: $SOURCE_CHECK"
        
        DEST_CHECK=$(curl -s -u "$AUTH" "$AIRBYTE_API/destinations/get" -H "$HEADER" -d "{\"destinationId\": \"$POSTGRES_DEST_ID\"}")
        echo "Destination check response: $DEST_CHECK"
        
        # Continue anyway
      else
        echo "Created MySQL Metagalaxy to PostgreSQL connection with ID: $METAGALAXY_TO_PG_CONNECTION_ID"
        
        # Function to check if a connection ID exists in the database
        check_connection_exists() {
          local connection_id=$1
          local exists=$(docker exec $AIRBYTE_DB_HOST psql -U $AIRBYTE_DB_USER -d $AIRBYTE_DB_NAME -t -c "SELECT EXISTS (SELECT 1 FROM connection WHERE id = '$connection_id');")
          if [[ $exists == *t* ]]; then
            return 0
          else
            return 1
          fi
        }

        # Function to update connection ID safely
        update_connection_id() {
          local old_id=$1
          local new_id=$2
          
          # First check if the new ID already exists
          if check_connection_exists "$new_id"; then
            echo "⚠️ Connection ID $new_id already exists, skipping update"
            return 1
          fi
          
          # Check if the old ID exists
          if ! check_connection_exists "$old_id"; then
            echo "⚠️ Connection ID $old_id does not exist, skipping update"
            return 1
          fi
          
          # Update the ID
          echo "Updating connection ID from $old_id to $new_id..."
          docker exec $AIRBYTE_DB_HOST psql -U $AIRBYTE_DB_USER -d $AIRBYTE_DB_NAME -c "UPDATE connection SET id = '$new_id' WHERE id = '$old_id';"
          return 0
        }

        # Update the connection ID update sections
        if [ -n "$METAGALAXY_TO_PG_CONNECTION_ID" ] && [ "$METAGALAXY_TO_PG_CONNECTION_ID" != "null" ]; then
          echo "Updating Metagalaxy to PostgreSQL connection ID in the database to use static ID..."
          if update_connection_id "$METAGALAXY_TO_PG_CONNECTION_ID" "$STATIC_METAGALAXY_TO_PG_CONNECTION_ID"; then
            echo "Now using static connection ID: $STATIC_METAGALAXY_TO_PG_CONNECTION_ID"
            METAGALAXY_TO_PG_CONNECTION_ID="$STATIC_METAGALAXY_TO_PG_CONNECTION_ID"
          else
            echo "Using original connection ID: $METAGALAXY_TO_PG_CONNECTION_ID"
          fi
        fi
      fi
      set -e
    fi
  fi
else
  echo "[DEBUG] Skipping MySQL Metagalaxy to PostgreSQL connection creation"
  echo "⚠️ Skipping MySQL Metagalaxy to PostgreSQL connection creation because source or destination ID is missing."
  echo "MySQL Metagalaxy Source ID: $MYSQL_METAGALAXY_SOURCE_ID"
  echo "Postgres Destination ID: $POSTGRES_DEST_ID"
fi
set -e

# Verify if connections were created
echo "[DEBUG] Verifying connections"
echo "Verifying connections..."
CONNECTIONS_RESPONSE=$(curl -s -u "$AUTH" "$AIRBYTE_API/connections/list" -H "$HEADER" -d "{\"workspaceId\": \"$WORKSPACE_ID\"}")
CONNECTIONS_COUNT=$(echo "$CONNECTIONS_RESPONSE" | jq -r '.connections | length')
echo "Found $CONNECTIONS_COUNT connections"

# Print summary of what was created
echo "========================================"
echo "SETUP SUMMARY"
echo "========================================"
echo "API Endpoint: $AIRBYTE_API"
echo "Workspace ID: $WORKSPACE_ID"
echo ""

echo "SOURCES:"
echo "  Postgres Source ID: ${POSTGRES_SOURCE_ID:-NOT CREATED}"
echo "  MySQL Source ID: ${MYSQL_SOURCE_ID:-NOT CREATED}"
echo "  MySQL Metagalaxy Source ID: ${MYSQL_METAGALAXY_SOURCE_ID:-NOT CREATED}"
echo ""

echo "DESTINATIONS:"
echo "  Postgres Destination ID: ${POSTGRES_DEST_ID:-NOT CREATED}"
echo ""

echo "CONNECTIONS:"
echo "  Postgres to Postgres: ${PG_TO_PG_CONNECTION_ID:-NOT CREATED}"
echo "  MySQL to Postgres: ${MYSQL_TO_PG_CONNECTION_ID:-NOT CREATED}"
echo "  Metagalaxy to Postgres: ${METAGALAXY_TO_PG_CONNECTION_ID:-NOT CREATED}"
echo ""

# Keep the existing code for database initialization if connections were created
if [ "$CONNECTIONS_COUNT" -gt 0 ]; then
  # Add database initialization checks
  echo "Waiting for databases to be ready..."
  max_attempts=30
  attempt=0

  # Wait for Postgres
  while true; do
    attempt=$((attempt + 1))
    echo "Checking Postgres connectivity (attempt $attempt of $max_attempts)..."
    if docker exec test-postgres pg_isready -U test -d test; then
      echo "✅ Postgres is ready!"
      break
    fi
    
    if [ $attempt -eq $max_attempts ]; then
      echo "❌ Postgres did not become ready in time"
      exit 1
    fi
    
    sleep 2
  done

  # Wait for MySQL
  attempt=0
  while true; do
    attempt=$((attempt + 1))
    echo "Checking MySQL connectivity (attempt $attempt of $max_attempts)..."
    if docker exec test-mysql mysqladmin ping -h localhost -u test -ptest; then
      echo "✅ MySQL is ready!"
      break
    fi
    
    if [ $attempt -eq $max_attempts ]; then
      echo "❌ MySQL did not become ready in time"
      exit 1
    fi
    
    sleep 2
  done

  # Initialize test data in Postgres
  echo "Initializing test data in Postgres..."
  docker exec test-postgres psql -U test -d test -c "
  CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100)
  );

  CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    order_date TIMESTAMP,
    total_amount DECIMAL(10,2)
  );

  INSERT INTO customers (name, email) VALUES 
    ('John Doe', 'john@example.com'),
    ('Jane Smith', 'jane@example.com')
  ON CONFLICT DO NOTHING;

  INSERT INTO orders (customer_id, order_date, total_amount) VALUES 
    (1, NOW(), 100.00),
    (2, NOW(), 200.00)
  ON CONFLICT DO NOTHING;
  "

  # Initialize test data in MySQL
  echo "Initializing test data in MySQL..."
  docker exec test-mysql mysql -u test -ptest test -e "
  CREATE TABLE IF NOT EXISTS customers (
    customer_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100)
  );

  CREATE TABLE IF NOT EXISTS orders (
    order_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT,
    order_date TIMESTAMP,
    total_amount DECIMAL(10,2),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
  );

  INSERT IGNORE INTO customers (name, email) VALUES 
    ('John Doe', 'john@example.com'),
    ('Jane Smith', 'jane@example.com');

  INSERT IGNORE INTO orders (customer_id, order_date, total_amount) VALUES 
    (1, NOW(), 100.00),
    (2, NOW(), 200.00);
  "
else
  echo "[DEBUG] No connections were created"
  echo "❌ No connections were created!"
  echo "Response: $CONNECTIONS_RESPONSE"
  
  # Print troubleshooting information
  echo ""
  echo "TROUBLESHOOTING INFORMATION:"
  echo "- Check if MySQL is running and accessible from Airbyte"
  echo "- Verify that the database credentials are correct"
  echo "- Check if the required schemas exist in the databases"
  
  # Check Airbyte logs
  echo ""
  echo "AIRBYTE SERVER LOGS (last 20 lines):"
  docker logs $(docker ps -q --filter name=airbyte-server) --tail 20 || echo "Could not get Airbyte server logs"
fi

# IMPORTANT: Trigger syncs at the very end of the script, regardless of previous outcomes
echo ""
echo "========================================"
echo "[DEBUG] TRIGGERING AVAILABLE SYNCS"
echo "========================================"
echo "Triggering syncs for available connections..."

# Function to safely trigger a sync for a connection
trigger_sync_if_valid() {
  local connection_id=$1
  local connection_name=$2
  
  # Skip if connection ID is null, "null", or not a valid UUID
  if [ -z "$connection_id" ] || [ "$connection_id" == "null" ] || [ "$connection_id" == "NOT CREATED" ]; then
    echo "⚠️ Skipping $connection_name sync - no valid connection ID available"
    return 1
  fi
  
  # Validate UUID format
  if ! validate_uuid "$connection_id"; then
    echo "⚠️ Skipping $connection_name sync - invalid UUID format: $connection_id"
    return 1
  fi
  
  # Trigger the sync
  echo "Triggering $connection_name sync (ID: $connection_id)..."
  local sync_response=$(curl -s -u "$AUTH" "$AIRBYTE_API/connections/sync" -H "$HEADER" -d '{"connectionId": "'"$connection_id"'"}')
  local job_id=$(echo "$sync_response" | jq -r '.job.id')
  
  if [[ -n "$job_id" && "$job_id" != "null" ]]; then
    echo "✅ Successfully started $connection_name sync job: $job_id"
    return 0
  else
    echo "❌ Failed to start $connection_name sync: $sync_response"
    return 1
  fi
}

# Attempt to trigger syncs for each connection
sync_count=0
sync_success=0

# Postgres to Postgres connection
if trigger_sync_if_valid "$PG_TO_PG_CONNECTION_ID" "Postgres -> Postgres"; then
  sync_count=$((sync_count + 1))
  sync_success=$((sync_success + 1))
else
  sync_count=$((sync_count + 1))
fi

# MySQL to Postgres connection
if trigger_sync_if_valid "$MYSQL_TO_PG_CONNECTION_ID" "MySQL -> Postgres"; then
  sync_count=$((sync_count + 1))
  sync_success=$((sync_success + 1))
else
  sync_count=$((sync_count + 1))
fi

# Metagalaxy to Postgres connection
if trigger_sync_if_valid "$METAGALAXY_TO_PG_CONNECTION_ID" "Metagalaxy -> Postgres"; then
  sync_count=$((sync_count + 1))
  sync_success=$((sync_success + 1))
else
  sync_count=$((sync_count + 1))
fi

echo "[DEBUG] Sync triggering complete"
if [ $sync_count -eq 0 ]; then
  echo "⚠️ No syncs were triggered - no valid connections found"
elif [ $sync_success -eq 0 ]; then
  echo "❌ All sync trigger attempts failed"
elif [ $sync_success -eq $sync_count ]; then
  echo "✅ All $sync_success syncs were triggered successfully"
else
  echo "⚠️ $sync_success out of $sync_count syncs were triggered successfully"
fi
echo "Check the Airbyte UI for sync status."

# Always exit with success if we made it this far
exit 0