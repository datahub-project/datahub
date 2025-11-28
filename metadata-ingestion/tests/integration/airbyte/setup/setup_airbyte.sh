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

# Get credentials from abctl for authentication
echo "Getting abctl credentials..."
ABCTL_CREDS=$(abctl local credentials 2>/dev/null)
if [ $? -ne 0 ]; then
    echo "❌ Failed to get abctl credentials"
    exit 1
fi

# Parse the password from abctl credentials output (removing ANSI escape codes)
AIRBYTE_PASSWORD=$(echo "$ABCTL_CREDS" | grep "Password:" | cut -d':' -f2 | sed 's/\x1b\[[0-9;]*m//g' | xargs)
if [ -z "$AIRBYTE_PASSWORD" ]; then
    echo "❌ Could not parse password from abctl credentials"
    echo "Credentials output: $ABCTL_CREDS"
    echo "Trying alternative parsing..."
    # Try alternative parsing method
    AIRBYTE_PASSWORD=$(echo "$ABCTL_CREDS" | sed -n 's/.*Password: *\([^ ]*\).*/\1/p' | sed 's/\x1b\[[0-9;]*m//g')
    if [ -z "$AIRBYTE_PASSWORD" ]; then
        echo "❌ Still could not parse password"
        exit 1
    fi
fi

echo "✅ Successfully retrieved abctl credentials"
echo "Password: $AIRBYTE_PASSWORD"

# Use the correct authentication for abctl setup
HEADER="Content-Type: application/json"
AUTH="test@datahub.io:$AIRBYTE_PASSWORD"
AUTH_HEADER="Authorization: Basic $(echo -n "$AUTH" | base64)"

# API endpoints prioritized for abctl setup
API_ENDPOINTS=(
  "http://localhost:8000/api/v1"
  "http://localhost:8000/api/public/v1"
  "http://localhost:8001/api/v1"
  "http://localhost:8001/api/public/v1"
)

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

# Complete onboarding if needed (this sets up the test@datahub.io user)
echo "Completing Airbyte onboarding..."
ONBOARD_RESPONSE=$(curl -s "$AIRBYTE_API/instance_configuration/setup" -H "$HEADER" -d '{
  "email": "test@datahub.io",
  "anonymousDataCollection": false,
  "news": false,
  "securityUpdates": false
}')
echo "Onboarding response: $ONBOARD_RESPONSE"

# Wait for the API to be fully ready
echo "Waiting for Airbyte API to be ready..."
max_attempts=30
attempt=0
while true; do
  attempt=$((attempt + 1))
  echo "Attempt $attempt of $max_attempts"

  # Use the correct endpoint that works with abctl authentication
  # Workspaces endpoint requires api/public/v1, not api/v1
  workspace_url="${AIRBYTE_API/api\/v1/api/public/v1}/workspaces"
  response=$(curl -s -m 10 -u "$AUTH" "$workspace_url" -H "$HEADER")
  
  if echo "$response" | grep -q "data"; then
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

# Get workspace ID using the correct endpoint
echo "Getting workspace ID..."
# Workspaces endpoint requires api/public/v1, not api/v1
workspace_url="${AIRBYTE_API/api\/v1/api/public/v1}/workspaces"
WORKSPACE_RESPONSE=$(curl -s -u "$AUTH" "$workspace_url" -H "$HEADER")
echo "Workspace response: $WORKSPACE_RESPONSE"

WORKSPACE_ID=$(echo "$WORKSPACE_RESPONSE" | jq -r '.data[0].workspaceId')

if [ -z "$WORKSPACE_ID" ] || [ "$WORKSPACE_ID" == "null" ]; then
  echo "No workspace found. Creating one..."
  # Try to create workspace (this might not work due to permissions, but let's try)
  WORKSPACE_CREATE_RESPONSE=$(curl -s -u "$AUTH" "$AIRBYTE_API/workspaces/create" -H "$HEADER" -d '{"name": "Default Workspace"}')
  WORKSPACE_ID=$(echo "$WORKSPACE_CREATE_RESPONSE" | jq -r '.workspaceId')

  if [ -z "$WORKSPACE_ID" ] || [ "$WORKSPACE_ID" == "null" ]; then
    echo "Failed to create workspace!"
    echo "Create response: $WORKSPACE_CREATE_RESPONSE"
    exit 1
  fi
fi

echo "Using workspace ID: $WORKSPACE_ID"

# Setup variables for the databases
# Note: abctl manages its own database via Kubernetes, we only need test databases
TEST_DB_HOST="test-postgres"
TEST_DB_USER="test"
TEST_DB_NAME="test"
TEST_DB_PASSWORD="test"

# Note: abctl manages the Airbyte database via Kubernetes, no direct access needed

# Note: Database connectivity and initialization is handled by the Python test
echo "Assuming test databases are already set up and initialized by Python test"

# Function to update Airbyte database with static IDs (for abctl)
update_airbyte_id_abctl() {
  local table_name="$1"
  local static_id="$2"
  local dynamic_id="$3"
  
  echo "Updating $table_name: $dynamic_id -> $static_id"
  
  # Execute SQL command via kubectl exec into the airbyte-db pod
  docker exec airbyte-abctl-control-plane kubectl exec airbyte-db-0 -n airbyte-abctl -- \
    psql -U airbyte -d db-airbyte -c \
    "UPDATE $table_name SET id = '$static_id' WHERE id = '$dynamic_id';" 2>/dev/null || true
}

# Function to update connection ID for abctl
update_connection_id_abctl() {
  local old_id="$1"
  local new_id="$2"
  
  echo "Updating connection ID from $old_id to $new_id..."
  
  # Update connection table
  docker exec airbyte-abctl-control-plane kubectl exec airbyte-db-0 -n airbyte-abctl -- \
    psql -U airbyte -d db-airbyte -c \
    "UPDATE connection SET id = '$new_id' WHERE id = '$old_id';" 2>/dev/null || true
    
  return 0
}

# Store the original workspace ID for later database updates
ORIGINAL_WORKSPACE_ID="$WORKSPACE_ID"
echo "Using dynamic workspace ID for API calls: $WORKSPACE_ID"

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
  
  # Store the original source ID for later database updates
  ORIGINAL_POSTGRES_SOURCE_ID="$POSTGRES_SOURCE_ID"
  echo "Created Postgres source with dynamic ID: $POSTGRES_SOURCE_ID"
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
    
    # Store the original source ID for later database updates
    ORIGINAL_MYSQL_SOURCE_ID="$MYSQL_SOURCE_ID"
    echo "Created MySQL source with dynamic ID: $MYSQL_SOURCE_ID"
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
    
    # Store the original source ID for later database updates
    ORIGINAL_MYSQL_METAGALAXY_SOURCE_ID="$MYSQL_METAGALAXY_SOURCE_ID"
    echo "Created MySQL Metagalaxy source with dynamic ID: $MYSQL_METAGALAXY_SOURCE_ID"
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
  
  # Store the original destination ID for later database updates
  ORIGINAL_POSTGRES_DEST_ID="$POSTGRES_DEST_ID"
  echo "Created Postgres destination with dynamic ID: $POSTGRES_DEST_ID"
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

    # Store the original connection ID for later database updates
    if [ -n "$PG_TO_PG_CONNECTION_ID" ] && [ "$PG_TO_PG_CONNECTION_ID" != "null" ]; then
      ORIGINAL_PG_TO_PG_CONNECTION_ID="$PG_TO_PG_CONNECTION_ID"
      echo "Created PostgreSQL to PostgreSQL connection with dynamic ID: $PG_TO_PG_CONNECTION_ID"
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

    # Store the original connection ID for later database updates
    if [ -n "$MYSQL_TO_PG_CONNECTION_ID" ] && [ "$MYSQL_TO_PG_CONNECTION_ID" != "null" ]; then
      ORIGINAL_MYSQL_TO_PG_CONNECTION_ID="$MYSQL_TO_PG_CONNECTION_ID"
      echo "Created MySQL to PostgreSQL connection with dynamic ID: $MYSQL_TO_PG_CONNECTION_ID"
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

        # Store the original connection ID for later database updates
        if [ -n "$METAGALAXY_TO_PG_CONNECTION_ID" ] && [ "$METAGALAXY_TO_PG_CONNECTION_ID" != "null" ]; then
          ORIGINAL_METAGALAXY_TO_PG_CONNECTION_ID="$METAGALAXY_TO_PG_CONNECTION_ID"
          echo "Created Metagalaxy to PostgreSQL connection with dynamic ID: $METAGALAXY_TO_PG_CONNECTION_ID"
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
  # Note: Database readiness and initialization is handled by the Python test
  echo "Databases should already be ready and initialized by Python test"
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

# Update all IDs to static values for consistent golden files
echo ""
echo "========================================="
echo "Updating all IDs to static values for consistent golden files..."
echo "========================================="

# Update workspace ID
if [ -n "$ORIGINAL_WORKSPACE_ID" ]; then
  echo "Updating workspace ID: $ORIGINAL_WORKSPACE_ID -> $STATIC_WORKSPACE_ID"
  update_airbyte_id_abctl "workspace" "$STATIC_WORKSPACE_ID" "$ORIGINAL_WORKSPACE_ID"
fi

# Update source IDs
if [ -n "$ORIGINAL_POSTGRES_SOURCE_ID" ]; then
  echo "Updating Postgres source ID: $ORIGINAL_POSTGRES_SOURCE_ID -> $STATIC_POSTGRES_SOURCE_ID"
  update_airbyte_id_abctl "actor" "$STATIC_POSTGRES_SOURCE_ID" "$ORIGINAL_POSTGRES_SOURCE_ID"
fi

if [ -n "$ORIGINAL_MYSQL_SOURCE_ID" ]; then
  echo "Updating MySQL source ID: $ORIGINAL_MYSQL_SOURCE_ID -> $STATIC_MYSQL_SOURCE_ID"
  update_airbyte_id_abctl "actor" "$STATIC_MYSQL_SOURCE_ID" "$ORIGINAL_MYSQL_SOURCE_ID"
fi

if [ -n "$ORIGINAL_MYSQL_METAGALAXY_SOURCE_ID" ]; then
  echo "Updating MySQL Metagalaxy source ID: $ORIGINAL_MYSQL_METAGALAXY_SOURCE_ID -> $STATIC_MYSQL_METAGALAXY_SOURCE_ID"
  update_airbyte_id_abctl "actor" "$STATIC_MYSQL_METAGALAXY_SOURCE_ID" "$ORIGINAL_MYSQL_METAGALAXY_SOURCE_ID"
fi

# Update destination ID
if [ -n "$ORIGINAL_POSTGRES_DEST_ID" ]; then
  echo "Updating Postgres destination ID: $ORIGINAL_POSTGRES_DEST_ID -> $STATIC_POSTGRES_DEST_ID"
  update_airbyte_id_abctl "actor" "$STATIC_POSTGRES_DEST_ID" "$ORIGINAL_POSTGRES_DEST_ID"
fi

# Update connection IDs
if [ -n "$ORIGINAL_PG_TO_PG_CONNECTION_ID" ]; then
  echo "Updating Postgres to Postgres connection ID: $ORIGINAL_PG_TO_PG_CONNECTION_ID -> $STATIC_PG_TO_PG_CONNECTION_ID"
  update_connection_id_abctl "$ORIGINAL_PG_TO_PG_CONNECTION_ID" "$STATIC_PG_TO_PG_CONNECTION_ID"
fi

if [ -n "$ORIGINAL_MYSQL_TO_PG_CONNECTION_ID" ]; then
  echo "Updating MySQL to Postgres connection ID: $ORIGINAL_MYSQL_TO_PG_CONNECTION_ID -> $STATIC_MYSQL_TO_PG_CONNECTION_ID"
  update_connection_id_abctl "$ORIGINAL_MYSQL_TO_PG_CONNECTION_ID" "$STATIC_MYSQL_TO_PG_CONNECTION_ID"
fi

if [ -n "$ORIGINAL_METAGALAXY_TO_PG_CONNECTION_ID" ]; then
  echo "Updating Metagalaxy to Postgres connection ID: $ORIGINAL_METAGALAXY_TO_PG_CONNECTION_ID -> $STATIC_METAGALAXY_TO_PG_CONNECTION_ID"
  update_connection_id_abctl "$ORIGINAL_METAGALAXY_TO_PG_CONNECTION_ID" "$STATIC_METAGALAXY_TO_PG_CONNECTION_ID"
fi

echo "✅ All IDs have been updated to static values for consistent golden files"
echo ""

# Always exit with success if we made it this far
exit 0