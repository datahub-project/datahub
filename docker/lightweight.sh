#!/bin/bash

set -euo pipefail

# Define color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Print banner
echo -e "${GREEN}==========================================${NC}"
echo -e "${GREEN}  DataHub Lightweight Setup${NC}"
echo -e "${GREEN}  Using: Redpanda, MySQL/MariaDB, Elasticsearch${NC}"
echo -e "${GREEN}==========================================${NC}"

# Check Docker is running
if ! docker info > /dev/null 2>&1; then
  echo -e "${RED}Error: Docker is not running. Please start Docker and try again.${NC}"
  exit 1
fi

# Check for M1/M2 Mac and set appropriate MySQL image
if [[ $(uname -m) == 'arm64' ]]; then
  export MYSQL_IMAGE="mariadb:10.5.8"
  export MYSQL_PLATFORM="linux/arm64"
  echo -e "${YELLOW}M1/M2 Mac detected. Using MariaDB instead of MySQL.${NC}"
fi

# Parse command-line arguments
FRONTEND=true
for arg in "$@"; do
  case $arg in
    --no-frontend)
      FRONTEND=false
      shift
      ;;
    --help)
      echo "Usage: $0 [OPTIONS]"
      echo "Options:"
      echo "  --no-frontend  Start without the frontend"
      echo "  --help         Display this help message"
      exit 0
      ;;
  esac
done

# Configure services based on command-line arguments
SERVICES="redpanda elasticsearch elasticsearch-setup mysql mysql-setup kafka-setup datahub-upgrade datahub-gms"
if [ "$FRONTEND" = true ]; then
  SERVICES="$SERVICES datahub-frontend-react"
fi

# Print configuration
echo -e "${YELLOW}Starting with configuration:${NC}"
echo -e "- Frontend: $([ "$FRONTEND" = true ] && echo 'Enabled' || echo 'Disabled')"
echo ""

# Start the services
echo -e "${YELLOW}Starting services: $SERVICES${NC}"
docker-compose -f docker-compose.lightweight.yml up -d $SERVICES

echo ""
echo -e "${GREEN}DataHub is starting up...${NC}"
echo -e "${YELLOW}Waiting for GMS to be healthy...${NC}"

# Wait for GMS to be ready
GMS_URL="http://localhost:8080/health"
MAX_WAIT=300  # 5 minutes
START_TIME=$(date +%s)

while true; do
  CURRENT_TIME=$(date +%s)
  ELAPSED=$((CURRENT_TIME - START_TIME))
  
  if [ $ELAPSED -gt $MAX_WAIT ]; then
    echo -e "${RED}GMS did not become healthy within the timeout period.${NC}"
    echo -e "${YELLOW}Check logs with: docker-compose -f docker-compose.lightweight.yml logs datahub-gms${NC}"
    exit 1
  fi
  
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$GMS_URL" || echo "000")
  
  if [ "$STATUS" = "200" ]; then
    echo -e "${GREEN}GMS is healthy!${NC}"
    break
  fi
  
  echo -e "${YELLOW}Waiting for GMS to be ready... (${ELAPSED}s)${NC}"
  sleep 5
done

# Display success message
echo -e "${GREEN}==========================================${NC}"
echo -e "${GREEN}DataHub is now running!${NC}"
echo ""
echo -e "${YELLOW}Access points:${NC}"
echo -e "- GMS API:   http://localhost:8080"
if [ "$FRONTEND" = true ]; then
  echo -e "- Frontend:  http://localhost:9002"
fi
echo ""
echo -e "${YELLOW}Useful commands:${NC}"
echo -e "- View logs:  docker-compose -f docker-compose.lightweight.yml logs -f"
echo -e "- Stop:       docker-compose -f docker-compose.lightweight.yml down"
echo -e "${GREEN}==========================================${NC}"