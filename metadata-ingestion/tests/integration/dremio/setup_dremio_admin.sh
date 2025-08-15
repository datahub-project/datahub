#!/bin/bash

# Set variables
DREMIO_URL="http://localhost:9047"
ADMIN_USER="admin"
ADMIN_PASSWORD="2310Admin1234!@"
ADMIN_FIRST_NAME="Admin"
ADMIN_LAST_NAME="User"
ADMIN_EMAIL="admin@dremio.com"

# Wait for Dremio to become available
until $(curl --output /dev/null --silent --head --fail "$DREMIO_URL"); do
    echo "Waiting for Dremio to start..."
    sleep 5
done

# Create admin user
echo "Creating Dremio Admin User..."
RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" -X PUT "$DREMIO_URL/apiv2/bootstrap/firstuser" \
  -H "Content-Type: application/json" \
  -d "{
        \"userName\": \"$ADMIN_USER\",
        \"firstName\": \"$ADMIN_FIRST_NAME\",
        \"lastName\": \"$ADMIN_LAST_NAME\",
        \"email\": \"$ADMIN_EMAIL\",
        \"password\": \"$ADMIN_PASSWORD\"
      }")

if [ $RESPONSE -eq 200 ]; then
    echo "Admin user created successfully!"
else
    echo "Failed to create admin user. HTTP response: $RESPONSE"
    exit 1
fi
