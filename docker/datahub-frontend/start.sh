#!/bin/sh

echo "Generating user.props file..."
touch /datahub-frontend/conf/user.props
echo "admin:${ADMIN_PASSWORD}" > /datahub-frontend/conf/user.props
/datahub-frontend/bin/playBinary