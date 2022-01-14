#!/bin/sh

echo "Generating user.props file..."
touch /datahub-frontend/conf/user.props
touch /datahub-frontend/conf/tmp.props
echo "admin:${ADMIN_PASSWORD}\n" >> /datahub-frontend/conf/tmp.props
cat /datahub-frontend/conf/user.props >> /datahub-frontend/conf/tmp.props
# Remove empty newlines, if there are any.
sed '/^[[:space:]]*$/d' /datahub-frontend/conf/tmp.props > /datahub-frontend/conf/user.props
rm /datahub-frontend/conf/tmp.props
/datahub-frontend/bin/playBinary