#!/bin/sh

echo "Generating user.props file..."
touch /datahub-frontend/conf/user.props
touch /datahub-frontend/conf/tmp.props

echo "admin:${ADMIN_PASSWORD}" >> /datahub-frontend/conf/tmp.props
if [ -n "${CUSTOM_USER_PROPS_FILE}" ]; then
  cat "${CUSTOM_USER_PROPS_FILE}" >> /datahub-frontend/conf/tmp.props
fi
# Remove empty newlines, if there are any.
sed '/^[[:space:]]*$/d' /datahub-frontend/conf/tmp.props > /datahub-frontend/conf/user.props
rm /datahub-frontend/conf/tmp.props
/datahub-frontend/bin/datahub-frontend