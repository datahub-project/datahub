# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

echo $CYPRESS_ADMIN_USERNAME
echo $CYPRESS_ADMIN_PASSWORD
echo $CYPRESS_BASE_URL
npx cypress open --env "ADMIN_USERNAME=$CYPRESS_ADMIN_USERNAME,ADMIN_PASSWORD=$CYPRESS_ADMIN_PASSWORD"
