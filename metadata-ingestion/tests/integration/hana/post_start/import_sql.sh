#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.


set -euo pipefail

#found in /run_hana.sh, hxe_optimize.sh
#durinng the 'initial' phase there is key for SYSTEM available
declare -r tenant_store_key=us_key_tenantdb

# import dump
function main() {
    case "$_HOOK_START_TYPE" in
        initial)
            # create user
            /usr/sap/HXE/HDB90/exe/hdbsql -a -x -i 90 -d HXE -U ${tenant_store_key} -B UTF8 "CREATE USER $SCHEMA_NAME PASSWORD \"$SCHEMA_PWD\" NO FORCE_FIRST_PASSWORD_CHANGE" 2>&1
            /usr/sap/HXE/HDB90/exe/hdbsql -a -x -i 90 -d HXE -U ${tenant_store_key} -B UTF8 "ALTER USER $SCHEMA_NAME DISABLE PASSWORD LIFETIME" 2>&1
            # import dump
            /usr/sap/HXE/HDB90/exe/hdbsql -a -x -i 90 -d HXE -U ${tenant_store_key} -B UTF8 -I "/hana/mounts/setup/$DUMP_FILE" 2>&1
            ;;
    esac
}
main
