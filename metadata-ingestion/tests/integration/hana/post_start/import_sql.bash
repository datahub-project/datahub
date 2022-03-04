#!/bin/bash

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
            /usr/sap/HXE/HDB90/exe/hdbsql -a -x -i 90 -d HXE -U ${tenant_store_key} -B UTF8 "IMPORT \"$SOURCE_SCHEMA\".\"*\" from '/hana/mounts/dump/$DUMP_FOLDER' WITH RENAME SCHEMA \"$SOURCE_SCHEMA\" TO \"$SCHEMA_NAME\" REPLACE THREADS 4" 2>&1
            # reset SAP Commerce admin user
            /usr/sap/HXE/HDB90/exe/hdbsql -a -x -i 90 -d HXE -U ${tenant_store_key} -B UTF8 "UPDATE \"$SCHEMA_NAME\".\"USERS\" SET PASSWD = 'nimda', P_PASSWORDENCODING = 'plain' WHERE P_UID = 'admin'" 2>&1
            ;;
    esac
}

main
