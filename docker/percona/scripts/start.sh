#!/bin/bash

CRON_PATTERN=${CRON_SCHEDULE:-"5 5 * * *"}

CRON_FILE=/etc/cron.d/backup
mkdir -p /etc/cron.d

echo -e "${CRON_SCHEDULE} root \
/scripts/backup.sh | unbuffer -p tee -a /backup/log > /proc/1/fd/1 2>/proc/1/fd/1 \
&& /scripts/rotate.sh | unbuffer -p tee -a /backup/log > /proc/1/fd/1 2>/proc/1/fd/1 \n" > ${CRON_FILE}

printenv | cat - ${CRON_FILE} > /tmp/cron
cat /tmp/cron > ${CRON_FILE}
chmod 0644 ${CRON_FILE}

echo "starting cron to execute xtrabackup periodically (${CRON_SCHEDULE})"

cron -f