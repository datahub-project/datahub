#!/bin/bash

incremental=${INCREMENTAL:-true}
compress_threads=${COMPRESS_THREADS:-0}
backup_dir="${BACKUP:-/backup}"
dir_date_pattern=${DIR_DATE_PATTERN:-"%Y%m%d"}

full_backup_date_format=${FULL_BACKUP_DATE_FORMAT:-"%a"}
full_backup_date_result=${FULL_BACKUP_DATE_RESULT:-"Sun"}

before_backup_script=${BEFORE_BACKUP_SCRIPT}
after_backup_script=${AFTER_BACKUP_SCRIPT}
databases_exclude="${DATABASES_EXCLUDE}"

db_user=${MYSQL_USER:-"root"}
db_password="${MYSQL_PASSWORD}"
db_host=${MYSQL_HOST:-"db"}
db_port=${MYSQL_PORT:-3306}

if [ ! -z "${MYSQL_PASSWORD_FILE}" ]; then
    db_password="$(< "${MYSQL_PASSWORD_FILE}")"
fi

rotation1_days=${ROTATION1_DAYS:-6}
rotation1_date_format=${ROTATION1_DATE_FORMAT:-"%a"}
rotation1_date_result=${ROTATION1_DATE_RESULT:-"Sun"}

rotation2_days=${ROTATION2_DAYS:-30}
rotation2_date_format=${ROTATION2_DATE_FORMAT:-"%d"}
rotation2_date_result=${ROTATION2_DATE_RESULT:-"<8"}

rotation3_days=${ROTATION3_DAYS:-365}
rotation3_date_format=${ROTATION3_DATE_FORMAT:-"%m"}
rotation3_date_result=${ROTATION3_DATE_RESULT:-"01"}

log_prefix='date +%FT%T%z'
xtrabackup=$(which xtrabackup mariabackup)

target_dir="${backup_dir}/current"
archive_dir="${backup_dir}/archive"

mkdir -p ${target_dir}
mkdir -p ${archive_dir}