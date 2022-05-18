#!/bin/bash

source $(dirname "$0")/config.sh
source $(dirname "$0")/functions.sh

echo "$(${log_prefix}) INFO: *********** starting backup";

current_dir="${target_dir}/$(date +${dir_date_pattern})"
run_incremental=false

if [ ! -z "${before_backup_script}" ]; then
    echo "$(${log_prefix}) INFO: call before backup script"
    ${before_backup_script} "${current_dir}"
fi

if [ "${incremental}" = true ]; then
    #check if a full backup has to be done again
    if [ "$(date +${full_backup_date_format})" = "${full_backup_date_result}" ]; then
        echo "$(${log_prefix}) INFO: Full backup condition true, so let's do a full backup"
    else
        #find directory of previous backup
        full_backups=$(full_backups)
        newest_backup=$(find_newest_backup "${full_backups}")
        current_base_dir=$(dirname "${newest_backup}")
        if [ -d "${current_base_dir}" ] && [ -f "${current_base_dir}/xtrabackup_checkpoints" ]; then
            echo "$(${log_prefix}) INFO: using ${current_base_dir} as a base for the incremental backup"
            run_incremental=true
        else
            >&2 echo "$(${log_prefix}) ERROR: ${current_base_dir} can't be used as a base dir for an incremental backup, doing a full backup"
        fi
    fi
fi

mkdir -p "${current_dir}"
printf "$(${log_prefix}) INFO: starting";

if [ "${run_incremental}" = true ]; then
    OPT="--incremental-basedir=${current_base_dir} --target-dir=${current_dir}"
    printf " incremental backup"
else
    OPT="--target-dir=${current_dir}"
    printf " full backup"
fi

if [ ${compress_threads} -gt 0 ]; then
    OPT="--compress --compress-threads=${compress_threads} ${OPT}"
    echo " with compression enabled"
fi

if [ ! -z "${databases_exclude}" ]; then
    OPT="--databases-exclude=\"${databases_exclude}\" ${OPT}"
    echo " with databases exclude"
fi

command="${xtrabackup} --backup \
     --user=${db_user} \
     --password=\"${db_password}\" \
     --host=${db_host} \
     --port=${db_port} \
     ${OPT}"

tmp_output=$(mktemp)
eval ${command} 2>&1 | tee ${tmp_output}

if [ ! -z "${after_backup_script}" ]; then
    echo "$(${log_prefix}) INFO: call after backup script"
    output=$(cat ${tmp_output})
    if [[ ${output} =~ "completed OK!" ]]
    then
        status="succeed"
    else
        status="failed"
    fi
    ${after_backup_script} ${status} "${output}" "${current_dir}" 
fi
rm ${tmp_output}

echo "$(${log_prefix}) INFO: backup process finished"
