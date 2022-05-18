#!/bin/bash

get_lsn () {
    cat "$1" | grep to_lsn | cut -d ' ' -f 3
}

backups_by_type () {
    fgrep -l "backup_type = $1" ${target_dir}/*/xtrabackup_checkpoints
}

full_backups () {
    backups_by_type "full-backuped"
}

inc_backups () {
    backups_by_type "incremental"
}

count_backups () {
    echo "$1" | wc -l
}

find_newest_backup () {
    greatest_backup=
    greatest_lsn=0

    for backup in $1;
    do
        current_lsn=$(get_lsn ${backup})
        if [ "${current_lsn}" -gt "${greatest_lsn}" ]; then
            greatest_backup="${backup}"
            greatest_lsn="${current_lsn}"
        fi
    done;    
    echo ${greatest_backup}
}