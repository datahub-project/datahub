#!/bin/bash

source $(dirname "$0")/config.sh
source $(dirname "$0")/functions.sh

echo "$(${log_prefix}) INFO: *********** starting rotation";

full_backups=$(full_backups)
inc_backups=$(inc_backups)

number_full_backups=$(count_backups "${full_backups}")
number_inc_backups=$(count_backups "${inc_backups}")

echo "$(${log_prefix}) INFO: ${number_full_backups} full, ${number_inc_backups} incremental backups found"

newest_backup=$(find_newest_backup "$full_backups")
newest_lsn=$(get_lsn ${newest_backup})

echo "$(${log_prefix}) INFO: moving backups older than $(dirname ${newest_backup}) with newest lsn: ${newest_lsn}"

#### Incremental backup rotation

# move full backups to archive if there is a newer full backups
for fulldir in ${full_backups};
do
    current_lsn=$(get_lsn ${fulldir})
    current_dir=$(dirname ${fulldir})
    if [ "${current_lsn}" -lt "${newest_lsn}" ]; then
        echo "$(${log_prefix}) INFO: ${current_dir} with lsn ${current_lsn} is older, will be moved"
        mv "${current_dir}" "${archive_dir}"
    fi
done;

echo "$(${log_prefix}) INFO: deleting incremental backups older than full backup $(dirname ${newest_backup}) with newest lsn: ${newest_lsn}"

# delete incremental backups if there is a newer full backup
for incdir in ${inc_backups};
do
    current_lsn=$(get_lsn ${incdir})
    current_dir=$(dirname ${incdir})
    if [ "${current_lsn}" -lt "${newest_lsn}" ]; then
        echo "$(${log_prefix}) INFO: ${current_dir} (incremental backup) with lsn ${current_lsn} is older, will be deleted"
        rm -rf "${current_dir}"
    fi
done;

#### Rotation of backups in archive directory

rotation () {
    days=$1
    date_check_format="$2"
    date_check_result="$3"

    case "${date_check_result:0:1}" in
        ">")
            comparison="-gt";
            date_check_result=$(echo "${date_check_result}" | tail -c +2)
            ;;
        "<")
            comparison="-lt";
            date_check_result=$(echo "${date_check_result}" | tail -c +2)
            ;;
        *)
            comparison="=";
            ;;
    esac

    echo "$(${log_prefix}) INFO: checking rotation pattern > ${days} days with date pattern ${date_check_format} ${comparison} ${date_check_result}"

    for dir in $(find ${archive_dir} -maxdepth 1 -mindepth 1 -type d -mtime +${days});
    do
        dirname=$(basename $dir);
        mtime=$(stat -c %y $dir);
        expected_dirname=$(date -d "$mtime" +${dir_date_pattern});

        # check if mtime of dir corresponds to the directory name, otherwise ignore the folder
        if [ "${dirname}" = "${expected_dirname}" ]; then
            # if given pattern doesn't match, consider it is old and no longer needed -> delete
            if [ ! "$(date +${date_check_format} -d "${mtime}")" ${comparison} "${date_check_result}" ]; then
                echo "$(${log_prefix}) INFO: ${dir} will be deleted, as it is no longer needed"
                rm -rf "${dir}"
            fi
        else 
            >&2 echo "$(${log_prefix}) ERROR: mtime ${mtime} of ${dirname} doesn't match with expected filename ${expected_dirname} -> won't be touched."
        fi
    done;

}

# call up to three date patterns which can be defined
for i in {1..3};
do
    vardays="rotation${i}_days";
    varformat="rotation${i}_date_format";
    varresult="rotation${i}_date_result";
    if [ ! -z "${!vardays}" ] && [ ! -z "${!varformat}" ] && [ ! -z "${!varresult}" ]; then
        rotation ${!vardays} "${!varformat}" "${!varresult}";
    fi
done;

echo "$(${log_prefix}) INFO: rotation finished";
