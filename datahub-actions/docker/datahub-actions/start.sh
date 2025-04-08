#!/bin/bash

# Copyright 2021 Acryl Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

SYS_CONFIGS_PATH="${DATAHUB_ACTIONS_SYSTEM_CONFIGS_PATH:-/etc/datahub/actions/system/conf}"
USER_CONFIGS_PATH="${DATAHUB_ACTIONS_USER_CONFIGS_PATH:-/etc/datahub/actions/conf}"
MONITORING_ENABLED="${DATAHUB_ACTIONS_MONITORING_ENABLED:-false}"
MONITORING_PORT="${DATAHUB_ACTIONS_MONITORING_PORT:-8000}"

touch /tmp/datahub/logs/actions/actions.out

# Deploy System Actions
if [ "$(ls -A ${SYS_CONFIGS_PATH}/)" ]; then
    config_files=""
    # .yml
    for file in ${SYS_CONFIGS_PATH}/*.yml;
    do
        if [ -f "$file" ]; then
            config_files+="-c $file "
        fi
    done
    #.yaml
    for file in ${SYS_CONFIGS_PATH}/*.yaml;
    do
        if [ -f "$file" ]; then
            config_files+="-c $file "
        fi
    done
else
    echo "No system action configurations found. Not starting system actions."
fi

# Deploy User Actions
if [ "$(ls -A ${USER_CONFIGS_PATH}/)" ]; then
    # .yml
    for file in ${USER_CONFIGS_PATH}/*.yml;
    do
        if [ -f "$file" ]; then
            config_files+="-c $file "
        fi
    done
    #.yaml
    for file in ${USER_CONFIGS_PATH}/*.yaml;
    do
        if [ -f "$file" ]; then
            config_files+="-c $file "
        fi
    done
else
    echo "No user action configurations found. Not starting user actions."
fi

if [ "$MONITORING_ENABLED" = true ]; then
    datahub-actions --enable-monitoring --monitoring-port "$MONITORING_PORT" actions $config_files
else
    datahub-actions actions $config_files
fi
