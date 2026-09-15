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

# Copy existing AWS SSO tokens from read-only mount to writable cache
if [ -d "/home/datahub/.aws-ro/sso/cache" ]; then
    echo "Copying existing AWS SSO tokens to writable cache..."
    mkdir -p /home/datahub/.aws/sso/cache
    cp -r /home/datahub/.aws-ro/sso/cache/* /home/datahub/.aws/sso/cache/ 2>/dev/null || true
    chmod 700 /home/datahub/.aws/sso/cache
    chmod 600 /home/datahub/.aws/sso/cache/* 2>/dev/null || true
    echo "AWS SSO token copy complete."
fi

# Wait for GMS health (replaces dockerize, which is not always present or on PATH in Wolfi images)
wait_for_gms_ready() {
  local protocol="${DATAHUB_GMS_PROTOCOL:-http}"
  local host="${DATAHUB_GMS_HOST:-datahub-gms}"
  local port="${DATAHUB_GMS_PORT:-8080}"
  local url="${protocol}://${host}:${port}/health"
  local timeout_sec="${DATAHUB_GMS_STARTUP_TIMEOUT_SEC:-240}"
  local start
  start=$(date +%s)
  echo "Waiting for GMS at ${url} (timeout ${timeout_sec}s)..."
  while true; do
    if curl -sf --connect-timeout 2 --max-time 10 "$url" >/dev/null; then
      echo "GMS is ready."
      return 0
    fi
    if [ $(($(date +%s) - start)) -ge "$timeout_sec" ]; then
      echo "Timeout waiting for GMS at $url" >&2
      return 1
    fi
    sleep 2
  done
}
wait_for_gms_ready || exit 1

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
