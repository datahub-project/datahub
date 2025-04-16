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

# Defining environment
ARG BASE_IMAGE=acryldata/datahub-ingestion-base
ARG DOCKER_VERSION=head-full
ARG APP_ENV=prod

FROM $BASE_IMAGE:$DOCKER_VERSION AS base

USER root

COPY --chown=datahub:datahub ./docker/datahub-actions/start.sh /start_datahub_actions.sh
COPY --chown=datahub:datahub ./docker/datahub-actions/readiness-check.sh /readiness-check.sh

RUN chmod a+x /start_datahub_actions.sh && \
    mkdir -p /etc/datahub/actions && \
    mkdir -p /tmp/datahub/logs/actions/system && \
    chown -R datahub:datahub /etc/datahub /tmp/datahub && \
    apt-get update && \
    apt-get install -y -qq default-jre && \
    apt-get clean && \
    rm -rf /var/lib/{apt,dpkg,cache,log}/

COPY --chown=datahub:datahub ./datahub-actions /actions-src
# Add other default configurations into this!
COPY --chown=datahub:datahub ./docker/datahub-actions/config /etc/datahub/actions/system/conf

USER datahub
WORKDIR /actions-src

FROM base AS slim-install

# Effectively builds the image without the .cache (looks like it is duplicated but is not)
RUN --mount=type=cache,target=/datahub-ingestion/.cache/uv,uid=1000,gid=1000 \
    UV_LINK_MODE=copy uv pip install -e ".[all]"

FROM base AS prod-install

RUN --mount=type=cache,target=/datahub-ingestion/.cache/uv,uid=1000,gid=1000 \
    UV_LINK_MODE=copy uv pip install -e ".[all]"

FROM ${APP_ENV}-install AS final
WORKDIR /datahub-ingestion

# This is required to fix security vulnerability in htrace-core4
RUN find . -name "htrace-core4-4.1.0-incubating.jar" -exec rm "{}" \;

ENTRYPOINT [ ]
CMD dockerize -wait ${DATAHUB_GMS_PROTOCOL:-http}://$DATAHUB_GMS_HOST:$DATAHUB_GMS_PORT/health -timeout 240s /start_datahub_actions.sh