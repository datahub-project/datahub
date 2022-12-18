# Defining environment
ARG APP_ENV=prod

FROM acryldata/datahub-ingestion-base as base

FROM openjdk:11 as prod-build
COPY . /datahub-src
# We noticed that the gradle wrapper download failed frequently on in CI on arm64 machines.
# I suspect this was due because of the QEMU emulation slowdown, combined with the arm64
# build being starved for CPU by the x86_64 build's codegen step.
#
# The middle step will attempt to download gradle wrapper 5 times with exponential backoff.
# The ./gradlew --version will force the download of the gradle wrapper but is otherwise a no-op.
# Note that the retry logic will always return success, so we should always attempt to run codegen.
# Inspired by https://github.com/gradle/gradle/issues/18124#issuecomment-958182335.
# and https://unix.stackexchange.com/a/82610/378179.
# This is a workaround for https://github.com/gradle/gradle/issues/18124.
RUN cd /datahub-src && \
    (for attempt in 1 2 3 4 5; do ./gradlew --version && break ; echo "Failed to download gradle wrapper (attempt $attempt)" && sleep $((2<<$attempt)) ; done ) && \
    ./gradlew :metadata-events:mxe-schemas:build

FROM base as prod-codegen
COPY --from=prod-build /datahub-src /datahub-src
RUN cd /datahub-src/metadata-ingestion && \
    pip install -e ".[base]" && \
    ./scripts/codegen.sh

FROM base as prod-install
COPY --from=prod-codegen /datahub-src/metadata-ingestion /datahub-ingestion
COPY --from=prod-codegen /root/.cache/pip /root/.cache/pip
ARG RELEASE_VERSION
RUN cd /datahub-ingestion && \
    sed -i.bak "s/__version__ = \"0.0.0.dev0\"/__version__ = \"$RELEASE_VERSION\"/" src/datahub/__init__.py && \
    cat src/datahub/__init__.py && \
    pip install ".[all]" && \
    pip freeze

FROM base as dev-install
# Dummy stage for development. Assumes code is built on your machine and mounted to this image.
# See this excellent thread https://github.com/docker/cli/issues/1134

FROM ${APP_ENV}-install as final

RUN addgroup --system datahub && adduser --system datahub --ingroup datahub
USER datahub

ENTRYPOINT [ "datahub" ]
