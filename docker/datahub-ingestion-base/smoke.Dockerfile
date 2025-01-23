FROM acryldata/datahub-ingestion-base AS base

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    sudo \
    python3-dev \
    libgtk2.0-0 \
    libgtk-3-0 \
    libgbm-dev \
    libnotify-dev \
    libgconf-2-4 \
    libnss3 \
    libxss1 \
    libasound2 \
    libxtst6 \
    xauth \
    xvfb \
    openjdk-17-jdk && \
    rm -rf /var/lib/apt/lists/* /var/cache/apk/*

COPY . /datahub-src
RUN cd /datahub-src && \
    ./gradlew :metadata-ingestion:codegen :metadata-ingestion:installDev  && \
    pip install --upgrade uv && \
    uv venv venv && \
    . venv/bin/activate && \
    uv pip install --upgrade pip wheel setuptools && \
    uv pip install 'pytest-sentry==0.3.2' && \
    cd smoke-test && \
    uv pip install -r /datahub-src/smoke-test/requirements.txt && \
    cd .. && \
    ./gradlew :smoke-test:yarnInstall