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
    pip install --upgrade uv && \
    uv venv venv --python 3.11 && \
    . venv/bin/activate && \
    uv pip install --upgrade pip wheel setuptools 'pytest-sentry==0.3.2' 'pytest-rerunfailures==15.1'

RUN cd /datahub-src/metadata-ingestion && \
    ../gradlew :metadata-ingestion:codegen && \
    cd .. && \
    . venv/bin/activate && \
    cd smoke-test && \
    uv pip install -r ./requirements.txt

RUN cd /datahub-src && \
    ./gradlew :smoke-test:yarnInstall

