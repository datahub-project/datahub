FROM acryldata/datahub-ingestion-base as base

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
ARG RELEASE_VERSION
RUN cd /datahub-src && \
    sed -i.bak "s/__version__ = .*$/__version__ = \"$(echo $RELEASE_VERSION|sed s/-/+/)\"/" metadata-ingestion/src/datahub/_version.py && \
    sed -i.bak "s/__version__ = .*$/__version__ = \"$(echo $RELEASE_VERSION|sed s/-/+/)\"/" metadata-ingestion-modules/airflow-plugin/src/datahub_airflow_plugin/_version.py && \
    cat metadata-ingestion/src/datahub/_version.py && \
    ./gradlew :metadata-ingestion:codegen && \
    pip install file:metadata-ingestion-modules/airflow-plugin#egg=acryl-datahub-airflow-plugin file:metadata-ingestion#egg=acryl-datahub

