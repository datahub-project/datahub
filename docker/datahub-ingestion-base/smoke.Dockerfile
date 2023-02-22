FROM acryldata/datahub-ingestion-base as base

RUN apt-get update && apt-get install -y \
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
    xvfb

RUN DEBIAN_FRONTEND=noninteractive apt-get install -y  openjdk-11-jdk

COPY . /datahub-src
RUN cd /datahub-src && \
    ./gradlew :metadata-ingestion:installDev
