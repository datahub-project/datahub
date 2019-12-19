# DataHub
[![Build Status](https://travis-ci.org/linkedin/WhereHows.svg?branch=datahub)](https://travis-ci.org/linkedin/WhereHows)
[![Gitter](https://img.shields.io/gitter/room/nwjs/nw.js.svg)](https://gitter.im/linkedin/datahub)

![DataHub](docs/imgs/datahub-logo.png)

## Introduction
DataHub is Linkedin's generalized metadata search & discovery tool. To learn more about DataHub, check out our 
[Linkedin blog post](https://engineering.linkedin.com/blog/2019/data-hub) and [Strata presentation](https://speakerdeck.com/shirshanka/the-evolution-of-metadata-linkedins-journey-strata-nyc-2019). This repository contains the complete source code to be able to build DataHub's frontend & backend services.

## Quickstart
1. Install [docker](https://docs.docker.com/install/) and [docker-compose](https://docs.docker.com/compose/install/).
2. Clone this repo and make sure you are at the `datahub` branch.
3. Run below command to download and run all Docker containers in your local:
```
cd docker/quickstart && docker-compose pull && docker-compose up --build
```
4. After you have all Docker containers running in your machine, run below command to ingest provided sample data to DataHub:
```
./gradlew :metadata-events:mxe-schemas:build && cd metadata-ingestion/mce-cli && pip install --user -r requirements.txt && python mce_cli.py produce -d bootstrap_mce.dat
```
Note: Make sure that you're using Java 8, we have a strict dependency to Java 8 for build.

5. Finally, you can start `DataHub` by typing `http://localhost:9001` in your browser. You can sign in with `datahub`
as username and password.

## Quicklinks
* [Docker Images](docker)
* [Frontend App](datahub-frontend)
* [Generalized Metadata Store](gms)
* [Metadata Consumer Jobs](metadata-jobs)
* [Metadata Ingestion](metadata-ingestion)

## Roadmap
1. Add user profile page
2. Deploy DataHub to [Azure Cloud](https://azure.microsoft.com/en-us/)
