<<<<<<< HEAD
# DataHub: A Generalized Metadata Search & Discovery Tool
[![Version](https://img.shields.io/github/v/release/linkedin/datahub?include_prereleases)](https://github.com/linkedin/datahub/releases)
[![Build Status](https://travis-ci.org/linkedin/datahub.svg)](https://travis-ci.org/linkedin/datahub)
[![License](https://img.shields.io/github/license/linkedin/datahub)](LICENSE)
[![Gitter](https://img.shields.io/gitter/room/nwjs/nw.js.svg)](https://gitter.im/linkedin/datahub)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/linkedin/datahub/blob/master/CONTRIBUTING.md)

![DataHub](docs/imgs/datahub-logo.png)

> :sparkles:Feb 2020 Update:
> - Our [blog post](https://engineering.linkedin.com/blog/2020/open-sourcing-datahub--linkedins-metadata-search-and-discovery-p) on open sourcing DataHub is out!
> - *DataHub v0.3.0* is [released](https://github.com/linkedin/datahub/releases/tag/v0.3.0)!

## Introduction
DataHub is LinkedIn's generalized metadata search & discovery tool. To learn more about DataHub, check out our
[LinkedIn blog post](https://engineering.linkedin.com/blog/2019/data-hub) and [Strata presentation](https://speakerdeck.com/shirshanka/the-evolution-of-metadata-linkedins-journey-strata-nyc-2019).
You should also visit [DataHub Architecture](docs/architecture/architecture.md) to get a better understanding of how DataHub is implemented and [DataHub Onboarding Guide](docs/how/entity-onboarding.md) to understand how to extend DataHub for your own use case.

This repository contains the complete source code for both DataHub's frontend & backend. You can also read about [how we sync the changes](https://engineering.linkedin.com/blog/2020/open-sourcing-datahub--linkedins-metadata-search-and-discovery-p) between our the internal fork and GitHub.

## Quickstart
1. Install [docker](https://docs.docker.com/install/) and [docker-compose](https://docs.docker.com/compose/install/). Make sure to configure Docker to allocate enough hardware resources for Docker engine. Tested & confirmed config: 4 CPUs, 8GB RAM, 2GB Swap area.
2. Open Docker either from the command line or the Desktop app and ensure it is up and running.
3. Clone this repo and `cd` into the root directory for the cloned repository.
4. Run below command to download and run all Docker containers in your local:
    ```
    cd docker/quickstart && docker-compose pull && docker-compose up --build
    ```
    This step takes long time and it might be hard to figure out when DataHub is fully up. You can refer to [this guide](https://github.com/linkedin/datahub/blob/master/docs/debugging.md#how-can-i-confirm-if-all-docker-containers-are-running-as-expected-after-a-quickstart) to verify if DataHub is up and running.
5. At this point, you should be able to start `DataHub` by opening [http://localhost:9001](http://localhost:9001) in your browser. You can sign in using `datahub` as both username and password. However, there is no data just yet.
6. To ingest [provided](https://github.com/linkedin/datahub/blob/master/metadata-ingestion/mce-cli/bootstrap_mce.dat) sample data to DataHub, switch to a new terminal, `cd` into the cloned `datahub` repo, and run below command:
    ```
    docker build -t ingestion -f docker/ingestion/Dockerfile . && cd docker/ingestion && docker-compose up
    ```
    After running this, you should be able to see sample data in DataHub.

Refer to [debugging guide](docs/debugging.md) if you have issues in any of the above steps.

## Quicklinks
* [DataHub Architecture](docs/architecture/architecture.md)
* [DataHub Onboarding Guide](docs/how/entity-onboarding.md)
* [Docker Images](docker)
* [Frontend App](datahub-frontend)
* [Web Client App](datahub-web)
* [Generalized Metadata Service](gms)
* [Metadata Consumer Jobs](metadata-jobs)
* [Metadata Ingestion](metadata-ingestion)

## Releases
See [Releases](https://github.com/linkedin/datahub/releases) page for more details.

## Contributing
We welcome contributions from the community. Please refer to [the guidelines](CONTRIBUTING.md) for more details. We also have a [contrib](contrib) directory for incubation.
<<<<<<< Updated upstream
=======XZXZ
=======
>>>>>>> Stashed changes
## ⚠️ WhereHows is no longer being maintained. Please use [DataHub](https://github.com/linkedin/datahub) instead for all new production use cases.


# WhereHows [![Build Status](https://travis-ci.org/linkedin/WhereHows.svg?branch=master)](https://travis-ci.org/linkedin/WhereHows) [![latest](https://img.shields.io/badge/latest-1.0.0-blue.svg)](https://github.com/linkedin/WhereHows/releases) [![Gitter](https://img.shields.io/gitter/room/nwjs/nw.js.svg)](https://gitter.im/wherehows) [![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/LinkedIn/Wherehows/wiki/Contributing)

WhereHows is a data discovery and lineage tool built at LinkedIn. It integrates with all the major data processing systems and collects both catalog and operational metadata from them.

Within the central metadata repository, WhereHows curates, associates, and surfaces the metadata information through two interfaces:
* a web application that enables data & linage discovery, and community collaboration
* an API endpoint that empowers automation of data processes/applications

WhereHows serves as the single platform that:
* links data objects with people and processes
* enables crowdsourcing for data knowledge
* provides data governance and provenance based on ownership and lineage


## Who Uses WhereHows?
Here is a list of companies known to use WhereHows. Let us know if we missed your company!

* [LinkedIn](http://www.linkedin.com)
* [Overstock.com](http://www.overstock.com)
* [Fitbit](http://www.fitbit.com)
* [Carbonite](https://www.carbonite.com)


## How Is WhereHows Used?
How WhereHows is used inside of LinkedIn and other potential [use cases][USE].


## Documentation
The detailed information can be found in the [Wiki][wiki]


## Examples in VM (Deprecated)
There is a pre-built vmware image (about 11GB) to quickly demonstrate the functionality of WhereHows. Check out the [VM Guide][VM]
## WhereHows Docker
Docker can provide configuration free dev/production setup quickly, please check out [Docker Getting Start Guide](wherehows-docker/README.md)

## Getting Started
New to Wherehows? Check out the [Getting Started Guide][GS]


### Preparation
First, please [setup the metadata repository][DB] in MySQL.
```
CREATE DATABASE wherehows
  DEFAULT CHARACTER SET utf8
  DEFAULT COLLATE utf8_general_ci;

CREATE USER 'wherehows';
SET PASSWORD FOR 'wherehows' = PASSWORD('wherehows');
GRANT ALL ON wherehows.* TO 'wherehows'
```

Execute the [DDL files][DDL] to create the required repository tables in **wherehows** database.

### Build
1. Get the source code: ```git clone https://github.com/linkedin/WhereHows.git```
2. Put a few 3rd-party jar files to **wherehows-etl/extralibs** directory. Some of these jar files may not be available in Maven Central or Artifactory. See [the download instrucitons][EXJAR] for more detail. ```cd WhereHows/wherehows-etl/extralibs```
3. From the **WhereHows** root directory and build all the modules: ```./gradlew build```
4. Start the metadata ETL and API service: ```./gradlew wherehows-backend:runPlayBinary```
5. In a new terminal, start the web front-end: ```./gradlew wherehows-frontend:runPlayBinary```. The WhereHows UI is available at http://localhost:9001 by default. You can change the port number by editing the value of ```project.ext.httpPort``` in ```wherehows-frontend/build.gradle```.

>>>>>>> wherehows

## Roadmap
Check out DataHub's [roadmap](docs/roadmap.md).
