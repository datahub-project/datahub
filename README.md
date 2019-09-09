# Data Hub
[![Build Status](https://travis-ci.org/linkedin/WhereHows.svg?branch=datahub)](https://travis-ci.org/linkedin/WhereHows)
[![Gitter](https://img.shields.io/gitter/room/nwjs/nw.js.svg)](https://gitter.im/linkedin/datahub)

![Data Hub](docs/imgs/datahublogo.png)

## Introduction
Data Hub is Linkedin's generalized metadata search & discovery tool. Check out the 
[Linkedin blog post](https://engineering.linkedin.com/blog/2019/data-hub) about Data Hub. This repository is a monorepo 
which contains complete source code to be able to build Data Hub's frontend & backend services.

## Quickstart
1. To get a quick taste of Data Hub, check [Docker Quickstart Guide](docker/quickstart) first.
2. After you have all Docker containers running in your machine, you can ingest sample data by following 
[Data Hub Ingestion Guide](metadata-ingestion).
3. Finally, you can start `Data Hub` by typing `http://localhost:9001` in your browser. You can sign in with `datahub`
as username and password.

## Quicklinks
* [Docker Images](docker)
* [Frontend App](datahub-frontend)
* [Generalized Metadata Store](gms)
* [Metadata Consumer Jobs](metadata-jobs)
* [Metadata Ingestion](metadata-ingestion)

## Roadmap
1. Add [Neo4J](http://neo4j.com) graph query support 
2. Add user profile page
3. Deploy Data Hub to [Azure Cloud](https://azure.microsoft.com/en-us/)