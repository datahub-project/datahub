# Kubernetes  Setup for DataHub

## Introduction
This directory provides the Kubernetes [Helm](https://helm.sh/) charts for DataHub. 

## Setup
This kubernetes deployment doesn't contain the below artifacts. The idea is to use the original helm charts for deploying each of these separately.   

* Kafka and Schema Registry [Chart Link](https://hub.helm.sh/charts/incubator/kafka)
* Elasticsearch [Chart Link](https://hub.helm.sh/charts/elastic/elasticsearch)
* Mysql [Chart Link](https://hub.helm.sh/charts/stable/mysql)
* Neo4j [Chart Link](https://hub.helm.sh/charts/stable/neo4j)

Also, these can be installed on-prem or can be leveraged as managed service on any cloud platform.

## Quickstart

### Docker & Kubernetes
Install Docker & Kubernetes by following the instructions [here](https://kubernetes.io/docs/setup/).  Easiest is to use Docker Desktop for your platform [Mac](https://docs.docker.com/docker-for-mac/) & [Windows](https://docs.docker.com/docker-for-windows/)

### Helm
Helm is an open-source packaging tool that helps you install applications and services on kubernetes. Helm uses a packaging format called charts. Charts are a collection of YAML templates that describes a related set of kubernetes resources.

Install helm by following the instructions [here](https://helm.sh/docs/intro/install/).  We support Helm3 version.

### DataHub Helm Chart Configurations

The following table lists the configuration parameters and its default values

#### Chart Requirements

| Repository | Name | Version |
|------------|------|---------|
| file://./charts/datahub-frontend | datahub-frontend | 0.2.0 |
| file://./charts/datahub-gms | datahub-gms | 0.2.0 |
| file://./charts/datahub-mae-consumer | datahub-mae-consumer | 0.2.0 |
| file://./charts/datahub-mce-consumer | datahub-mce-consumer | 0.2.0 |

## Install DataHub
Navigate to the current directory and run the below command.  Update the `datahub/values.yaml` file with valid hostname/IP address configuration for elasticsearch, neo4j, schema-registry, broker & mysql. 

``
helm install datahub datahub/
``

## Testing
For testing this setup, we can use the existing quickstart's [docker-compose](https://github.com/linkedin/datahub/blob/master/docker/quickstart/docker-compose.yml) file but commenting out `data-hub-gms`, `datahub-frontend`, `datahub-mce-consumer` & `datahub-mae-consumer` sections for setting up prerequisite software
and then performing helm install by updating the values.yaml with proper IP address of Host Machine for elasticsearch, neo4j, schema-registry, broker & mysql in `global.hostAliases[0].ip` section.  


Alternatively, you can run this command directly without making any changes to `datahub/values.yaml` file
``
helm install --set "global.hostAliases[0].ip"="<<docker_host_ip>>","global.hostAliases[0].hostnames"="{broker,mysql,elasticsearch,neo4j}" datahub datahub/
`` 

## Other useful commands

| Command | Description | 
|-----|------|
| helm uninstall datahub | Remove DataHub |
| helm ls | List of Helm charts |
| helm history | Fetch a release history | 


