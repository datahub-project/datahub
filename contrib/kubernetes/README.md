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
| file://./charts/datahub-frontend | datahub-frontend | 0.1.0 |
| file://./charts/datahub-gms | datahub-gms | 0.1.0 |
| file://./charts/datahub-mae-consumer | datahub-mae-consumer | 0.1.0 |
| file://./charts/datahub-mce-consumer | datahub-mce-consumer | 0.1.0 |

#### Chart Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| datahub-frontend.enabled | bool | `true` |  |
| datahub-frontend.image.repository | string | `"linkedin/datahub-frontend"` |  |
| datahub-frontend.image.tag | string | `"latest"` |  |
| datahub-gms.enabled | bool | `true` |  |
| datahub-gms.image.repository | string | `"linkedin/datahub-gms"` |  |
| datahub-gms.image.tag | string | `"latest"` |  |
| datahub-mae-consumer.enabled | bool | `true` |  |
| datahub-mae-consumer.image.repository | string | `"linkedin/datahub-mae-consumer"` |  |
| datahub-mae-consumer.image.tag | string | `"latest"` |  |
| datahub-mce-consumer.enabled | bool | `true` |  |
| datahub-mce-consumer.image.repository | string | `"linkedin/datahub-mce-consumer"` |  |
| datahub-mce-consumer.image.tag | string | `"latest"` |  |
| global.datahub.appVersion | string | `"1.0"` |  |
| global.datahub.gms.host | string | `"datahub-gms-deployment"` |  |
| global.datahub.gms.port | string | `"8080"` |  |
| global.datahub.gms.secret | string | `"YouKnowNothing"` |  |
| global.elasticsearch.host | string | `"elasticsearch"` |  |
| global.elasticsearch.port | string | `"9200"` |  |
| global.hostAliases[0].hostnames[0] | string | `"broker"` |  |
| global.hostAliases[0].hostnames[1] | string | `"mysql"` |  |
| global.hostAliases[0].hostnames[2] | string | `"elasticsearch"` |  |
| global.hostAliases[0].hostnames[3] | string | `"neo4j"` |  |
| global.hostAliases[0].ip | string | `"192.168.0.104"` |  |
| global.kafka.bootstrap.server | string | `"broker:29092"` |  |
| global.kafka.schemaregistry.url | string | `"http://schema-registry:8081"` |  |
| global.neo4j.password | string | `"datahub"` |  |
| global.neo4j.uri | string | `"bolt://neo4j"` |  |
| global.neo4j.username | string | `"neo4j"` |  |
| global.sql.datasource.driver | string | `"com.mysql.jdbc.Driver"` |  |
| global.sql.datasource.host | string | `"mysql"` |  |
| global.sql.datasource.password | string | `"datahub"` |  |
| global.sql.datasource.url | string | `"jdbc:mysql://mysql:3306/datahub?verifyServerCertificate=false\u0026useSSL=true"` |  |
| global.sql.datasource.username | string | `"datahub"` |  |

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


