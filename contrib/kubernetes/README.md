# Kubernetes  Setup for DataHub

## Introduction
This directory provides the Kubernetes setup for DataHub.  This is the first version with simple YAML files.  
The next version will contain DataHub [Helm](https://helm.sh/) chart that can be published to [Helm Hub](https://hub.helm.sh/)

## Setup
This kubernetes deployment doesn't contain the below artifacts. The idea is to use the original helm charts for deploying each of these separately.   

* Kafka and Schema Registry [Chart Link](https://github.com/confluentinc/cp-helm-charts/tree/master/charts/cp-kafka)
* Elasticsearch [Chart Link](https://hub.helm.sh/charts/elastic/elasticsearch)
* Mysql [Chart Link](https://hub.helm.sh/charts/stable/mysql)
* Neo4j [Chart Link](https://hub.helm.sh/charts/stable/neo4j)

Also, these can be installed on-prem or can be leveraged as managed service on any cloud platform.

## Quickstart
1. Install Docker and Kubernetes
2. Update the values in the configmap (datahub-configmap.yaml) with Docker hostname.  For example
```
ebean.datasource.host: "192.168.0.104:3306"
ebean.datasource.url: "jdbc:mysql://192.168.0.104:3306/datahub?verifyServerCertificate=false&useSSL=true"
kafka.bootstrap.server: "192.168.0.104:29092"
kafka.schemaregistry.url: "http://192.168.0.104:8081"
elasticsearch.host: "192.168.0.104"
neo4j.uri: "bolt://192.168.0.104"
```
3. Create the configmap by running the following 
```
kubectl apply -f datahub-configmap.yaml
```
4. Run the below kubectl command
```
cd .. && kubectl apply -f kubernetes/
```  
Please note that these steps will be updated once it is made into a Helm chart.


## Testing
For testing this setup, we can use the existing quickstart's [docker-compose](https://github.com/linkedin/datahub/blob/master/docker/quickstart/docker-compose.yml) file but commenting out `data-hub-gms`, `datahub-frontend`, `datahub-mce-consumer` & `datahub-mae-consumer` sections.
