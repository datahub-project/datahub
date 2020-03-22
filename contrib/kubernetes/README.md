# Kubernetes  Setup for Datahub

## Introduction
This directory provides the Kubernetes setup for Datahub.  This is the first version with simple YAML files.  
The next version will contain Datahub [Helm](https://helm.sh/) chart that can be published to [Helm Hub](https://hub.helm.sh/)

## Setup
This kubernetes deployment doesn't contain the below artifacts. The idea is to use the original helm charts for deploying each of these separately.   

* Kafka and Schema Registry [Chart Link](https://hub.helm.sh/charts/incubator/kafka)
* Elasticsearch [Chart Link](https://hub.helm.sh/charts/elastic/elasticsearch)
* Mysql [Chart Link](https://hub.helm.sh/charts/stable/mysql)
* Neo4j [Chart Link](https://hub.helm.sh/charts/stable/neo4j)

Also, these can be installed on-prem or be leveraged as managed service on any cloud platform.

## Quickstart
1. Install Docker and Kubernetes
2. Run the below kubectl commands
```
kubectl apply -f datahub-configmap.yaml
kubectl apply -f datahub-secret.yaml
kubectl apply -f datahub-gms-deployment.yaml
kubectl apply -f datahub-frontend-deployment.yaml
kubectl apply -f datahub-mce-consumer-deployment.yaml
kubectl apply -f datahub-mae-consumer-deployment.yaml
```  
Please note that these steps will be updated once it is made into a Helm chart.


## Testing
For testing this setup, we can use the existing quickstart's [docker-compose](https://github.com/linkedin/datahub/blob/master/docker/quickstart/docker-compose.yml) file but commenting out data-hub-gms, datahub-frontend, datahub-mce-consumer & datahub-mae-consumer sections.