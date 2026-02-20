---
title: "Deploying with Kubernetes"
---

# Deploying DataHub with Kubernetes

## Introduction

Helm charts for deploying DataHub on a kubernetes cluster is located in
this [repository](https://github.com/acryldata/datahub-helm). We provide charts for
deploying [DataHub](https://github.com/acryldata/datahub-helm/tree/master/charts/datahub) and
its [dependencies](https://github.com/acryldata/datahub-helm/tree/master/charts/prerequisites)
(Elasticsearch, optionally Neo4j, MySQL, and Kafka) on a Kubernetes cluster.

This doc is a guide to deploy an instance of DataHub on a kubernetes cluster using the above charts from scratch.

## Setup

1. Set up a kubernetes cluster
   - In a cloud platform of choice like [Amazon EKS](https://aws.amazon.com/eks),
     [Google Kubernetes Engine](https://cloud.google.com/kubernetes-engine),
     and [Azure Kubernetes Service](https://azure.microsoft.com/en-us/services/kubernetes-service/) OR
   - In local environment using [Minikube](https://minikube.sigs.k8s.io/docs/). Note, more than 7GB of RAM is required
     to run DataHub and its dependencies
2. Install the following tools:
   - [kubectl](https://kubernetes.io/docs/tasks/tools/) to manage kubernetes resources
   - [helm](https://helm.sh/docs/intro/install/) to deploy the resources based on helm charts. Note, we only support
     Helm 3.

## Components

DataHub consists of 4 main components: [GMS](https://docs.datahub.com/docs/metadata-service),
[MAE Consumer](https://docs.datahub.com/docs/metadata-jobs/mae-consumer-job) (optional),
[MCE Consumer](https://docs.datahub.com/docs/metadata-jobs/mce-consumer-job) (optional), and
[Frontend](https://docs.datahub.com/docs/datahub-frontend). Kubernetes deployment for each of the components are
defined as subcharts under the main
[DataHub](https://github.com/acryldata/datahub-helm/tree/master/charts/datahub)
helm chart.

The main components are powered by 4 external dependencies:

- Kafka
- Local DB (MySQL, Postgres, MariaDB)
- Search Index (Elasticsearch)
- Graph Index (Supports either Neo4j or Elasticsearch)

The dependencies must be deployed before deploying DataHub. We created a separate
[chart](https://github.com/acryldata/datahub-helm/tree/master/charts/prerequisites)
for deploying the dependencies with example configuration. They could also be deployed separately on-prem or leveraged
as managed services. To remove your dependency on Neo4j, set enabled to false in
the [values.yaml](https://github.com/acryldata/datahub-helm/blob/master/charts/prerequisites/values.yaml#L54) for
prerequisites. Then, override the `graph_service_impl` field in
the [values.yaml](https://github.com/acryldata/datahub-helm/blob/master/charts/datahub/values.yaml#L63) of datahub
instead of `neo4j`.

## Quickstart

Assuming kubectl context points to the correct kubernetes cluster, first create kubernetes secrets that contain MySQL
and Neo4j passwords.

```(shell)
kubectl create secret generic mysql-secrets --from-literal=mysql-root-password=datahub
kubectl create secret generic neo4j-secrets --from-literal=neo4j-password=datahub
```

The above commands sets the passwords to "datahub" as an example. Change to any password of choice.

Add datahub helm repo by running the following

```(shell)
helm repo add datahub https://helm.datahubproject.io/
```

Then, deploy the dependencies by running the following

```(shell)
helm install prerequisites datahub/datahub-prerequisites
```

Note, the above uses the default configuration
defined [here](https://github.com/acryldata/datahub-helm/blob/master/charts/prerequisites/values.yaml). You can change
any of the configuration and deploy by running the following command.

```(shell)
helm install prerequisites datahub/datahub-prerequisites --values <<path-to-values-file>>
```

Run `kubectl get pods` to check whether all the pods for the dependencies are running. You should get a result similar
to below.

```
NAME                                               READY   STATUS      RESTARTS   AGE
elasticsearch-master-0                             1/1     Running     0          62m
elasticsearch-master-1                             1/1     Running     0          62m
elasticsearch-master-2                             1/1     Running     0          62m
prerequisites-cp-schema-registry-cf79bfccf-kvjtv   2/2     Running     1          63m
prerequisites-kafka-0                              1/1     Running     2          62m
prerequisites-mysql-0                              1/1     Running     1          62m
prerequisites-neo4j-community-0                    1/1     Running     0          52m
prerequisites-zookeeper-0                          1/1     Running     0          62m
```

deploy DataHub by running the following

```(shell)
helm install datahub datahub/datahub
```

Values in [values.yaml](https://github.com/acryldata/datahub-helm/blob/master/charts/datahub/values.yaml)
have been preset to point to the dependencies deployed using
the [prerequisites](https://github.com/acryldata/datahub-helm/tree/master/charts/prerequisites)
chart with release name "prerequisites". If you deployed the helm chart using a different release name, update the
quickstart-values.yaml file accordingly before installing.

Run `kubectl get pods` to check whether all the datahub pods are running. You should get a result similar to below.

```
NAME                                               READY   STATUS      RESTARTS   AGE
datahub-datahub-frontend-84c58df9f7-5bgwx          1/1     Running     0          4m2s
datahub-datahub-gms-58b676f77c-c6pfx               1/1     Running     0          4m2s
datahub-datahub-mae-consumer-7b98bf65d-tjbwx       1/1     Running     0          4m3s
datahub-datahub-mce-consumer-8c57d8587-vjv9m       1/1     Running     0          4m2s
datahub-elasticsearch-setup-job-8dz6b              0/1     Completed   0          4m50s
datahub-mysql-setup-job-b57kc                      0/1     Completed   0          4m7s
elasticsearch-master-0                             1/1     Running     0          97m
elasticsearch-master-1                             1/1     Running     0          97m
elasticsearch-master-2                             1/1     Running     0          97m
prerequisites-cp-schema-registry-cf79bfccf-kvjtv   2/2     Running     1          99m
prerequisites-kafka-0                              1/1     Running     2          97m
prerequisites-mysql-0                              1/1     Running     1          97m
prerequisites-neo4j-community-0                    1/1     Running     0          88m
prerequisites-zookeeper-0                          1/1     Running     0          97m
```

You can run the following to expose the frontend locally. Note, you can find the pod name using the command above. In
this case, the datahub-frontend pod name was `datahub-datahub-frontend-84c58df9f7-5bgwx`.

```(shell)
kubectl port-forward <datahub-frontend pod name> 9002:9002
```

You should be able to access the frontend via http://localhost:9002.

Once you confirm that the pods are running well, you can set up ingress for datahub-frontend to expose the 9002 port to
the public.

## Other useful commands

| Command                | Description             |
| ---------------------- | ----------------------- |
| helm uninstall datahub | Remove DataHub          |
| helm ls                | List of Helm charts     |
| helm history           | Fetch a release history |
