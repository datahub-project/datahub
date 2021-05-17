---
title: "Deploying with Kubernetes"
hide_title: true
---

# Deploying Datahub with Kubernetes

## Introduction
[This directory](https://github.com/linkedin/datahub/tree/master/datahub-kubernetes) provides 
the Kubernetes [Helm](https://helm.sh/) charts for deploying [Datahub](https://github.com/linkedin/datahub/tree/master/datahub-kubernetes/datahub) and it's [dependencies](https://github.com/linkedin/datahub/tree/master/datahub-kubernetes/prerequisites) 
(Elasticsearch, Neo4j, MySQL, and Kafka) on a Kubernetes cluster.  

## Setup
1. Set up a kubernetes cluster
   - In a cloud platform of choice like [Amazon EKS](https://aws.amazon.com/eks), 
     [Google Kubernetes Engine](https://cloud.google.com/kubernetes-engine), 
     and [Azure Kubernetes Service](https://azure.microsoft.com/en-us/services/kubernetes-service/) OR
   - In local environment using [Minikube](https://minikube.sigs.k8s.io/docs/). 
     Note, more than 7GB of RAM is required to run Datahub and it's dependencies 
2. Install the following tools: 
   - [kubectl](https://kubernetes.io/docs/tasks/tools/) to manage kubernetes resources
   - [helm](https://helm.sh/docs/intro/install/) to deploy the resources based on helm charts. 
     Note, we only support Helm 3.
   
## Components
Datahub consists of 4 main components: [GMS](https://datahubproject.io/docs/gms), 
[MAE Consumer](https://datahubproject.io/docs/metadata-jobs/mae-consumer-job), 
[MCE Consumer](https://datahubproject.io/docs/metadata-jobs/mce-consumer-job), and 
[Frontend](https://datahubproject.io/docs/datahub-frontend). Kubernetes deployment 
for each of the components are defined as subcharts under the main 
[Datahub](https://github.com/linkedin/datahub/tree/master/datahub-kubernetes/datahub) 
helm chart.

The main components are powered by 4 external dependencies:
- Kafka
- Local DB (MySQL, Postgres, MariaDB)
- Search Index (Elasticsearch)
- Graph Index (Supports only Neo4j)

The dependencies must be deployed before deploying Datahub. We created a separate 
[chart](https://github.com/linkedin/datahub/tree/master/datahub-kubernetes/prerequisites) 
for deploying the dependencies with example configuration. They could also be deployed 
separately on-prem or leveraged as managed services.   

## Quickstart
Assuming kubectl context points to the correct kubernetes cluster, first create kubernetes secrets that contain MySQL and Neo4j passwords. 

```(shell)
kubectl create secret generic mysql-secrets --from-literal=mysql-root-password=datahub
kubectl create secret generic neo4j-secrets --from-literal=neo4j-password=datahub
```

The above commands sets the passwords to "datahub" as an example. Change to any password of choice. 

Second, deploy the dependencies by running the following

```(shell)
(cd prerequisites && helm dep update)
helm install prerequisites prerequisites/
```

Note, after changing the configurations in the values.yaml file, you can run 

```(shell)
helm upgrade prerequisites prerequisites/
```

To just redeploy the dependencies impacted by the change. 

Run `kubectl get pods` to check whether all the pods for the dependencies are running. 
You should get a result similar to below.

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

deploy Datahub by running the following

```(shell)
helm install datahub datahub/ --values datahub/quickstart-values.yaml
```

Values in [quickstart-values.yaml](https://github.com/linkedin/datahub/tree/master/datahub-kubernetes/datahub/quickstart-values.yaml) 
have been preset to point to the dependencies deployed using the [prerequisites](https://github.com/linkedin/datahub/tree/master/datahub-kubernetes/prerequisites) 
chart with release name "prerequisites". If you deployed the helm chart using a different release name, update the quickstart-values.yaml file accordingly before installing. 

Run `kubectl get pods` to check whether all the datahub pods are running. You should get a result similar to below.

```
NAME                                               READY   STATUS      RESTARTS   AGE
datahub-datahub-frontend-84c58df9f7-5bgwx          1/1     Running     0          4m2s
datahub-datahub-gms-58b676f77c-c6pfx               1/1     Running     0          4m2s
datahub-datahub-mae-consumer-7b98bf65d-tjbwx       1/1     Running     0          4m3s
datahub-datahub-mce-consumer-8c57d8587-vjv9m       1/1     Running     0          4m2s
datahub-elasticsearch-setup-job-8dz6b              0/1     Completed   0          4m50s
datahub-kafka-setup-job-6blcj                      0/1     Completed   0          4m40s
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

You can run the following to expose the frontend locally. Note, you can find the pod name using the command above. 
In this case, the datahub-frontend pod name was `datahub-datahub-frontend-84c58df9f7-5bgwx`. 

```(shell)
kubectl port-forward <datahub-frontend pod name> 9002:9002
```

You should be able to access the frontend via http://localhost:9002. 

Once you confirm that the pods are running well, you can set up ingress for datahub-frontend 
to expose the 9002 port to the public.  
## Other useful commands

| Command | Description | 
|-----|------|
| helm uninstall datahub | Remove DataHub |
| helm ls | List of Helm charts |
| helm history | Fetch a release history | 


