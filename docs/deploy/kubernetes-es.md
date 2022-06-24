***

## título: "Implementación con Kubernetes"

# Implementación de DataHub con Kubernetes

## Introducción

Los gráficos de Helm para implementar DataHub en un clúster de kubernetes se encuentran en
éste [depósito](https://github.com/acryldata/datahub-helm). Proporcionamos gráficos para
Implementar [Centro de datos](https://github.com/acryldata/datahub-helm/tree/master/charts/datahub) y
es [Dependencias](https://github.com/acryldata/datahub-helm/tree/master/charts/prerequisites)
(Elasticsearch, opcionalmente Neo4j, MySQL y Kafka) en un clúster de Kubernetes.

Este documento es una guía para implementar una instancia de DataHub en un clúster de kubernetes utilizando los gráficos anteriores desde cero.

## Arreglo

1.  Configurar un clúster de kubernetes
    *   En una plataforma en la nube de elección como [Amazon EKS](https://aws.amazon.com/eks),
        [Motor de Google Kubernetes](https://cloud.google.com/kubernetes-engine),
        y [Azure Kubernetes Service](https://azure.microsoft.com/en-us/services/kubernetes-service/) O
    *   En el entorno local utilizando [Minikube](https://minikube.sigs.k8s.io/docs/). Tenga en cuenta que se requieren más de 7 GB de RAM
        para ejecutar Datahub y sus dependencias
2.  Instale las siguientes herramientas:
    *   [kubectl](https://kubernetes.io/docs/tasks/tools/) para administrar los recursos de Kubernetes
    *   [timón](https://helm.sh/docs/intro/install/) para implementar los recursos basados en gráficos de timón. Tenga en cuenta que solo admitimos
        Timón 3.

## Componentes

Datahub consta de 4 componentes principales: [GMS](https://datahubproject.io/docs/metadata-service),
[Consumidor MAE](https://datahubproject.io/docs/metadata-jobs/mae-consumer-job) (opcional),
[Consumidor de MCE](https://datahubproject.io/docs/metadata-jobs/mce-consumer-job) (opcional), y
[Frontend](https://datahubproject.io/docs/datahub-frontend). La implementación de Kubernetes para cada uno de los componentes son
definidas como subgráficos en el principal
[Centro de datos](https://github.com/acryldata/datahub-helm/tree/master/charts/datahub)
Gráfico de timón.

Los componentes principales están alimentados por 4 dependencias externas:

*   Kafka
*   Base de datos local (MySQL, Postgres, MariaDB)
*   Índice de búsqueda (Elasticsearch)
*   Graph Index (Compatible con Neo4j o Elasticsearch)

Las dependencias deben implementarse antes de implementar Datahub. Creamos un
[gráfico](https://github.com/acryldata/datahub-helm/tree/master/charts/prerequisites)
para implementar las dependencias con configuración de ejemplo. También podrían desplegarse por separado en las instalaciones o apalancarse.
como servicios gestionados. Para eliminar la dependencia de Neo4j, establezca habilitado en false en
el [valores.yaml](https://github.com/acryldata/datahub-helm/blob/master/charts/prerequisites/values.yaml#L54) para
prerrequisitos. A continuación, anule el botón `graph_service_impl` campo en
el [valores.yaml](https://github.com/acryldata/datahub-helm/blob/master/charts/datahub/values.yaml#L63) de datahub
En lugar de `neo4j`.

## Inicio rápido

Suponiendo que el contexto de kubectl apunte al clúster de kubernetes correcto, primero cree secretos de kubernetes que contengan MySQL
y contraseñas Neo4j.

```(shell)
kubectl create secret generic mysql-secrets --from-literal=mysql-root-password=datahub
kubectl create secret generic neo4j-secrets --from-literal=neo4j-password=datahub
```

Los comandos anteriores establecen las contraseñas en "datahub" como ejemplo. Cambie a cualquier contraseña de su elección.

Agregue datahub helm repo ejecutando lo siguiente

```(shell)
helm repo add datahub https://helm.datahubproject.io/
```

A continuación, implemente las dependencias ejecutando lo siguiente

```(shell)
helm install prerequisites datahub/datahub-prerequisites
```

Tenga en cuenta que lo anterior utiliza la configuración predeterminada
definido [aquí](https://github.com/acryldata/datahub-helm/blob/master/charts/prerequisites/values.yaml). Puedes cambiar
cualquiera de las configuraciones e implementaciones ejecutando el siguiente comando.

```(shell)
helm install prerequisites datahub/datahub-prerequisites --values <<path-to-values-file>>
```

Correr `kubectl get pods` para comprobar si todos los pods de las dependencias se están ejecutando. Debería obtener un resultado similar
a continuación.

    NAME                                               READY   STATUS      RESTARTS   AGE
    elasticsearch-master-0                             1/1     Running     0          62m
    elasticsearch-master-1                             1/1     Running     0          62m
    elasticsearch-master-2                             1/1     Running     0          62m
    prerequisites-cp-schema-registry-cf79bfccf-kvjtv   2/2     Running     1          63m
    prerequisites-kafka-0                              1/1     Running     2          62m
    prerequisites-mysql-0                              1/1     Running     1          62m
    prerequisites-neo4j-community-0                    1/1     Running     0          52m
    prerequisites-zookeeper-0                          1/1     Running     0          62m

Implemente Datahub ejecutando lo siguiente

```(shell)
helm install datahub datahub/datahub
```

Valores en [valores.yaml](https://github.com/acryldata/datahub-helm/blob/master/charts/datahub/values.yaml)
se han preestablecido para que apunten a las dependencias implementadas mediante
el [prerrequisitos](https://github.com/acryldata/datahub-helm/tree/master/charts/prerequisites)
gráfico con el nombre de la versión "requisitos previos". Si implementó el gráfico helm con un nombre de versión diferente, actualice el
quickstart-values.yaml archivo en consecuencia antes de la instalación.

Correr `kubectl get pods` para comprobar si todos los pods de datahub se están ejecutando. Debería obtener un resultado similar al siguiente.

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

Puede ejecutar lo siguiente para exponer el frontend localmente. Tenga en cuenta que puede encontrar el nombre del pod utilizando el comando anterior. En
En este caso, el nombre del pod DataHub-Frontend era `datahub-datahub-frontend-84c58df9f7-5bgwx`.

```(shell)
kubectl port-forward <datahub-frontend pod name> 9002:9002
```

Debería poder acceder al frontend a través de http://localhost:9002.

Una vez que confirme que los pods se están ejecutando bien, puede configurar la entrada para datahub-frontend para exponer el puerto 9002 a
el público.

## Otros comandos útiles

| Comando | Descripción |
|-----|------|
| helm desinstalar datahub | Eliminar DataHub |
| helm ls | Lista de listas de Helm |
| | de historia del timón Obtener un historial de lanzamientos |
