***

## título: "Implementación en AWS"

# Guía de configuración de AWS

A continuación se muestra un conjunto de instrucciones para iniciar rápidamente DataHub en AWS Elastic Kubernetes Service (EKS). Nota, la guía
supone que no tiene configurado un clúster de kubernetes. Si está implementando DataHub en un clúster existente, por favor
omitir las secciones correspondientes.

## Prerrequisitos

Esta guía requiere las siguientes herramientas:

*   [kubectl](https://kubernetes.io/docs/tasks/tools/) para administrar los recursos de Kubernetes
*   [timón](https://helm.sh/docs/intro/install/) para implementar los recursos basados en gráficos de timón. Tenga en cuenta que solo admitimos Helm
    3\.
*   [eksctl](https://eksctl.io/introduction/#installation) para crear y administrar clústeres en EKS
*   [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html) para administrar los recursos de AWS

Para utilizar las herramientas anteriores, debe configurar las credenciales de AWS siguiendo
éste [guiar](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-profiles.html).

## Inicie un clúster de kubernetes en AWS EKS

Sigamos esto [guiar](https://docs.aws.amazon.com/eks/latest/userguide/getting-started-eksctl.html) Para crear un nuevo
cluster using eksctl. Ejecute el siguiente comando con el nombre del clúster establecido en el nombre del clúster de su elección y la región establecida en
la región de AWS en la que está operando.

    eksctl create cluster \
        --name <<cluster-name>> \
        --region <<region>> \
        --with-oidc \
        --nodes=3

El comando aprovisionará un clúster de EKS alimentado por 3 nodos EC2 m3.large y aprovisionará una capa de red basada en VPC.

Si planea ejecutar la capa de almacenamiento (MySQL, Elasticsearch, Kafka) como pods en el clúster, necesita al menos 3
Nodos. Si decide utilizar servicios de almacenamiento administrado, puede reducir el número de nodos o utilizar nodos m3.medium para guardar
costar. Consulte esto [guiar](https://eksctl.io/usage/creating-and-managing-clusters/) Para personalizar aún más el clúster
antes del aprovisionamiento.

Tenga en cuenta que la configuración de OIDC es necesaria para seguir esta guía al configurar el equilibrador de carga.

Correr `kubectl get nodes` para confirmar que el clúster se ha configurado correctamente. Debería obtener resultados como los siguientes

    NAME                                          STATUS   ROLES    AGE   VERSION
    ip-192-168-49-49.us-west-2.compute.internal   Ready    <none>   3h    v1.18.9-eks-d1db3c
    ip-192-168-64-56.us-west-2.compute.internal   Ready    <none>   3h    v1.18.9-eks-d1db3c
    ip-192-168-8-126.us-west-2.compute.internal   Ready    <none>   3h    v1.18.9-eks-d1db3c

## Configurar DataHub con Helm

Una vez que se ha configurado el clúster de kubernetes, puede implementar DataHub y sus requisitos previos con helm. Por favor, siga el
pasos en este [guiar](kubernetes.md)

## Exponer extremos mediante un equilibrador de carga

Ahora que todos los pods están en funcionamiento, debe exponer el punto final de datahub-frontend configurando
hacia arriba [ingreso](https://kubernetes.io/docs/concepts/services-networking/ingress/). Para hacer esto, primero debe configurar un
controlador de entrada. Hay
mucho [controladores de entrada](https://kubernetes.io/docs/concepts/services-networking/ingress-controllers/)  elegir
desde, pero aquí, seguiremos
éste [guiar](https://docs.aws.amazon.com/eks/latest/userguide/aws-load-balancer-controller.html) Para configurar AWS
Controlador de balanceador de carga de aplicaciones (ALB).

En primer lugar, si no utilizó eksctl para configurar el clúster de kubernetes, asegúrese de seguir los requisitos previos enumerados
[aquí](https://docs.aws.amazon.com/eks/latest/userguide/alb-ingress.html).

Descargue el documento de política de IAM para permitir que el controlador realice llamadas a las API de AWS en su nombre.

    curl -o iam_policy.json https://raw.githubusercontent.com/kubernetes-sigs/aws-load-balancer-controller/v2.2.0/docs/install/iam_policy.json

Cree una política de IAM basada en el documento de política ejecutando lo siguiente.

    aws iam create-policy \
        --policy-name AWSLoadBalancerControllerIAMPolicy \
        --policy-document file://iam_policy.json

Utilice eksctl para crear una cuenta de servicio que nos permita adjuntar la política anterior a los pods de kubernetes.

    eksctl create iamserviceaccount \
      --cluster=<<cluster-name>> \
      --namespace=kube-system \
      --name=aws-load-balancer-controller \
      --attach-policy-arn=arn:aws:iam::<<account-id>>:policy/AWSLoadBalancerControllerIAMPolicy \
      --override-existing-serviceaccounts \
      --approve      

Instale la definición de recurso personalizada de TargetGroupBinding ejecutando lo siguiente.

    kubectl apply -k "github.com/aws/eks-charts/stable/aws-load-balancer-controller//crds?ref=master"

Agregue el repositorio de gráficos helm que contiene la versión más reciente del controlador ALB.

    helm repo add eks https://aws.github.io/eks-charts
    helm repo update

Instale el controlador en el clúster de kubernetes ejecutando lo siguiente.

    helm upgrade -i aws-load-balancer-controller eks/aws-load-balancer-controller \
      --set clusterName=<<cluster-name>> \
      --set serviceAccount.create=false \
      --set serviceAccount.name=aws-load-balancer-controller \
      -n kube-system

Compruebe que la instalación se ha completado ejecutando `kubectl get deployment -n kube-system aws-load-balancer-controller`. Debería
devolver un resultado como el siguiente.

    NAME                           READY   UP-TO-DATE   AVAILABLE   AGE
    aws-load-balancer-controller   2/2     2            2           142m

Ahora que el controlador se ha configurado, podemos habilitar la entrada actualizando values.yaml (o cualquier otro values.yaml
archivo utilizado para implementar datahub). Cambie los valores de datahub-frontend a los siguientes.

    datahub-frontend:
      enabled: true
      image:
        repository: linkedin/datahub-frontend-react
        tag: "latest"
      ingress:
        enabled: true
        annotations:
          kubernetes.io/ingress.class: alb
          alb.ingress.kubernetes.io/scheme: internet-facing
          alb.ingress.kubernetes.io/target-type: instance
          alb.ingress.kubernetes.io/certificate-arn: <<certificate-arn>>
          alb.ingress.kubernetes.io/inbound-cidrs: 0.0.0.0/0
          alb.ingress.kubernetes.io/listen-ports: '[{"HTTP": 80}, {"HTTPS":443}]'
          alb.ingress.kubernetes.io/actions.ssl-redirect: '{"Type": "redirect", "RedirectConfig": { "Protocol": "HTTPS", "Port": "443", "StatusCode": "HTTP_301"}}'
        hosts:
          - host: <<host-name>>
            redirectPaths:
              - path: /*
                name: ssl-redirect
                port: use-annotation
            paths:
              - /*

Debe solicitar un certificado en AWS Certificate Manager siguiendo esto
[guiar](https://docs.aws.amazon.com/acm/latest/userguide/gs-acm-request-public.html), y reemplace certificate-arn por
el ARN del nuevo certificado. También debe reemplazar el nombre de host con el nombre de host de elección, como
demo.datahubproject.io.

Para tener los metadatos [servicio de autenticación](https://datahubproject.io/docs/introducing-metadata-service-authentication/#configuring-metadata-service-authentication) habilitar y usar [Tokens de API](https://datahubproject.io/docs/introducing-metadata-service-authentication/#generating-personal-access-tokens) Desde la interfaz de usuario, deberá establecer la configuración en Values.yaml para el cuadro de usuario `gms` y el `frontend` Implementaciones. Esto podría hacerse habilitando el `metadata_service_authentication`:

    datahub:
      metadata_service_authentication:
        enabled: true

Después de actualizar el archivo yaml, ejecute lo siguiente para aplicar las actualizaciones.

    helm upgrade --install datahub datahub/datahub --values values.yaml

Una vez completada la actualización, ejecute `kubectl get ingress` para comprobar la configuración de entrada. Debería ver un resultado como el
siguiente.

    NAME                       CLASS    HOSTS                         ADDRESS                                                                 PORTS   AGE
    datahub-datahub-frontend   <none>   demo.datahubproject.io   k8s-default-datahubd-80b034d83e-904097062.us-west-2.elb.amazonaws.com   80      3h5m

Anote la dirección elb en la columna de dirección. Agregue el registro DNS CNAME al dominio host que apunte al nombre de host (
desde arriba) a la dirección del ELB. Las actualizaciones de DNS generalmente tardan de unos minutos a una hora. Una vez hecho esto, deberías ser
capaz de acceder a datahub-frontend a través del nombre de host.

## Uso de servicios administrados de AWS para la capa de almacenamiento

Administrar los servicios de almacenamiento como MySQL, Elasticsearch y Kafka como pods de kubernetes requiere una gran cantidad de
carga de trabajo de mantenimiento. Para reducir la carga de trabajo, puede utilizar servicios administrados como AWS [RDS](https://aws.amazon.com/rds),
[Servicio Elasticsearch](https://aws.amazon.com/elasticsearch-service/)y [Kafka gestionado](https://aws.amazon.com/msk/)
como la capa de almacenamiento para DataHub. La compatibilidad con el uso de AWS Neptune como base de datos de gráficos llegará pronto.

### RDS

Aprovisione una base de datos MySQL en AWS RDS que comparta la VPC con el clúster de kubernetes o que tenga configurado el emparejamiento de VPC entre
la VPC del clúster de kubernetes. Una vez aprovisionada la base de datos, debería poder ver la página siguiente. Tomar
una nota del extremo marcado por el cuadro rojo.

![AWS RDS](../imgs/aws/aws-rds.png)

Primero, agregue la contraseña de base de datos a kubernetes ejecutando lo siguiente.

    kubectl delete secret mysql-secrets
    kubectl create secret generic mysql-secrets --from-literal=mysql-root-password=<<password>>

Actualice la configuración de sql en global en values.yaml de la siguiente manera.

      sql:
        datasource:
          host: "<<rds-endpoint>>:3306"
          hostForMysqlClient: "<<rds-endpoint>>"
          port: "3306"
          url: "jdbc:mysql://<<rds-endpoint>>:3306/datahub?verifyServerCertificate=false&useSSL=true&useUnicode=yes&characterEncoding=UTF-8"
          driver: "com.mysql.jdbc.Driver"
          username: "root"
          password:
            secretRef: mysql-secrets
            secretKey: mysql-root-password

Correr `helm upgrade --install datahub datahub/datahub --values values.yaml` para aplicar los cambios.

### Servicio Elasticsearch

Aprovisionar un dominio de elasticsearch que ejecute elasticsearch versión 7.9 o superior que comparta la VPC con kubernetes
clúster o tiene configurado el emparejamiento de VPC entre la VPC del clúster de kubernetes. Una vez que se aprovisiona el dominio, debe
poder ver la siguiente página. Tome nota del extremo marcado por el cuadro rojo.

![AWS Elasticsearch Service](../imgs/aws/aws-elasticsearch.png)

Actualice la configuración de elasticsearch en global en values.yaml de la siguiente manera.

      elasticsearch:
        host: <<elasticsearch-endpoint>>
        port: "443"
        useSSL: "true"

También puede permitir la comunicación a través de HTTP (sin SSL) utilizando la configuración a continuación.

      elasticsearch:
        host: <<elasticsearch-endpoint>>
        port: "80"

Si tiene habilitado el control de acceso detallado con autenticación básica, primero ejecute lo siguiente para crear un k8s
secreto con la contraseña.

    kubectl delete secret elasticsearch-secrets
    kubectl create secret generic elasticsearch-secrets --from-literal=elasticsearch-password=<<password>>

A continuación, utilice la configuración a continuación.

      elasticsearch:
        host: <<elasticsearch-endpoint>>
        port: "443"
        useSSL: "true"
        auth:
          username: <<username>>
          password:
            secretRef: elasticsearch-secrets
            secretKey: elasticsearch-password

Por último, usted **NECESITAR** Para establecer la siguiente variable ENV para **elasticsearchSetupJob**. AWS Elasticsearch/Opensearch
El servicio utiliza la versión OpenDistro de Elasticsearch, que no admite la funcionalidad "datastream". Como tal, utilizamos
una forma diferente de crear índices basados en el tiempo.

      elasticsearchSetupJob:
        enabled: true
        image:
          repository: linkedin/datahub-elasticsearch-setup
          tag: "***"
        extraEnvs:
          - name: USE_AWS_ELASTICSEARCH
            value: "true"

Correr `helm upgrade --install datahub datahub/datahub --values values.yaml` para aplicar los cambios.

**Nota:**
Si tiene una configuración personalizada del clúster de elastic search y está implementando a través de docker, puede modificar las configuraciones
en datahub para apuntar a la instancia ES específica:

1.  Si está utilizando `docker quickstart` Puede modificar el nombre de host y el puerto de la instancia de ES en Docker Compose
    Archivos de inicio rápido localizados [aquí](../../docker/quickstart/).
    1.  Una vez que haya modificado las recetas de inicio rápido, puede ejecutar el comando de inicio rápido mediante una composición de Docker específica
        archivo. Ejemplo de comando para que sea
        *   `datahub docker quickstart --quickstart-compose-file docker/quickstart/docker-compose-without-neo4j.quickstart.yml`
2.  Si no está utilizando recetas de inicio rápido, puede modificar la variable de entorno en GMS para que apunte a la instancia de ES. El
    Se encuentran los archivos ENV para DataHub-GMS [aquí](../../docker/datahub-gms/env/).

Además, puede encontrar una lista de propiedades admitidas para trabajar con un ES personalizado
instancia [aquí](../../metadata-service/factories/src/main/java/com/linkedin/gms/factory/common/ElasticsearchSSLContextFactory.java)
y [aquí](../../metadata-service/factories/src/main/java/com/linkedin/gms/factory/common/RestHighLevelClientFactory.java)
.

Una asignación entre el nombre de propiedad utilizado en los dos archivos anteriores y el nombre utilizado en el archivo docker/env puede ser
fundar [aquí](../../metadata-service/factories/src/main/resources/application.yml).

### Streaming gestionado para Apache Kafka (MSK)

Aprovisionamiento de un clúster de MSK que comparta la VPC con el clúster de kubernetes o que tenga configurado el emparejamiento de VPC entre la VPC de
el clúster de kubernetes. Una vez aprovisionado el dominio, haga clic en el botón "Ver información del cliente" en el 'Clúster
Resumen". Debería ver una página como la siguiente. Tome nota de los puntos finales marcados por los cuadros rojos.

![AWS MSK](../imgs/aws/aws-msk.png)

Actualice la configuración de kafka en global en values.yaml de la siguiente manera.

    kafka:
        bootstrap:
          server: "<<bootstrap-server endpoint>>"
        zookeeper:
          server:  "<<zookeeper endpoint>>"
        schemaregistry:
          url: "http://prerequisites-cp-schema-registry:8081"
        partitions: 3
        replicationFactor: 3

Tenga en cuenta que el número de particiones y replicationFactor debe coincidir con el número de servidores de arranque. Esto es por defecto 3
para AWS MSK.

Correr `helm upgrade --install datahub datahub/datahub --values values.yaml` para aplicar los cambios.

### Registro de esquemas de AWS Glue

> **ADVERTENCIA**: AWS Glue Schema Registry NO tiene un SDK de Python. Como tal, las bibliotecas basadas en Python, como la ingesta o las acciones de datahub (ingesta de la interfaz de usuario), no se admiten cuando se utiliza AWS Glue Schema Registry

Puede utilizar el registro de esquemas de AWS Glue en lugar del registro de esquemas de kafka. Para ello, primero aprovisione un esquema de AWS Glue
en la pestaña "Registro de esquema" de la página de la consola de AWS Glue.

Una vez aprovisionado el registro, puede cambiar el gráfico helm de la siguiente manera.

    kafka:
        bootstrap:
          ...
        zookeeper:
          ...
        schemaregistry:
          type: AWS_GLUE
          glue:
            region: <<AWS region of registry>>
            registry: <<name of registry>>

Tenga en cuenta que utilizará el nombre del tema como nombre de esquema en el Registro.

Antes de actualizar los pods, debe conceder a los nodos de trabajo de k8s los permisos correctos para acceder al registro de esquemas.

Los permisos mínimos requeridos se ven así

    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "VisualEditor0",
                "Effect": "Allow",
                "Action": [
                    "glue:GetRegistry",
                    "glue:ListRegistries",
                    "glue:CreateSchema",
                    "glue:UpdateSchema",
                    "glue:GetSchema",
                    "glue:ListSchemas",
                    "glue:RegisterSchemaVersion",
                    "glue:GetSchemaByDefinition",
                    "glue:GetSchemaVersion",
                    "glue:GetSchemaVersionsDiff",
                    "glue:ListSchemaVersions",
                    "glue:CheckSchemaVersionValidity",
                    "glue:PutSchemaVersionMetadata",
                    "glue:QuerySchemaVersionMetadata"
                ],
                "Resource": [
                    "arn:aws:glue:*:795586375822:schema/*",
                    "arn:aws:glue:us-west-2:795586375822:registry/demo-shared"
                ]
            },
            {
                "Sid": "VisualEditor1",
                "Effect": "Allow",
                "Action": [
                    "glue:GetSchemaVersion"
                ],
                "Resource": [
                    "*"
                ]
            }
        ]
    }

Se requiere que la última parte tenga "\*" como recurso debido a un problema en la biblioteca de registro de esquemas de AWS Glue.
Consulte [Este problema](https://github.com/awslabs/aws-glue-schema-registry/issues/68) para cualquier actualización.

Actualmente, Glue no es compatible con AWS Signature V4. Como tal, no podemos usar cuentas de servicio para otorgar permisos de acceso
el registro de esquemas. La solución consiste en conceder el permiso anterior al rol de IAM del nodo de trabajo de EKS. Recomienda
Para [Este problema](https://github.com/awslabs/aws-glue-schema-registry/issues/69) para cualquier actualización.

Correr `helm upgrade --install datahub datahub/datahub --values values.yaml` para aplicar los cambios.

Tenga en cuenta que verá el registro "Schema Version Id is null. Intentando registrar el esquema" en cada solicitud. Este registro es
engañoso, por lo que debe ser ignorado. Los esquemas se almacenan en caché, por lo que no registra una nueva versión en cada solicitud (también conocida como no
problemas de rendimiento). Esto ha sido corregido por [este PR](https://github.com/awslabs/aws-glue-schema-registry/pull/64) pero
el código aún no se ha publicado. Actualizaremos la versión una vez que salga una nueva versión.

### Políticas de IAM para la ingesta basada en ui

En esta sección se detalla cómo adjuntar directivas al pod acryl-datahub-actions que impulsa la ingesta basada en la interfaz de usuario. Para algunos de
las recetas de ingestión, se especifican los credos de inicio de sesión en la propia receta, lo que facilita la configuración de la autenticación para obtener metadatos
desde el origen de datos. Sin embargo, para los recursos de AWS, la recomendación es utilizar roles y políticas de IAM para proteger las solicitudes.
para acceder a los metadatos de estos recursos.

Para hacer esto, sigamos
éste [guiar](https://docs.aws.amazon.com/eks/latest/userguide/create-service-account-iam-policy-and-role.html) Para
asociar una cuenta de servicio de kubernetes con un rol de IAM. A continuación, podemos adjuntar este rol de IAM a las acciones de acryl-datahub
para permitir que el pod asuma el rol especificado.

En primer lugar, debe crear una política de IAM con todos los permisos necesarios para ejecutar la ingesta. Esto es específico para cada uno
conector y el conjunto de metadatos que está intentando extraer. es decir, la elaboración de perfiles requiere más permisos, ya que necesita
acceso a los datos, no solo a los metadatos. Digamos que asumimos el ARN de esa política
es `arn:aws:iam::<<account-id>>:policy/policy1`.

A continuación, cree una cuenta de servicio con la directiva adjunta que se va a utilizar [eksctl](https://eksctl.io/). Puede ejecutar el
siguiendo el comando para hacerlo.

    eksctl create iamserviceaccount \
        --name <<service-account-name>> \
        --namespace <<namespace>> \
        --cluster <<eks-cluster-name>> \
        --attach-policy-arn <<policy-ARN>> \
        --approve \
        --override-existing-serviceaccounts

Por ejemplo, al ejecutar lo siguiente se creará una cuenta de servicio "acryl-datahub-actions" en el espacio de nombres datahub de
clúster de datahub EKS con `arn:aws:iam::<<account-id>>:policy/policy1` adjunto.

    eksctl create iamserviceaccount \
        --name acryl-datahub-actions \
        --namespace datahub \
        --cluster datahub \
        --attach-policy-arn arn:aws:iam::<<account-id>>:policy/policy1 \
        --approve \
        --override-existing-serviceaccounts

Por último, en helm values.yaml, puede agregar lo siguiente a acryl-datahub-actions para adjuntar la cuenta de servicio a
el pod acryl-datahub-actions.

```yaml
acryl-datahub-actions:
  enabled: true
  serviceAccount:
    name: <<service-account-name>>
  ...
```
