# Cómo extraer registros de contenedores de DataHub

Los contenedores DataHub, datahub GMS (servidor back-end) y datahub frontend (servidor UI), escriben archivos de registro en el sistema de archivos contenedor local. Para extraer estos registros, deberá obtenerlos desde el interior del contenedor donde se ejecutan los servicios.

Puede hacerlo fácilmente usando la CLI de Docker si está implementando con vanilla docker o compose, y kubectl si está en K8s.

## Paso 1: Encuentra la identificación del contenedor que te interesa

Primero deberá obtener el identificador del contenedor para el que desea extraer registros. Por ejemplo, datahub-gms.

### Docker y Docker Compose

Para ello, puede ver todos los contenedores que Docker conoce ejecutando el siguiente comando:

    johnjoyce@Johns-MBP datahub-fork % docker container ls
    CONTAINER ID   IMAGE                                   COMMAND                  CREATED      STATUS                  PORTS                                                      NAMES
    6c4a280bc457   linkedin/datahub-frontend-react   "datahub-frontend/bi…"   5 days ago   Up 46 hours (healthy)   0.0.0.0:9002->9002/tcp                                     datahub-frontend-react
    122a2488ab63   linkedin/datahub-gms              "/bin/sh -c /datahub…"   5 days ago   Up 5 days (healthy)     0.0.0.0:8080->8080/tcp                                     datahub-gms
    7682dcc64afa   confluentinc/cp-schema-registry:5.4.0   "/etc/confluent/dock…"   5 days ago   Up 5 days               0.0.0.0:8081->8081/tcp                                     schema-registry
    3680fcaef3ed   confluentinc/cp-kafka:5.4.0             "/etc/confluent/dock…"   5 days ago   Up 5 days               0.0.0.0:9092->9092/tcp, 0.0.0.0:29092->29092/tcp           broker
    9d6730ddd4c4   neo4j:4.0.6                             "/sbin/tini -g -- /d…"   5 days ago   Up 5 days               0.0.0.0:7474->7474/tcp, 7473/tcp, 0.0.0.0:7687->7687/tcp   neo4j
    c97edec663af   confluentinc/cp-zookeeper:5.4.0         "/etc/confluent/dock…"   5 days ago   Up 5 days               2888/tcp, 0.0.0.0:2181->2181/tcp, 3888/tcp                 zookeeper
    150ba161cf26   mysql:5.7                               "docker-entrypoint.s…"   5 days ago   Up 5 days               0.0.0.0:3306->3306/tcp, 33060/tcp                          mysql
    4b72a3eab73f   elasticsearch:7.9.3                     "/tini -- /usr/local…"   5 days ago   Up 5 days (healthy)     0.0.0.0:9200->9200/tcp, 9300/tcp                           elasticsearch

En este caso, el identificador del contenedor que nos gustaría tener en cuenta es `122a2488ab63`, que corresponde a la `datahub-gms` servicio.

### Kubernetes y Helm

Busque el nombre del pod que le interesa con el siguiente comando:

    kubectl get pods

    ...
    default   datahub-frontend-1231ead-6767                        1/1     Running     0          42h
    default   datahub-gms-c578b47cd-7676                              1/1     Running     0          13d
    ...

En este caso, el nombre del pod que nos gustaría tener en cuenta es `datahub-gms-c578b47cd-7676` , que contiene el servicio backend de GMS.

## Paso 2: Buscar los archivos de registro

El segundo paso es ver todos los archivos de registro. Los archivos de registro vivirán dentro del contenedor bajo los siguientes directorios para cada servicio:

*   **datahub-gms:** `/tmp/datahub/logs/gms`
*   **datahub-frontend**: `/tmp/datahub/logs/datahub-frontend`

Hay 2 tipos de registros que se recopilan:

1.  **Registros de información**: Estos incluyen información, advertencia, líneas de registro de errores. Son los que imprimen a stdout cuando el contenedor se ejecuta.
2.  **Registros de depuración**: Estos archivos tienen una retención más corta (más de 1 día) pero incluyen información de depuración más granular del código de DataHub específicamente. Ignoramos los registros de depuración de bibliotecas externas de las que depende DataHub.

### Docker y Docker Compose

Dado que los archivos de registro se nombran en función de la fecha actual, deberá usar "ls" para ver qué archivos existen actualmente. Para ello, puede utilizar el `docker exec` , utilizando el identificador de contenedor registrado en el paso uno:

    docker exec --privileged <container-id> <shell-command> 

Por ejemplo:

    johnjoyce@Johns-MBP datahub-fork % docker exec --privileged 122a2488ab63 ls -la /tmp/datahub/logs/gms 
    total 4664
    drwxr-xr-x    2 datahub  datahub       4096 Jul 28 05:14 .
    drwxr-xr-x    3 datahub  datahub       4096 Jul 23 08:37 ..
    -rw-r--r--    1 datahub  datahub    2001112 Jul 23 23:33 gms.2021-23-07-0.log
    -rw-r--r--    1 datahub  datahub      74343 Jul 24 20:29 gms.2021-24-07-0.log
    -rw-r--r--    1 datahub  datahub      70252 Jul 25 17:56 gms.2021-25-07-0.log
    -rw-r--r--    1 datahub  datahub     626985 Jul 26 23:36 gms.2021-26-07-0.log
    -rw-r--r--    1 datahub  datahub     712270 Jul 27 23:59 gms.2021-27-07-0.log
    -rw-r--r--    1 datahub  datahub     867707 Jul 27 23:59 gms.debug.2021-27-07-0.log
    -rw-r--r--    1 datahub  datahub       3563 Jul 28 05:26 gms.debug.log
    -rw-r--r--    1 datahub  datahub     382443 Jul 28 16:16 gms.log

Dependiendo de su problema, es posible que le interese ver los registros de información normales y de depuración.

### Kubernetes y Helm

Dado que los archivos de registro se nombran en función de la fecha actual, deberá usar "ls" para ver qué archivos existen actualmente. Para ello, puede utilizar el `kubectl exec` , utilizando el nombre del pod registrado en el paso uno:

    kubectl exec datahub-frontend-1231ead-6767 -n default -- ls -la /tmp/datahub/logs/gms

    total 36388
    drwxr-xr-x    2 datahub  datahub       4096 Jul 29 07:45 .
    drwxr-xr-x    3 datahub  datahub         17 Jul 15 08:47 ..
    -rw-r--r--    1 datahub  datahub     104548 Jul 15 22:24 gms.2021-15-07-0.log
    -rw-r--r--    1 datahub  datahub      12684 Jul 16 14:55 gms.2021-16-07-0.log
    -rw-r--r--    1 datahub  datahub    2482571 Jul 17 14:40 gms.2021-17-07-0.log
    -rw-r--r--    1 datahub  datahub      49120 Jul 18 14:31 gms.2021-18-07-0.log
    -rw-r--r--    1 datahub  datahub      14167 Jul 19 23:47 gms.2021-19-07-0.log
    -rw-r--r--    1 datahub  datahub      13255 Jul 20 22:22 gms.2021-20-07-0.log
    -rw-r--r--    1 datahub  datahub     668485 Jul 21 19:52 gms.2021-21-07-0.log
    -rw-r--r--    1 datahub  datahub    1448589 Jul 22 20:18 gms.2021-22-07-0.log
    -rw-r--r--    1 datahub  datahub      44187 Jul 23 13:51 gms.2021-23-07-0.log
    -rw-r--r--    1 datahub  datahub      14173 Jul 24 22:59 gms.2021-24-07-0.log
    -rw-r--r--    1 datahub  datahub      13263 Jul 25 21:11 gms.2021-25-07-0.log
    -rw-r--r--    1 datahub  datahub      13261 Jul 26 19:02 gms.2021-26-07-0.log
    -rw-r--r--    1 datahub  datahub    1118105 Jul 27 21:10 gms.2021-27-07-0.log
    -rw-r--r--    1 datahub  datahub     678423 Jul 28 23:57 gms.2021-28-07-0.log
    -rw-r--r--    1 datahub  datahub    1776274 Jul 28 07:19 gms.debug.2021-28-07-0.log
    -rw-r--r--    1 datahub  datahub   27576533 Jul 29 09:55 gms.debug.log
    -rw-r--r--    1 datahub  datahub    1195940 Jul 29 14:54 gms.log

En el siguiente paso, guardaremos archivos de registro específicos en nuestro sistema de archivos local.

## Paso 3: Guardar el archivo de registro del contenedor en local

Este paso implica guardar una copia de los archivos de registro del contenedor en el sistema de archivos local para una mayor investigación.

### Docker y Docker Compose

Simplemente use el `docker exec` para "cat" los archivos de registro de interés y enrutarlos a un nuevo archivo.

    docker exec --privileged 122a2488ab63 cat /tmp/datahub/logs/gms/gms.debug.log > my-local-log-file.log

Ahora debería poder ver los registros localmente.

### Kubernetes y Helm

Hay algunas maneras de sacar archivos del pod y llevarlos a un archivo local. Puede usar `kubectl cp` o simplemente `cat` y canalizar el archivo de interés. Mostraremos un ejemplo utilizando este último enfoque:

    kubectl exec datahub-frontend-1231ead-6767 -n default -- cat /tmp/datahub/logs/gms/gms.log > my-local-gms.log
