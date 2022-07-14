# Ejecución de Airflow localmente con DataHub

## Introducción

En este documento se describe cómo puede ejecutar Airflow en paralelo con las imágenes de Docker de inicio rápido de DataHub para probar el linaje de Airflow con DataHub.
Esto ofrece una forma mucho más fácil de probar Airflow con DataHub, en comparación con la configuración de contenedores a mano, la configuración de configuraciones y la conectividad de red entre los dos sistemas.

## Requisitos previos

*   Docker: asegúrese de tener una instalación de Docker que funcione y de que tenga al menos 8 GB de memoria para asignar a Airflow y DataHub combinados.

<!---->

    docker info | grep Memory

    > Total Memory: 7.775GiB

*   Inicio rápido: asegúrese de que lo siguió [inicio rápido](../../docs/quickstart.md) para poner en marcha DataHub.

## Paso 1: Configura tu área de flujo de aire

*   Cree un área para alojar su instalación de flujo de aire
*   Descargue el archivo docker-compose alojado en el repositorio de DataHub en ese directorio
*   Descargue un dag de muestra para usarlo en la prueba del linaje de flujo de aire

<!---->

    mkdir -p airflow_install
    cd airflow_install
    # Download docker-compose file
    curl -L 'https://raw.githubusercontent.com/datahub-project/datahub/master/docker/airflow/docker-compose.yaml' -o docker-compose.yaml
    # Create dags directory
    mkdir -p dags
    # Download a sample DAG
    curl -L 'https://raw.githubusercontent.com/datahub-project/datahub/master/metadata-ingestion/src/datahub_provider/example_dags/lineage_backend_demo.py' -o dags/lineage_backend_demo.py

### ¿Qué diferencia hay entre este archivo docker-compose y el archivo oficial de composición docker Apache Airflow?

*   Este archivo docker-compose se deriva del [archivo oficial de Airflow docker-compose](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html#docker-compose-yaml) pero realiza algunos cambios críticos para que la interoperabilidad con DataHub sea perfecta.
*   La imagen Airflow en este archivo de redacción docker extiende el [imagen base de Apache Airflow docker](https://airflow.apache.org/docs/docker-stack/index.html) y se publica [aquí](https://hub.docker.com/r/acryldata/airflow-datahub). Incluye lo último `acryl-datahub` pip package instalado de forma predeterminada, por lo que no necesita instalarlo usted mismo.
*   Este archivo docker-compose configura la red para que
    *   Los contenedores Airflow pueden comunicarse con los contenedores DataHub a través del `datahub_network` interfaz puente.
    *   Modifica el reenvío de puertos para asignar el puerto del servidor web Airflow `8080` a puerto `58080` en localhost (para evitar conflictos con datahub metadata-service, que se asigna a 8080 de forma predeterminada)
*   Este archivo docker-compose también configura las variables ENV para configurar el Lineage Backend de Airflow para que se comunique con DataHub. (Busque el `AIRFLOW__LINEAGE__BACKEND` y `AIRFLOW__LINEAGE__DATAHUB_KWARGS` variables)

## Paso 2: Abre el flujo de aire

Primero debe inicializar el flujo de aire para crear tablas de base de datos iniciales y el usuario de flujo de aire inicial.

    docker-compose up airflow-init

Debería ver el siguiente mensaje de inicialización final

```
airflow-init_1       | Admin user airflow created
airflow-init_1       | 2.1.3
airflow_install_airflow-init_1 exited with code 0

```

Después, debe iniciar el docker-compose de flujo de aire

    docker-compose up

Debería ver una gran cantidad de mensajes a medida que se inicia Airflow.

    Container airflow_deploy_airflow-scheduler_1  Started                                                                               15.7s
    Attaching to airflow-init_1, airflow-scheduler_1, airflow-webserver_1, airflow-worker_1, flower_1, postgres_1, redis_1
    airflow-worker_1     | BACKEND=redis
    airflow-worker_1     | DB_HOST=redis
    airflow-worker_1     | DB_PORT=6379
    airflow-worker_1     | 
    airflow-webserver_1  | 
    airflow-init_1       | DB: postgresql+psycopg2://airflow:***@postgres/airflow
    airflow-init_1       | [2021-08-31 20:02:07,534] {db.py:702} INFO - Creating tables
    airflow-init_1       | INFO  [alembic.runtime.migration] Context impl PostgresqlImpl.
    airflow-init_1       | INFO  [alembic.runtime.migration] Will assume transactional DDL.
    airflow-scheduler_1  |   ____________       _____________
    airflow-scheduler_1  |  ____    |__( )_________  __/__  /________      __
    airflow-scheduler_1  | ____  /| |_  /__  ___/_  /_ __  /_  __ \_ | /| / /
    airflow-scheduler_1  | ___  ___ |  / _  /   _  __/ _  / / /_/ /_ |/ |/ /
    airflow-scheduler_1  |  _/_/  |_/_/  /_/    /_/    /_/  \____/____/|__/
    airflow-scheduler_1  | [2021-08-31 20:02:07,736] {scheduler_job.py:661} INFO - Starting the scheduler
    airflow-scheduler_1  | [2021-08-31 20:02:07,736] {scheduler_job.py:666} INFO - Processing each file at most -1 times
    airflow-scheduler_1  | [2021-08-31 20:02:07,915] {manager.py:254} INFO - Launched DagFileProcessorManager with pid: 25
    airflow-scheduler_1  | [2021-08-31 20:02:07,918] {scheduler_job.py:1197} INFO - Resetting orphaned tasks for active dag runs
    airflow-scheduler_1  | [2021-08-31 20:02:07,923] {settings.py:51} INFO - Configured default timezone Timezone('UTC')
    flower_1             | 
    airflow-worker_1     |  * Serving Flask app "airflow.utils.serve_logs" (lazy loading)
    airflow-worker_1     |  * Environment: production
    airflow-worker_1     |    WARNING: This is a development server. Do not use it in a production deployment.
    airflow-worker_1     |    Use a production WSGI server instead.
    airflow-worker_1     |  * Debug mode: off
    airflow-worker_1     | [2021-08-31 20:02:09,283] {_internal.py:113} INFO -  * Running on http://0.0.0.0:8793/ (Press CTRL+C to quit)
    flower_1             | BACKEND=redis
    flower_1             | DB_HOST=redis
    flower_1             | DB_PORT=6379
    flower_1             | 

Finalmente, el flujo de aire debe estar saludable y estar en el puerto 58080. Navegue hasta http://localhost:58080 para confirmar y encontrar su servidor web Airflow.
El nombre de usuario y la contraseña predeterminados son:

    airflow:airflow

## Paso 3: Registrar la conexión de DataHub (enlace) a Airflow

    docker exec -it `docker ps | grep webserver | cut -d " " -f 1` airflow connections add --conn-type 'datahub_rest' 'datahub_rest_default' --conn-host 'http://datahub-gms:8080'

### Resultado

    Successfully added `conn_id`=datahub_rest_default : datahub_rest://:@http://datahub-gms:8080:

### ¿Qué está haciendo el comando anterior?

*   Busque el contenedor que ejecuta el servidor web de flujo de aire: `docker ps | grep webserver | cut -d " " -f 1`
*   Ejecutando el `airflow connections add ...` comando dentro de ese contenedor para registrar el `datahub_rest` tipo de conexión y conéctelo al `datahub-gms` en el puerto 8080.
*   Nota: Esto es lo que requiere Airflow para poder conectarse a `datahub-gms` el host (este es el contenedor que ejecuta la imagen DataHub-GMS) y es por eso que necesitábamos conectar los contenedores Airflow al `datahub_network` utilizando nuestro archivo docker-compose personalizado.

## Paso 4: Busque los DAG y ejecútelos

Navegue por la interfaz de usuario de Airflow para encontrar el dag de airflow de muestra que acabamos de traer

![Find the DAG](../../docs/imgs/airflow/find_the_dag.png)

De forma predeterminada, Airflow carga todos los DAG-s en estado pausado. Despause el DAG de ejemplo para usarlo.
![Paused DAG](../../docs/imgs/airflow/paused_dag.png)
![Unpaused DAG](../../docs/imgs/airflow/unpaused_dag.png)

A continuación, active el DAG para que se ejecute.

![Trigger the DAG](../../docs/imgs/airflow/trigger_dag.png)

Después de que el DAG se ejecute correctamente, vaya a su instancia de DataHub para ver la canalización y navegar por su linaje.

![DataHub Pipeline View](../../docs/imgs/airflow/datahub_pipeline_view.png)

![DataHub Pipeline Entity](../../docs/imgs/airflow/datahub_pipeline_entity.png)

![DataHub Task View](../../docs/imgs/airflow/datahub_task_view.png)

![DataHub Lineage View](../../docs/imgs/airflow/datahub_lineage_view.png)

## Solución de problemas

La mayoría de los problemas están relacionados con la conectividad entre Airflow y DataHub.

Así es como puede depurarlos.

![Find the Task Log](../../docs/imgs/airflow/finding_failed_log.png)

![Inspect the Log](../../docs/imgs/airflow/connection_error.png)

En este caso, claramente la conexión `datahub-rest` no se ha registrado. ¡Parece que olvidamos registrar la conexión con Airflow!
Ejecutemos el Paso 4 para registrar la conexión del centro de datos con Airflow.

En caso de que la conexión se haya registrado correctamente pero aún esté viendo `Failed to establish a new connection`, compruebe si la conexión es `http://datahub-gms:8080` y no `http://localhost:8080`.

Después de volver a ejecutar el DAG, ¡vemos el éxito!

![Pipeline Success](../../docs/imgs/airflow/successful_run.png)
