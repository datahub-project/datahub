# Monitoreo de DataHub

Monitorear los componentes del sistema de DataHub es fundamental para operar y mejorar DataHub. Este documento explica cómo agregar
mediciones de seguimiento y métricas en los contenedores de DataHub.

## Trazado

Los rastreos nos permiten rastrear la vida de una solicitud a través de múltiples componentes. Cada traza se compone de múltiples tramos, que
son unidades de trabajo, que contienen varios contextos sobre el trabajo que se está realizando, así como el tiempo necesario para terminar el trabajo. Por
al observar el seguimiento, podemos identificar más fácilmente los cuellos de botella de rendimiento.

Habilitamos el rastreo mediante el uso de
el [Biblioteca de instrumentación java OpenTelemetry](https://github.com/open-telemetry/opentelemetry-java-instrumentation).
Este proyecto proporciona un JAR de agente Java que se adjunta a las aplicaciones Java. El agente inyecta código de bytes para capturar
telemetría de bibliotecas populares.

Usando el agente somos capaces de

1.  Plug and play diferentes herramientas de rastreo basadas en la configuración del usuario: Jaeger, Zipkin u otras herramientas
2.  Obtener seguimientos para Kafka, JDBC y Elasticsearch sin ningún código adicional
3.  Rastree los rastros de cualquier función con un simple `@WithSpan` anotación

Puede habilitar el agente estableciendo la variable env `ENABLE_OTEL` Para `true` para consumidores de GMS y MAE/MCE. En nuestro
ejemplo [docker-compose](../../docker/monitoring/docker-compose.monitoring.yml), exportamos métricas a un Jaeger local
instancia estableciendo la variable env `OTEL_TRACES_EXPORTER` Para `jaeger`
y `OTEL_EXPORTER_JAEGER_ENDPOINT` Para `http://jaeger-all-in-one:14250`, pero puede cambiar fácilmente este comportamiento mediante
establecer las variables env correctas. Consulte
éste [Doc](https://github.com/open-telemetry/opentelemetry-java/blob/main/sdk-extensions/autoconfigure/README.md) para
todas las configuraciones.

Una vez que se configura lo anterior, debería poder ver un seguimiento detallado a medida que se envía una solicitud a GMS. Añadimos
el `@WithSpan` anotación en varios lugares para hacer que el rastro sea más legible. Debería comenzar a ver rastros en el
colector de rastreo de elección. Nuestro ejemplo [docker-compose](../../docker/monitoring/docker-compose.monitoring.yml) Despliega
una instancia de Jaeger con el puerto 16686. Los rastros deben estar disponibles en http://localhost:16686.

## Métricas

Con el rastreo, podemos observar cómo una solicitud fluye a través de nuestro sistema hacia la capa de persistencia. Sin embargo, para un más
Imagen holística, necesitamos poder exportar métricas y medirlas a través del tiempo. Desafortunadamente, java de OpenTelemetry
La biblioteca de métricas todavía está en desarrollo activo.

Como tal, decidimos usar [Métricas de Dropwizard](https://metrics.dropwizard.io/4.2.0/) para exportar métricas personalizadas a JMX,
y luego use [Exportador prometheus-JMX](https://github.com/prometheus/jmx_exporter) Para exportar todas las métricas de JMX a
Prometeo. Esto permite que nuestra base de código sea independiente de la herramienta de recopilación de métricas, lo que facilita el uso de las personas.
su herramienta de elección. Puede habilitar el agente estableciendo la variable env `ENABLE_PROMETHEUS` Para `true` para GMS y MAE/MCE
consumidores. Consulte este ejemplo [docker-compose](../../docker/monitoring/docker-compose.monitoring.yml) para establecer el
Variables.

En nuestro ejemplo [docker-compose](../../docker/monitoring/docker-compose.monitoring.yml), hemos configurado prometheus para
raspado de 4318 puertos de cada contenedor utilizado por el exportador de JMX para exportar métricas. También configuramos grafana para
escucha prometheus y crea cuadros de mando útiles. De forma predeterminada, proporcionamos dos
Paneles: [Panel de JVM](https://grafana.com/grafana/dashboards/14845) y panel de control de DataHub.

En el panel de JVM, puede encontrar gráficos detallados basados en métricas de JVM como el uso de CPU / memoria / disco. En el DataHub
dashboard, puede encontrar gráficos para monitorear cada punto final y los temas de kafka. Usando la implementación de ejemplo, vaya
para http://localhost:3001 encontrar los tableros de grafana! (Nombre de usuario: admin, PW: admin)

Para facilitar el seguimiento de varias métricas dentro de la base de código, creamos la clase MetricUtils. Esta clase de utilidad crea un
registro central de métricas, configura el informador JMX y proporciona funciones convenientes para configurar contadores y temporizadores.
Puede ejecutar lo siguiente para crear un contador e incremento.

```java
MetricUtils.counter(this.getClass(),"metricName").increment();
```

Puede ejecutar lo siguiente para cronometrar un bloque de código.

```java
try(Timer.Context ignored=MetricUtils.timer(this.getClass(),"timerName").timer()){
    ...block of code
    }
```

## Habilite la supervisión a través de docker-compose

Proporcionamos algunos ejemplos de configuración para habilitar la supervisión en
éste [directorio](https://github.com/datahub-project/datahub/tree/master/docker/monitoring). Echa un vistazo al docker-compose
files, que agrega las variables env necesarias a los contenedores existentes y genera nuevos contenedores (Jaeger, Prometheus,
Grafana).

Puede agregar el docker-compose anterior utilizando el botón `-f <<path-to-compose-file>>` al ejecutar comandos docker-compose.
Por ejemplo

```shell
docker-compose \
  -f quickstart/docker-compose.quickstart.yml \
  -f monitoring/docker-compose.monitoring.yml \
  pull && \
docker-compose -p datahub \
  -f quickstart/docker-compose.quickstart.yml \
  -f monitoring/docker-compose.monitoring.yml \
  up
```

Configuramos quickstart.sh, dev.sh y dev-without-neo4j.sh para agregar el docker-compose anterior cuando MONITORING=true. Para
instancia `MONITORING=true ./docker/quickstart.sh` agregará las variables env correctas para comenzar a recopilar trazas y
métricas, y también implementar Jaeger, Prometheus y Grafana. Pronto admitiremos esto como una bandera durante el inicio rápido.
