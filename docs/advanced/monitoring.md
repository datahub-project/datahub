# Monitoring DataHub

Monitoring DataHub's system components is critical for operating and improving DataHub. This doc explains how to add
tracing and metrics measurements in the DataHub containers.

## Tracing

Traces let us track the life of a request across multiple components. Each trace is consisted of multiple spans, which
are units of work, containing various context about the work being done as well as time taken to finish the work. By
looking at the trace, we can more easily identify performance bottlenecks.

We enable tracing by using
the [OpenTelemetry java instrumentation library](https://github.com/open-telemetry/opentelemetry-java-instrumentation).
This project provides a Java agent JAR that is attached to java applications. The agent injects bytecode to capture
telemetry from popular libraries.

Using the agent we are able to

1) Plug and play different tracing tools based on the user's setup: Jaeger, Zipkin, or other tools
2) Get traces for Kafka, JDBC, and Elasticsearch without any additional code
3) Track traces of any function with a simple `@WithSpan` annotation

You can enable the agent by setting env variable `ENABLE_OTEL` to `true` for GMS and MAE/MCE consumers. In our
example [docker-compose](../../docker/monitoring/docker-compose.monitoring.yml), we export metrics to a local Jaeger
instance by setting env variable `OTEL_TRACES_EXPORTER` to `jaeger`
and `OTEL_EXPORTER_JAEGER_ENDPOINT` to `http://jaeger-all-in-one:14250`, but you can easily change this behavior by
setting the correct env variables. Refer to
this [doc](https://github.com/open-telemetry/opentelemetry-java/blob/main/sdk-extensions/autoconfigure/README.md) for
all configs.

Once the above is set up, you should be able to see a detailed trace as a request is sent to GMS. We added
the `@WithSpan` annotation in various places to make the trace more readable. You should start to see traces in the
tracing collector of choice. Our example [docker-compose](../../docker/monitoring/docker-compose.monitoring.yml) deploys
an instance of Jaeger with port 16686. The traces should be available at http://localhost:16686.

## Metrics

With tracing, we can observe how a request flows through our system into the persistence layer. However, for a more
holistic picture, we need to be able to export metrics and measure them across time. Unfortunately, OpenTelemetry's java
metrics library is still in active development.

As such, we decided to use [Dropwizard Metrics](https://metrics.dropwizard.io/4.2.0/) to export custom metrics to JMX,
and then use [Prometheus-JMX exporter](https://github.com/prometheus/jmx_exporter) to export all JMX metrics to
Prometheus. This allows our code base to be independent of the metrics collection tool, making it easy for people to use
their tool of choice. You can enable the agent by setting env variable `ENABLE_PROMETHEUS` to `true` for GMS and MAE/MCE
consumers. Refer to this example [docker-compose](../../docker/monitoring/docker-compose.monitoring.yml) for setting the
variables.

In our example [docker-compose](../../docker/monitoring/docker-compose.monitoring.yml), we have configured prometheus to
scrape from 4318 ports of each container used by the JMX exporter to export metrics. We also configured grafana to
listen to prometheus and create useful dashboards. By default, we provide two
dashboards: [JVM dashboard](https://grafana.com/grafana/dashboards/14845) and DataHub dashboard.

In the JVM dashboard, you can find detailed charts based on JVM metrics like CPU/memory/disk usage. In the DataHub
dashboard, you can find charts to monitor each endpoint and the kafka topics. Using the example implementation, go
to http://localhost:3001 to find the grafana dashboards! (Username: admin, PW: admin)

To make it easy to track various metrics within the code base, we created MetricUtils class. This util class creates a
central metric registry, sets up the JMX reporter, and provides convenient functions for setting up counters and timers.
You can run the following to create a counter and increment.

```java
MetricUtils.counter(this.getClass(),"metricName").increment();
```

You can run the following to time a block of code.

```java
try(Timer.Context ignored=MetricUtils.timer(this.getClass(),"timerName").timer()){
    ...block of code
    }
```

## Enable monitoring through docker-compose

We provide some example configuration for enabling monitoring in
this [directory](https://github.com/datahub-project/datahub/tree/master/docker/monitoring). Take a look at the docker-compose
files, which adds necessary env variables to existing containers, and spawns new containers (Jaeger, Prometheus,
Grafana).

You can add in the above docker-compose using the `-f <<path-to-compose-file>>` when running docker-compose commands.
For instance,

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

We set up quickstart.sh, dev.sh, and dev-without-neo4j.sh to add the above docker-compose when MONITORING=true. For
instance `MONITORING=true ./docker/quickstart.sh` will add the correct env variables to start collecting traces and
metrics, and also deploy Jaeger, Prometheus, and Grafana. We will soon support this as a flag during quickstart. 