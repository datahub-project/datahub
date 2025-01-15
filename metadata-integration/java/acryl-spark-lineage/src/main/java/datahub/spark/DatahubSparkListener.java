package datahub.spark;

import static datahub.spark.conf.SparkConfigParser.*;
import static io.openlineage.spark.agent.util.ScalaConversionUtils.*;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import datahub.client.file.FileEmitterConfig;
import datahub.client.kafka.KafkaEmitterConfig;
import datahub.client.rest.RestEmitterConfig;
import datahub.client.s3.S3EmitterConfig;
import datahub.spark.conf.DatahubEmitterConfig;
import datahub.spark.conf.FileDatahubEmitterConfig;
import datahub.spark.conf.KafkaDatahubEmitterConfig;
import datahub.spark.conf.RestDatahubEmitterConfig;
import datahub.spark.conf.S3DatahubEmitterConfig;
import datahub.spark.conf.SparkAppContext;
import datahub.spark.conf.SparkConfigParser;
import datahub.spark.conf.SparkLineageConf;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.openlineage.client.OpenLineageConfig;
import io.openlineage.client.circuitBreaker.CircuitBreaker;
import io.openlineage.client.circuitBreaker.CircuitBreakerFactory;
import io.openlineage.client.circuitBreaker.NoOpCircuitBreaker;
import io.openlineage.client.metrics.MicrometerProvider;
import io.openlineage.spark.agent.ArgumentParser;
import io.openlineage.spark.agent.OpenLineageSparkListener;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.lifecycle.ContextFactory;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext$;
import org.apache.spark.SparkEnv;
import org.apache.spark.SparkEnv$;
import org.apache.spark.package$;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function0;
import scala.Option;

public class DatahubSparkListener extends SparkListener {
  private static final Logger log = LoggerFactory.getLogger(DatahubSparkListener.class);
  private final Map<String, Instant> batchLastUpdated = new HashMap<String, Instant>();
  private final OpenLineageSparkListener listener;
  private DatahubEventEmitter emitter;
  private Config datahubConf = ConfigFactory.empty();
  private SparkAppContext appContext;
  private static ContextFactory contextFactory;
  private static CircuitBreaker circuitBreaker = new NoOpCircuitBreaker();
  private static final String sparkVersion = package$.MODULE$.SPARK_VERSION();

  private final Function0<Option<SparkContext>> activeSparkContext =
      ScalaConversionUtils.toScalaFn(SparkContext$.MODULE$::getActive);

  private static MeterRegistry meterRegistry;
  private boolean isDisabled;

  public DatahubSparkListener() throws URISyntaxException {
    listener = new OpenLineageSparkListener();
  }

  private static SparkAppContext getSparkAppContext(
      SparkListenerApplicationStart applicationStart) {
    SparkAppContext appContext = new SparkAppContext();
    appContext.setAppName(applicationStart.appName());
    if (applicationStart.appAttemptId().isDefined()) {
      appContext.setAppAttemptId(applicationStart.appAttemptId().get());
    }
    appContext.setSparkUser(applicationStart.sparkUser());
    appContext.setStartTime(applicationStart.time());
    appContext.setAppId(applicationStart.appId().get());
    return appContext;
  }

  public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
    long startTime = System.currentTimeMillis();

    log.info("Application start called");
    this.appContext = getSparkAppContext(applicationStart);
    initializeContextFactoryIfNotInitialized();
    listener.onApplicationStart(applicationStart);
    long elapsedTime = System.currentTimeMillis() - startTime;
    log.info("onApplicationStart completed successfully in {} ms", elapsedTime);
  }

  public Optional<DatahubEmitterConfig> initializeEmitter(Config sparkConf) {
    String emitterType =
        sparkConf.hasPath(SparkConfigParser.EMITTER_TYPE)
            ? sparkConf.getString(SparkConfigParser.EMITTER_TYPE)
            : "rest";
    switch (emitterType) {
      case "rest":
        String gmsUrl =
            sparkConf.hasPath(SparkConfigParser.GMS_URL_KEY)
                ? sparkConf.getString(SparkConfigParser.GMS_URL_KEY)
                : "http://localhost:8080";
        String token =
            sparkConf.hasPath(SparkConfigParser.GMS_AUTH_TOKEN)
                ? sparkConf.getString(SparkConfigParser.GMS_AUTH_TOKEN)
                : null;
        boolean disableSslVerification =
            sparkConf.hasPath(SparkConfigParser.DISABLE_SSL_VERIFICATION_KEY)
                && sparkConf.getBoolean(SparkConfigParser.DISABLE_SSL_VERIFICATION_KEY);
        boolean disableChunkedEncoding =
            sparkConf.hasPath(SparkConfigParser.REST_DISABLE_CHUNKED_ENCODING)
                && sparkConf.getBoolean(SparkConfigParser.REST_DISABLE_CHUNKED_ENCODING);
        int retry_interval_in_sec =
            sparkConf.hasPath(SparkConfigParser.RETRY_INTERVAL_IN_SEC)
                ? sparkConf.getInt(SparkConfigParser.RETRY_INTERVAL_IN_SEC)
                : 5;

        int max_retries =
            sparkConf.hasPath(SparkConfigParser.MAX_RETRIES)
                ? sparkConf.getInt(SparkConfigParser.MAX_RETRIES)
                : 0;

        log.info(
            "REST Emitter Configuration: GMS url {}{}",
            gmsUrl,
            (sparkConf.hasPath(SparkConfigParser.GMS_URL_KEY) ? "" : "(default)"));
        if (token != null) {
          log.info("REST Emitter Configuration: Token {}", "XXXXX");
        }

        if (disableSslVerification) {
          log.warn("REST Emitter Configuration: ssl verification will be disabled.");
        }

        RestEmitterConfig restEmitterConf =
            RestEmitterConfig.builder()
                .server(gmsUrl)
                .token(token)
                .disableSslVerification(disableSslVerification)
                .maxRetries(max_retries)
                .retryIntervalSec(retry_interval_in_sec)
                .disableChunkedEncoding(disableChunkedEncoding)
                .build();
        return Optional.of(new RestDatahubEmitterConfig(restEmitterConf));
      case "kafka":
        KafkaEmitterConfig.KafkaEmitterConfigBuilder kafkaEmitterConfig =
            KafkaEmitterConfig.builder();
        if (sparkConf.hasPath(SparkConfigParser.KAFKA_EMITTER_BOOTSTRAP)) {
          kafkaEmitterConfig.bootstrap(
              sparkConf.getString(SparkConfigParser.KAFKA_EMITTER_BOOTSTRAP));
        }
        if (sparkConf.hasPath(SparkConfigParser.KAFKA_EMITTER_SCHEMA_REGISTRY_URL)) {
          kafkaEmitterConfig.schemaRegistryUrl(
              sparkConf.getString(SparkConfigParser.KAFKA_EMITTER_SCHEMA_REGISTRY_URL));
        }

        if (sparkConf.hasPath(KAFKA_EMITTER_SCHEMA_REGISTRY_CONFIG)) {
          Map<String, String> schemaRegistryConfig = new HashMap<>();
          sparkConf
              .getConfig(KAFKA_EMITTER_SCHEMA_REGISTRY_CONFIG)
              .entrySet()
              .forEach(
                  entry -> {
                    schemaRegistryConfig.put(
                        entry.getKey(), entry.getValue().unwrapped().toString());
                  });
          kafkaEmitterConfig.schemaRegistryConfig(schemaRegistryConfig);
        }

        if (sparkConf.hasPath(KAFKA_EMITTER_PRODUCER_CONFIG)) {
          Map<String, String> kafkaConfig = new HashMap<>();
          sparkConf
              .getConfig(KAFKA_EMITTER_PRODUCER_CONFIG)
              .entrySet()
              .forEach(
                  entry -> {
                    kafkaConfig.put(entry.getKey(), entry.getValue().unwrapped().toString());
                  });
          kafkaEmitterConfig.producerConfig(kafkaConfig);
        }
        if (sparkConf.hasPath(SparkConfigParser.KAFKA_MCP_TOPIC)) {
          String mcpTopic = sparkConf.getString(SparkConfigParser.KAFKA_MCP_TOPIC);
          return Optional.of(new KafkaDatahubEmitterConfig(kafkaEmitterConfig.build(), mcpTopic));
        } else {
          return Optional.of(new KafkaDatahubEmitterConfig(kafkaEmitterConfig.build()));
        }
      case "file":
        log.info("File Emitter Configuration: File emitter will be used");
        FileEmitterConfig.FileEmitterConfigBuilder fileEmitterConfig = FileEmitterConfig.builder();
        fileEmitterConfig.fileName(sparkConf.getString(SparkConfigParser.FILE_EMITTER_FILE_NAME));
        return Optional.of(new FileDatahubEmitterConfig(fileEmitterConfig.build()));
      case "s3":
        log.info("S3 Emitter Configuration: S3 emitter will be used");
        S3EmitterConfig.S3EmitterConfigBuilder s3EmitterConfig = S3EmitterConfig.builder();
        if (sparkConf.hasPath(SparkConfigParser.S3_EMITTER_BUCKET)) {
          s3EmitterConfig.bucketName(sparkConf.getString(SparkConfigParser.S3_EMITTER_BUCKET));
        }

        if (sparkConf.hasPath(SparkConfigParser.S3_EMITTER_PREFIX)) {
          s3EmitterConfig.pathPrefix(sparkConf.getString(SparkConfigParser.S3_EMITTER_PREFIX));
        }

        if (sparkConf.hasPath(SparkConfigParser.S3_EMITTER_REGION)) {
          s3EmitterConfig.region(sparkConf.getString(SparkConfigParser.S3_EMITTER_REGION));
        }

        if (sparkConf.hasPath(S3_EMITTER_PROFILE)) {
          s3EmitterConfig.profileName(sparkConf.getString(S3_EMITTER_PROFILE));
        }

        if (sparkConf.hasPath(S3_EMITTER_ENDPOINT)) {
          s3EmitterConfig.endpoint(sparkConf.getString(S3_EMITTER_ENDPOINT));
        }

        if (sparkConf.hasPath(S3_EMITTER_ACCESS_KEY)) {
          s3EmitterConfig.accessKey(sparkConf.getString(S3_EMITTER_ACCESS_KEY));
        }

        if (sparkConf.hasPath(S3_EMITTER_SECRET_KEY)) {
          s3EmitterConfig.secretKey(sparkConf.getString(S3_EMITTER_SECRET_KEY));
        }

        if (sparkConf.hasPath(S3_EMITTER_FILE_NAME)) {
          s3EmitterConfig.fileName(sparkConf.getString(S3_EMITTER_FILE_NAME));
        }

        return Optional.of(new S3DatahubEmitterConfig(s3EmitterConfig.build()));
      default:
        log.error(
            "DataHub Transport {} not recognized. DataHub Lineage emission will not work",
            emitterType);
        break;
    }

    return Optional.empty();
  }

  private synchronized SparkLineageConf loadDatahubConfig(
      SparkAppContext appContext, Properties properties) {
    long startTime = System.currentTimeMillis();
    datahubConf = parseSparkConfig();
    SparkEnv sparkEnv = SparkEnv$.MODULE$.get();
    if (sparkEnv != null) {
      log.info("sparkEnv: {}", sparkEnv.conf().toDebugString());
      sparkEnv.conf().set("spark.openlineage.facets.disabled", "[spark_unknown;spark.logicalPlan]");
    }

    if (properties != null) {
      datahubConf = parsePropertiesToConfig(properties);
      Optional<Map<String, String>> databricksTags = getDatabricksTags(datahubConf);
      this.appContext.setDatabricksTags(databricksTags.orElse(null));
    }

    Optional<DatahubEmitterConfig> emitterConfig = initializeEmitter(datahubConf);
    SparkLineageConf sparkLineageConf =
        SparkLineageConf.toSparkLineageConf(datahubConf, appContext, emitterConfig.orElse(null));

    long elapsedTime = System.currentTimeMillis() - startTime;
    log.debug("loadDatahubConfig completed successfully in {} ms", elapsedTime);
    return sparkLineageConf;
  }

  public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
    long startTime = System.currentTimeMillis();
    initializeContextFactoryIfNotInitialized();

    log.debug("Application end called");
    listener.onApplicationEnd(applicationEnd);
    if (datahubConf.hasPath(STREAMING_JOB) && (datahubConf.getBoolean(STREAMING_JOB))) {
      return;
    }
    if (emitter != null) {
      emitter.emitCoalesced();
    } else {
      log.warn("Emitter is not initialized, unable to emit coalesced events");
    }

    long elapsedTime = System.currentTimeMillis() - startTime;
    log.debug("onApplicationEnd completed successfully in {} ms", elapsedTime);
  }

  public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
    long startTime = System.currentTimeMillis();

    log.debug("Task end called");
    listener.onTaskEnd(taskEnd);
    long elapsedTime = System.currentTimeMillis() - startTime;
    log.debug("onTaskEnd completed successfully in {} ms", elapsedTime);
  }

  public void onJobEnd(SparkListenerJobEnd jobEnd) {
    long startTime = System.currentTimeMillis();

    log.debug("Job end called");
    listener.onJobEnd(jobEnd);
    long elapsedTime = System.currentTimeMillis() - startTime;
    log.debug("onJobEnd completed successfully in {} ms", elapsedTime);
  }

  public void onJobStart(SparkListenerJobStart jobStart) {
    long startTime = System.currentTimeMillis();
    initializeContextFactoryIfNotInitialized();

    log.debug("Job start called");
    listener.onJobStart(jobStart);
    long elapsedTime = System.currentTimeMillis() - startTime;
    log.debug("onJobStart completed successfully in {} ms", elapsedTime);
  }

  public void onOtherEvent(SparkListenerEvent event) {
    long startTime = System.currentTimeMillis();

    log.debug("Other event called {}", event.getClass().getName());
    // Switch to streaming mode if streaming mode is not set, but we get a progress event
    if ((event instanceof StreamingQueryListener.QueryProgressEvent)
        || (event instanceof StreamingQueryListener.QueryStartedEvent)) {
      if (!emitter.isStreaming()) {
        if (!datahubConf.hasPath(STREAMING_JOB)) {
          log.info("Streaming mode not set explicitly, switching to streaming mode");
          emitter.setStreaming(true);
        } else {
          emitter.setStreaming(datahubConf.getBoolean(STREAMING_JOB));
          log.info("Streaming mode set to {}", datahubConf.getBoolean(STREAMING_JOB));
        }
      }
    }

    if (datahubConf.hasPath(STREAMING_JOB) && !datahubConf.getBoolean(STREAMING_JOB)) {
      log.info("Not in streaming mode");
      return;
    }

    listener.onOtherEvent(event);

    if (event instanceof StreamingQueryListener.QueryProgressEvent) {
      int streamingHeartbeatIntervalSec = SparkConfigParser.getStreamingHeartbeatSec(datahubConf);
      StreamingQueryListener.QueryProgressEvent queryProgressEvent =
          (StreamingQueryListener.QueryProgressEvent) event;
      ((StreamingQueryListener.QueryProgressEvent) event).progress().id();
      if ((batchLastUpdated.containsKey(queryProgressEvent.progress().id().toString()))
          && (batchLastUpdated
              .get(queryProgressEvent.progress().id().toString())
              .isAfter(Instant.now().minusSeconds(streamingHeartbeatIntervalSec)))) {
        log.debug(
            "Skipping lineage emit as it was emitted in the last {} seconds",
            streamingHeartbeatIntervalSec);
        return;
      }
      try {
        batchLastUpdated.put(queryProgressEvent.progress().id().toString(), Instant.now());
        emitter.emit(queryProgressEvent.progress());
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
      log.debug("Query progress event: {}", queryProgressEvent.progress());
      long elapsedTime = System.currentTimeMillis() - startTime;
      log.debug("onOtherEvent completed successfully in {} ms", elapsedTime);
    }
  }

  private static void initializeMetrics(OpenLineageConfig openLineageConfig) {
    meterRegistry =
        MicrometerProvider.addMeterRegistryFromConfig(openLineageConfig.getMetricsConfig());
    String disabledFacets;
    if (openLineageConfig.getFacetsConfig() != null
        && openLineageConfig.getFacetsConfig().getDisabledFacets() != null) {
      disabledFacets =
          String.join(";", openLineageConfig.getFacetsConfig().getEffectiveDisabledFacets());
    } else {
      disabledFacets = "";
    }

    ((CompositeMeterRegistry) meterRegistry)
        .getRegistries()
        .forEach(
            r ->
                r.config()
                    .commonTags(
                        Tags.of(
                            Tag.of("openlineage.spark.integration.version", Versions.getVersion()),
                            Tag.of("openlineage.spark.version", sparkVersion),
                            Tag.of("openlineage.spark.disabled.facets", disabledFacets))));
  }

  private void initializeContextFactoryIfNotInitialized() {
    if (contextFactory != null || isDisabled) {
      return;
    }
    asJavaOptional(activeSparkContext.apply())
        .ifPresent(context -> initializeContextFactoryIfNotInitialized(context.appName()));
  }

  private void initializeContextFactoryIfNotInitialized(String appName) {
    if (contextFactory != null || isDisabled) {
      return;
    }
    SparkEnv sparkEnv = SparkEnv$.MODULE$.get();
    if (sparkEnv == null) {
      log.warn(
          "OpenLineage listener instantiated, but no configuration could be found. "
              + "Lineage events will not be collected");
      return;
    }
    initializeContextFactoryIfNotInitialized(sparkEnv.conf(), appName);
  }

  private void initializeContextFactoryIfNotInitialized(SparkConf sparkConf, String appName) {
    if (contextFactory != null || isDisabled) {
      return;
    }
    try {
      SparkLineageConf datahubConfig = loadDatahubConfig(appContext, null);
      SparkOpenLineageConfig config = ArgumentParser.parse(sparkConf);
      // Needs to be done before initializing OpenLineageClient
      initializeMetrics(config);
      emitter = new DatahubEventEmitter(config, appName);
      emitter.setConfig(datahubConfig);
      contextFactory = new ContextFactory(emitter, meterRegistry, config);
      circuitBreaker = new CircuitBreakerFactory(config.getCircuitBreaker()).build();
      OpenLineageSparkListener.init(contextFactory);
    } catch (URISyntaxException e) {
      log.error("Unable to parse OpenLineage endpoint. Lineage events will not be collected", e);
    }
  }
}
