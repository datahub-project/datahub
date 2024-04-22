package datahub.spark;

import static datahub.spark.conf.SparkConfigParser.*;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import datahub.client.rest.RestEmitterConfig;
import datahub.spark.conf.DatahubEmitterConfig;
import datahub.spark.conf.RestDatahubEmitterConfig;
import datahub.spark.conf.SparkAppContext;
import datahub.spark.conf.SparkConfigParser;
import datahub.spark.conf.SparkLineageConf;
import io.openlineage.spark.agent.OpenLineageSparkListener;
import io.openlineage.spark.agent.lifecycle.ContextFactory;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.spark.SparkEnv;
import org.apache.spark.SparkEnv$;
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

public class DatahubSparkListener extends SparkListener {
  private static final Logger log = LoggerFactory.getLogger(DatahubSparkListener.class);
  private final Map<String, Instant> batchLastUpdated = new HashMap<String, Instant>();
  private final OpenLineageSparkListener listener;
  private final DatahubEventEmitter emitter;
  private Config datahubConf = ConfigFactory.empty();
  private SparkAppContext appContext;

  public DatahubSparkListener() throws URISyntaxException {
    listener = new OpenLineageSparkListener();
    emitter = new DatahubEventEmitter();
    ContextFactory contextFactory = new ContextFactory(emitter);
    OpenLineageSparkListener.init(contextFactory);
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

    log.debug("Application start called");
    this.appContext = getSparkAppContext(applicationStart);

    listener.onApplicationStart(applicationStart);
    long elapsedTime = System.currentTimeMillis() - startTime;
    log.debug("onApplicationStart completed successfully in {} ms", elapsedTime);
  }

  public Optional<DatahubEmitterConfig> initializeEmitter(Config sparkConf) {
    String emitterType =
        sparkConf.hasPath(SparkConfigParser.TRANSPORT_KEY)
            ? sparkConf.getString(SparkConfigParser.TRANSPORT_KEY)
            : "rest";
    if (emitterType.equals("rest")) {
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
              .build();
      return Optional.of(new RestDatahubEmitterConfig(restEmitterConf));
    } else {
      log.error(
          "DataHub Transport {} not recognized. DataHub Lineage emission will not work",
          emitterType);
    }

    return Optional.empty();
  }

  private synchronized void loadDatahubConfig(SparkAppContext appContext, Properties properties) {
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
    log.info("Datahub configuration: {}", datahubConf.root().render());
    Optional<DatahubEmitterConfig> restEmitter = initializeEmitter(datahubConf);
    SparkLineageConf sparkLineageConf =
        SparkLineageConf.toSparkLineageConf(datahubConf, appContext, restEmitter.orElse(null));

    emitter.setConfig(sparkLineageConf);
    long elapsedTime = System.currentTimeMillis() - startTime;
    log.debug("loadDatahubConfig completed successfully in {} ms", elapsedTime);
  }

  public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
    long startTime = System.currentTimeMillis();

    log.debug("Application end called");
    listener.onApplicationEnd(applicationEnd);
    if (datahubConf.hasPath(STREAMING_JOB) && (datahubConf.getBoolean(STREAMING_JOB))) {
      return;
    }
    emitter.emitCoalesced();
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
    log.debug("Job start called");
    loadDatahubConfig(this.appContext, jobStart.properties());
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
}
