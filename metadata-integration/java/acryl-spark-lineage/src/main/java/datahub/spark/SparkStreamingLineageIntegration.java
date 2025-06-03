package datahub.spark;

import datahub.spark.conf.SparkLineageConf;
import io.openlineage.spark.agent.ArgumentParser;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.lang.reflect.Field;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryListener;

/**
 * Utility class to help integrate DataHub lineage capturing for Spark streaming queries. This class
 * provides methods to set up both types of streaming query listeners and log interceptors: 1.
 * MicroBatchExecution - for micro-batch processing mode 2. ContinuousExecution - for continuous
 * processing mode
 */
@Slf4j
public class SparkStreamingLineageIntegration {

  /**
   * Register all DataHub lineage components for capturing streaming query events. This includes: 1.
   * The MicroBatchExecutionListener and ContinuousExecutionListener to capture streaming query
   * events 2. The log interceptors to capture logs from both execution engines
   *
   * @param spark The SparkSession to register components with
   * @param datahubConf The DataHub lineage configuration
   * @param appName The name of the Spark application
   * @return The configured DatahubEventEmitter
   */
  public static DatahubEventEmitter setupLineageCapturing(
      SparkSession spark, SparkLineageConf datahubConf, String appName) {

    try {
      log.info("Setting up DataHub lineage capturing for Spark streaming queries");

      // Create and configure the DataHub event emitter
      SparkContext sc = spark.sparkContext();
      SparkOpenLineageConfig openLineageConfig = ArgumentParser.parse(sc.getConf());

      DatahubEventEmitter emitter = new DatahubEventEmitter(openLineageConfig, appName);
      emitter.setConfig(datahubConf);

      // Install the log interceptors to capture execution logs
      MicroBatchLogInterceptor.install(emitter);
      ContinuousExecutionLogInterceptor.install(emitter);

      // Register the streaming query listeners with Spark
      // Note: The listeners are already initialized by the emitter
      registerStreamingQueryListeners(spark, emitter);

      log.info("DataHub lineage capturing for streaming queries set up successfully");
      return emitter;

    } catch (Exception e) {
      log.error("Failed to set up DataHub lineage capturing", e);
      throw new RuntimeException("Failed to set up DataHub lineage capturing", e);
    }
  }

  /** Register the streaming query listeners with Spark. */
  private static void registerStreamingQueryListeners(
      SparkSession spark, DatahubEventEmitter emitter) {
    // Register the MicroBatchExecutionListener
    if (emitter.microBatchListener != null) {
      spark.streams().addListener(new ForwardingStreamingQueryListener(emitter.microBatchListener));
      log.info("Registered MicroBatchExecutionListener with Spark");
    }

    // Register the ContinuousExecutionListener
    if (emitter.continuousListener != null) {
      spark.streams().addListener(new ForwardingStreamingQueryListener(emitter.continuousListener));
      log.info("Registered ContinuousExecutionListener with Spark");
    }
  }

  /**
   * Simple forwarding listener that sends events to another StreamingQueryListener. This is needed
   * because we can't register the listeners directly (they're fields in the DatahubEventEmitter).
   */
  private static class ForwardingStreamingQueryListener extends StreamingQueryListener {
    private final StreamingQueryListener delegate;

    public ForwardingStreamingQueryListener(StreamingQueryListener delegate) {
      this.delegate = delegate;
    }

    @Override
    public void onQueryStarted(StreamingQueryListener.QueryStartedEvent event) {
      delegate.onQueryStarted(event);
    }

    @Override
    public void onQueryProgress(StreamingQueryListener.QueryProgressEvent event) {
      delegate.onQueryProgress(event);
    }

    @Override
    public void onQueryTerminated(StreamingQueryListener.QueryTerminatedEvent event) {
      delegate.onQueryTerminated(event);
    }
  }

  /**
   * Verifies that the DataHub lineage components are properly installed. This can be used to check
   * if the log interceptors and streaming listeners are properly registered after setup.
   *
   * @param spark The SparkSession to verify
   * @return true if all components are properly installed
   */
  public static boolean verifyLineageComponents(SparkSession spark) {
    boolean allComponentsInstalled = true;

    try {
      log.info("Verifying DataHub lineage components installation");

      // Check if the MicroBatchExecution logger has our appender
      try {
        Class<?> loggerClass = Class.forName("org.slf4j.LoggerFactory");
        Object logger =
            loggerClass
                .getMethod("getLogger", String.class)
                .invoke(null, "org.apache.spark.sql.execution.streaming.MicroBatchExecution");

        // Try to get the root logger and check appenders
        boolean microBatchAppenderFound =
            checkForDataHubAppender(logger, "DataHubMicroBatchAppender");

        if (microBatchAppenderFound) {
          log.info("MicroBatchExecution log interceptor is properly installed");
        } else {
          log.warn("MicroBatchExecution log interceptor may not be properly installed");
          allComponentsInstalled = false;
        }
      } catch (Exception e) {
        log.warn("Failed to check MicroBatchExecution log interceptor: {}", e.getMessage(), e);
        allComponentsInstalled = false;
      }

      // Check if the ContinuousExecution logger has our appender
      try {
        Class<?> loggerClass = Class.forName("org.slf4j.LoggerFactory");
        Object logger =
            loggerClass
                .getMethod("getLogger", String.class)
                .invoke(
                    null,
                    "org.apache.spark.sql.execution.streaming.continuous.ContinuousExecution");

        // Try to get the root logger and check appenders
        boolean continuousAppenderFound =
            checkForDataHubAppender(logger, "DataHubContinuousExecAppender");

        if (continuousAppenderFound) {
          log.info("ContinuousExecution log interceptor is properly installed");
        } else {
          log.warn("ContinuousExecution log interceptor may not be properly installed");
          allComponentsInstalled = false;
        }
      } catch (Exception e) {
        log.warn("Failed to check ContinuousExecution log interceptor: {}", e.getMessage(), e);
        allComponentsInstalled = false;
      }

      // Check if streaming query listeners are registered
      try {
        // There's no direct way to check for listeners, so we'll just log a message
        log.info(
            "Cannot directly verify streaming query listeners - assume they're installed if no errors occurred during setup");
      } catch (Exception e) {
        log.warn("Failed to check streaming query listeners: {}", e.getMessage(), e);
        allComponentsInstalled = false;
      }

      if (allComponentsInstalled) {
        log.info("All DataHub lineage components appear to be properly installed");
      } else {
        log.warn("Some DataHub lineage components may not be properly installed");
      }

      return allComponentsInstalled;
    } catch (Exception e) {
      log.error("Error verifying DataHub lineage components", e);
      return false;
    }
  }

  /** Helper method to check if a logger has our custom appender installed. */
  private static boolean checkForDataHubAppender(Object logger, String appenderName) {
    try {
      // First try Log4j appender check
      try {
        Field loggerField = logger.getClass().getDeclaredField("logger");
        loggerField.setAccessible(true);
        Object log4jLogger = loggerField.get(logger);

        // Get appenders from the logger
        java.util.Enumeration<?> appenders =
            (java.util.Enumeration<?>)
                log4jLogger.getClass().getMethod("getAllAppenders").invoke(log4jLogger);

        // Check if our appender is in the list
        while (appenders.hasMoreElements()) {
          Object appender = appenders.nextElement();
          String name = (String) appender.getClass().getMethod("getName").invoke(appender);
          if (appenderName.equals(name)) {
            return true;
          }
        }
      } catch (Exception e) {
        // Not a Log4j logger or error accessing it
        log.debug("Not a Log4j logger or error checking appenders: {}", e.getMessage());
      }

      // Then try Logback appender check
      try {
        Field loggerField = logger.getClass().getDeclaredField("logger");
        loggerField.setAccessible(true);
        Object logbackLogger = loggerField.get(logger);

        // Get appenders from the logger
        java.util.Iterator<?> appenderIt =
            (java.util.Iterator<?>)
                logbackLogger.getClass().getMethod("iteratorForAppenders").invoke(logbackLogger);

        // Check if our appender is in the list
        while (appenderIt.hasNext()) {
          Object appender = appenderIt.next();
          String name = (String) appender.getClass().getMethod("getName").invoke(appender);
          if (appenderName.equals(name)) {
            return true;
          }
        }
      } catch (Exception e) {
        // Not a Logback logger or error accessing it
        log.debug("Not a Logback logger or error checking appenders: {}", e.getMessage());
      }

      // We couldn't confirm the appender is installed
      return false;
    } catch (Exception e) {
      log.warn("Error checking for DataHub appender: {}", e.getMessage(), e);
      return false;
    }
  }
}
