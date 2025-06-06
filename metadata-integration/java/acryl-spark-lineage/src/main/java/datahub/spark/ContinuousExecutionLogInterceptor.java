package datahub.spark;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * Log interceptor for ContinuousExecution logs.
 *
 * <p>This class creates a log interceptor for Spark's ContinuousExecution engine logs. When a
 * relevant log is captured, it forwards it to the DatahubEventEmitter for processing.
 */
@Slf4j
public class ContinuousExecutionLogInterceptor {
  private static final String LOGGER_NAME =
      "org.apache.spark.sql.execution.streaming.continuous.ContinuousExecution";
  private static volatile DatahubEventEmitter emitter = null;

  /**
   * Install the log interceptor with the given DatahubEventEmitter.
   *
   * @param eventEmitter The DatahubEventEmitter that will process captured logs
   * @return true if installation was successful, false otherwise
   */
  public static boolean install(DatahubEventEmitter eventEmitter) {
    if (eventEmitter == null) {
      log.warn("Cannot install ContinuousExecutionLogInterceptor: emitter is null");
      return false;
    }

    emitter = eventEmitter;

    try {
      log.info("Installing ContinuousExecutionLogInterceptor");

      // Try to install log interceptor using reflection to avoid direct dependency on log
      // frameworks
      try {
        // Get the logger for ContinuousExecution
        Class<?> loggerFactoryClass = Class.forName("org.slf4j.LoggerFactory");
        Object logger =
            loggerFactoryClass.getMethod("getLogger", String.class).invoke(null, LOGGER_NAME);

        // Check if this is a Log4j logger or Logback logger
        String loggerClassName = logger.getClass().getName();

        if (loggerClassName.contains("Log4j")) {
          return installLog4jAppender(logger);
        } else if (loggerClassName.contains("Logback")) {
          return installLogbackAppender(logger);
        } else {
          log.warn(
              "Unknown logger implementation: {}, cannot install interceptor", loggerClassName);
          return false;
        }
      } catch (ClassNotFoundException e) {
        log.warn("SLF4J not found, ContinuousExecutionLogInterceptor may not work properly");
        return false;
      }
    } catch (Exception e) {
      log.error("Error installing ContinuousExecutionLogInterceptor", e);
      return false;
    }
  }

  /** Install a Log4j appender using reflection. */
  private static boolean installLog4jAppender(Object logger) {
    try {
      log.info("Installing Log4j appender for ContinuousExecution");

      // Get the underlying log4j logger
      Field loggerField = logger.getClass().getDeclaredField("logger");
      loggerField.setAccessible(true);
      Object log4jLogger = loggerField.get(logger);

      // Create a custom appender using proxy
      Class<?> appenderClass = Class.forName("org.apache.log4j.AppenderSkeleton");
      Object appender =
          java.lang.reflect.Proxy.newProxyInstance(
              appenderClass.getClassLoader(),
              new Class<?>[] {appenderClass},
              new java.lang.reflect.InvocationHandler() {
                @Override
                public Object invoke(Object proxy, java.lang.reflect.Method method, Object[] args) {
                  String methodName = method.getName();

                  if ("append".equals(methodName) && args != null && args.length > 0) {
                    try {
                      // Extract message from log event
                      Object event = args[0];
                      String message = null;

                      try {
                        Method getMessageMethod = event.getClass().getMethod("getRenderedMessage");
                        message = (String) getMessageMethod.invoke(event);
                      } catch (Exception e) {
                        // Try alternative method
                        try {
                          Method getMessageMethod = event.getClass().getMethod("getMessage");
                          Object msg = getMessageMethod.invoke(event);
                          message = msg != null ? msg.toString() : null;
                        } catch (Exception e2) {
                          log.warn("Failed to extract message from log event", e2);
                        }
                      }

                      if (message != null) {
                        // Get log level
                        String level = "INFO";
                        try {
                          Method getLevelMethod = event.getClass().getMethod("getLevel");
                          Object levelObj = getLevelMethod.invoke(event);
                          level = levelObj != null ? levelObj.toString() : "INFO";
                        } catch (Exception e) {
                          // Use default level
                        }

                        // Process the message
                        processLogMessage(message, level);
                      }
                    } catch (Exception e) {
                      log.warn("Error processing log event", e);
                    }
                  } else if ("getName".equals(methodName)) {
                    return "DataHubContinuousExecAppender";
                  } else if ("requiresLayout".equals(methodName)) {
                    return false;
                  } else if ("close".equals(methodName)
                      || "setName".equals(methodName)
                      || "setLayout".equals(methodName)) {
                    return null;
                  }

                  // Default return
                  return null;
                }
              });

      // Add appender to logger
      log4jLogger.getClass().getMethod("addAppender", appenderClass).invoke(log4jLogger, appender);

      log.info("Successfully installed Log4j appender for ContinuousExecution");
      return true;

    } catch (Exception e) {
      log.error("Error installing Log4j appender", e);
      return false;
    }
  }

  /** Install a Logback appender using reflection. */
  private static boolean installLogbackAppender(Object logger) {
    try {
      log.info("Installing Logback appender for ContinuousExecution");

      // Get the underlying logback logger
      Field loggerField = logger.getClass().getDeclaredField("logger");
      loggerField.setAccessible(true);
      Object logbackLogger = loggerField.get(logger);

      // Create a custom appender using proxy
      Class<?> appenderBaseClass = Class.forName("ch.qos.logback.core.AppenderBase");
      Object appender =
          java.lang.reflect.Proxy.newProxyInstance(
              appenderBaseClass.getClassLoader(),
              new Class<?>[] {appenderBaseClass},
              new java.lang.reflect.InvocationHandler() {
                @Override
                public Object invoke(Object proxy, java.lang.reflect.Method method, Object[] args) {
                  String methodName = method.getName();

                  if ("append".equals(methodName) && args != null && args.length > 0) {
                    try {
                      // Extract message from log event
                      Object event = args[0];
                      String message = null;

                      try {
                        Method getMessageMethod = event.getClass().getMethod("getFormattedMessage");
                        message = (String) getMessageMethod.invoke(event);
                      } catch (Exception e) {
                        // Try alternative method
                        try {
                          Method getMessageMethod = event.getClass().getMethod("getMessage");
                          Object msg = getMessageMethod.invoke(event);
                          message = msg != null ? msg.toString() : null;
                        } catch (Exception e2) {
                          log.warn("Failed to extract message from log event", e2);
                        }
                      }

                      if (message != null) {
                        // Get log level
                        String level = "INFO";
                        try {
                          Method getLevelMethod = event.getClass().getMethod("getLevel");
                          Object levelObj = getLevelMethod.invoke(event);
                          level = levelObj != null ? levelObj.toString() : "INFO";
                        } catch (Exception e) {
                          // Use default level
                        }

                        // Process the message
                        processLogMessage(message, level);
                      }
                    } catch (Exception e) {
                      log.warn("Error processing log event", e);
                    }
                  } else if ("getName".equals(methodName)) {
                    return "DataHubContinuousExecAppender";
                  } else if ("isStarted".equals(methodName)) {
                    return true;
                  } else if ("start".equals(methodName)) {
                    try {
                      Field startedField = appenderBaseClass.getDeclaredField("started");
                      startedField.setAccessible(true);
                      startedField.set(proxy, true);
                    } catch (Exception e) {
                      log.warn("Failed to set started field", e);
                    }
                    return null;
                  } else if ("stop".equals(methodName)
                      || "setName".equals(methodName)
                      || "setContext".equals(methodName)) {
                    return null;
                  }

                  // Default return
                  return null;
                }
              });

      // Add appender to logger
      logbackLogger
          .getClass()
          .getMethod("addAppender", appenderBaseClass)
          .invoke(logbackLogger, appender);

      // Start the appender
      appenderBaseClass.getMethod("start").invoke(appender);

      log.info("Successfully installed Logback appender for ContinuousExecution");
      return true;

    } catch (Exception e) {
      log.error("Error installing Logback appender", e);
      return false;
    }
  }

  /** Process a log message from ContinuousExecution. */
  protected static void processLogMessage(String message, String level) {
    if (emitter == null) {
      log.warn(
          "No emitter registered for ContinuousExecutionLogInterceptor, can't process log: {}",
          message);
      return;
    }

    try {
      // Process the log message based on the content
      if (message.contains("Starting continuous processing")
          || message.contains("Committing offset")) {
        processContinuousExecutionLog(message);
      } else if (message.contains("Logical plan:")) {
        // Extract logical plan
        log.debug("Processing continuous logical plan log message: {}", message);
        processContinuousExecutionLog(message);
      } else if (message.contains("Progress report:")) {
        // Extract progress report
        log.debug("Processing continuous progress report log message: {}", message);
        processContinuousExecutionLog(message);
      } else if (message.contains("Sink:")
          || message.contains("output table:")
          || message.contains("target table:")) {
        // Sink information
        log.info("Detected sink information in continuous streaming log: {}", message);
        processContinuousExecutionLog(message);
      }
    } catch (Exception e) {
      log.error("Error processing log message: {}", message, e);
    }
  }

  /** Process a log message from ContinuousExecution and forward to the emitter. */
  private static void processContinuousExecutionLog(String logMessage) {
    try {
      // Extract the query ID from the log message
      String queryId = extractQueryId(logMessage);
      if (queryId == null) {
        log.warn("Could not extract query ID from log message: {}", logMessage);
        return;
      }

      Map<String, String> metadata = extractMetadata(logMessage);

      // Forward to the appropriate emitter method based on the message content
      if (logMessage.contains("Starting continuous processing")) {
        emitter.processMicroBatchStart(queryId, logMessage);
      } else if (logMessage.contains("Committing offset")) {
        emitter.processMicroBatchCommit(queryId, metadata, logMessage);
      } else if (logMessage.contains("Logical plan:")) {
        Map<String, String> logicalPlanMetadata = extractLogicalPlanMetadata(logMessage);
        emitter.processMicroBatchLogicalPlan(queryId, logicalPlanMetadata, logMessage);
      } else if (logMessage.contains("Progress report:")) {
        Map<String, String> progressMetadata = extractProgressReport(logMessage);
        emitter.processMicroBatchProgress(queryId, progressMetadata, logMessage);
      } else {
        // For other interesting messages
        emitter.processInterestingMessage(queryId, metadata, logMessage);
      }
    } catch (Exception e) {
      log.error("Error processing ContinuousExecution log: {}", e.getMessage(), e);
    }
  }

  /** Extract the query ID from a log message. */
  private static String extractQueryId(String logMessage) {
    // The query ID is typically in the format [queryId]
    java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("\\[([0-9a-f-]+)\\]");
    java.util.regex.Matcher matcher = pattern.matcher(logMessage);
    if (matcher.find()) {
      return matcher.group(1);
    }

    // Also try "id = queryId" format
    pattern = java.util.regex.Pattern.compile("id = ([0-9a-f-]+)");
    matcher = pattern.matcher(logMessage);
    if (matcher.find()) {
      return matcher.group(1);
    }

    return null;
  }

  /** Extract general metadata from a log message. */
  private static Map<String, String> extractMetadata(String logMessage) {
    Map<String, String> metadata = new HashMap<>();

    // Extract offset information for commit messages
    if (logMessage.contains("Committing offset")) {
      java.util.regex.Pattern offsetPattern =
          java.util.regex.Pattern.compile("offset: (\\{[^\\}]+\\})");
      java.util.regex.Matcher offsetMatcher = offsetPattern.matcher(logMessage);
      if (offsetMatcher.find()) {
        metadata.put("offset", offsetMatcher.group(1));
      }
    }

    return metadata;
  }

  /** Extract metadata from a logical plan log message. */
  private static Map<String, String> extractLogicalPlanMetadata(String logMessage) {
    Map<String, String> metadata = new HashMap<>();

    // Extract basic information
    metadata.putAll(extractMetadata(logMessage));

    // Extract the logical plan text
    int planStart = logMessage.indexOf("Logical plan:");
    if (planStart >= 0) {
      String plan = logMessage.substring(planStart + "Logical plan:".length()).trim();
      metadata.put("logicalPlan", plan);

      // Look for table references in the plan
      java.util.regex.Pattern tablePattern =
          java.util.regex.Pattern.compile("Relation\\[(\\w+)\\]\\[(\\w+)\\]");
      java.util.regex.Matcher tableMatcher = tablePattern.matcher(plan);
      if (tableMatcher.find()) {
        metadata.put("tableFormat", tableMatcher.group(1));
        metadata.put("tableName", tableMatcher.group(2));
      }
    }

    return metadata;
  }

  /** Extract metadata from a progress report log message. */
  private static Map<String, String> extractProgressReport(String logMessage) {
    Map<String, String> metadata = new HashMap<>();

    // Extract basic information
    metadata.putAll(extractMetadata(logMessage));

    // Look for rates and statistics
    java.util.regex.Pattern ratePattern = java.util.regex.Pattern.compile("(\\w+):\\s+([\\d.]+)");
    java.util.regex.Matcher rateMatcher = ratePattern.matcher(logMessage);
    while (rateMatcher.find()) {
      metadata.put(rateMatcher.group(1), rateMatcher.group(2));
    }

    return metadata;
  }
}
