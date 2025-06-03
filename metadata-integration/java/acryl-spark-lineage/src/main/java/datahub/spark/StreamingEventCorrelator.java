package datahub.spark;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.linkedin.common.DatasetUrnArray;
import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.data.template.StringMap;
import com.linkedin.datajob.DataFlowInfo;
import com.linkedin.datajob.DataJobInfo;
import com.linkedin.datajob.DataJobInputOutput;
import datahub.event.MetadataChangeProposalWrapper;
import datahub.spark.conf.SparkLineageConf;
import datahub.spark.converter.SparkStreamingEventToDatahub;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.streaming.StreamingQueryProgress;

/**
 * Maintains correlation between Spark streaming events to ensure consistent job lineage. This class
 * tracks streaming queries and their associated delta sinks to create unified jobs that show both
 * inputs and outputs together.
 */
@Slf4j
public class StreamingEventCorrelator {
  private static final long DEFAULT_EXPIRATION_MS = Duration.ofMinutes(10).toMillis();
  private static final String DATASET_ENV_KEY = "dataset.env";
  private static final String DELTA_PLATFORM_ALIAS_KEY = "dataset.deltaPlatformAlias";

  // Maps query IDs to the streaming event data
  private final Map<String, StreamingEventData> eventDataByQueryId = new ConcurrentHashMap<>();

  // Maps sink path to query ID to handle correlations
  private final Map<String, String> sinkPathToQueryId = new ConcurrentHashMap<>();

  // Duration after which cached events should expire if not matched
  private final long expirationMs;

  // Map of streaming query ID to information about the query
  private final Map<String, QueryInfo> queryInfoMap = new ConcurrentHashMap<>();

  // Cache of dataset URNs for faster lookup
  private final Map<String, DatasetUrn> datasetUrnCache = new ConcurrentHashMap<>();

  // A scheduler for cleanup tasks
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

  private SparkLineageConf conf;

  // Store catalog table metadata by queryId
  private final Map<String, Map<String, String>> catalogTableByQueryId = new ConcurrentHashMap<>();

  // Store ForeachBatchSink metadata by queryId
  private final Map<String, String> foreachBatchSinkByQueryId = new ConcurrentHashMap<>();

  /** Creates a new StreamingEventCorrelator. */
  public StreamingEventCorrelator() {
    this.expirationMs = DEFAULT_EXPIRATION_MS;

    // Schedule regular cleanup of expired query information
    scheduler.scheduleAtFixedRate(
        this::cleanupExpiredEvents,
        DEFAULT_EXPIRATION_MS,
        DEFAULT_EXPIRATION_MS,
        TimeUnit.MILLISECONDS);
  }

  /**
   * Creates a new StreamingEventCorrelator with the specified config.
   *
   * @param conf the DataHub lineage configuration
   */
  public StreamingEventCorrelator(SparkLineageConf conf) {
    this();
    this.conf = conf;
  }

  /**
   * Creates a new StreamingEventCorrelator with custom expiration timeout.
   *
   * @param expirationMs duration in milliseconds after which unmatched events expire
   */
  public StreamingEventCorrelator(long expirationMs) {
    this.expirationMs = expirationMs;
    log.info("Initialized StreamingEventCorrelator with expiration: {} ms", expirationMs);

    // Schedule regular cleanup of expired query information
    scheduler.scheduleAtFixedRate(
        this::cleanupExpiredEvents, expirationMs, expirationMs, TimeUnit.MILLISECONDS);
  }

  /**
   * Sets the configuration for this correlator.
   *
   * @param conf the DataHub lineage configuration
   */
  public void setConfig(SparkLineageConf conf) {
    this.conf = conf;
  }

  /** Returns the FabricType from configuration, defaulting to PROD if not specified or invalid. */
  private FabricType getFabricType() {
    if (conf == null) {
      return FabricType.PROD;
    }

    String envStr = "PROD";
    try {
      // Try different configuration paths to find the environment
      if (conf.getOpenLineageConf() != null) {
        // OpenLineageConf directly contains the fabricType
        FabricType fabricType = conf.getOpenLineageConf().getFabricType();
        if (fabricType != null) {
          return fabricType;
        }
      }

      // Try to get from tags as a fallback
      if (conf.getTags() != null) {
        for (String tag : conf.getTags()) {
          if (tag.startsWith("env=") || tag.startsWith("environment=")) {
            String[] parts = tag.split("=", 2);
            if (parts.length == 2) {
              envStr = parts[1].toUpperCase();
              break;
            }
          }
        }
      }

      return FabricType.valueOf(envStr.toUpperCase());
    } catch (IllegalArgumentException e) {
      log.warn("Invalid environment '{}', using PROD", envStr);
      return FabricType.PROD;
    }
  }

  /** Gets the configured platform alias for delta tables. */
  private String getDeltaPlatformAlias() {
    if (conf != null && conf.getOpenLineageConf() != null) {
      String deltaPlatformAlias = conf.getOpenLineageConf().getHivePlatformAlias();
      if (deltaPlatformAlias != null && !deltaPlatformAlias.isEmpty()) {
        return deltaPlatformAlias;
      }
    }
    return "hive"; // Default to "hive" if no configuration is available
  }

  /** Gets the configured platform for Hive tables */
  private String getHivePlatformName() {
    if (conf != null && conf.getOpenLineageConf() != null) {
      String configuredPlatform = conf.getOpenLineageConf().getHivePlatformAlias();
      if (configuredPlatform != null && !configuredPlatform.isEmpty()) {
        return configuredPlatform;
      }
    }
    return "hive"; // Default to "hive" if no configuration is available
  }

  /**
   * Processes a streaming query progress event, correlates it with other events if possible, and
   * returns the metadata change proposals to emit.
   *
   * @param event the streaming query progress event
   * @param conf the Spark lineage configuration
   * @param schemaMap map of schemas for datasets
   * @return list of MCPs to emit, potentially from multiple correlated events
   */
  public List<MetadataChangeProposalWrapper> processEvent(
      StreamingQueryProgress event,
      SparkLineageConf conf,
      Map<String, MetadataChangeProposalWrapper> schemaMap) {

    // Clean up expired events
    cleanupExpiredEvents();

    String queryId = event.id().toString();
    log.debug("Processing streaming event for query ID: {}", queryId);

    // Parse the event JSON to extract details
    try {
      JsonElement root = new JsonParser().parse(event.json());

      // Extract sink information
      String sinkPath = extractSinkPath(root);
      String operation = detectOperationType(root);
      Set<DatasetUrn> inputDatasets = extractInputDatasets(root, conf);
      Set<DatasetUrn> outputDatasets = new HashSet<>();

      // Special handling for ForeachBatchSink
      boolean isForeachBatchSink = isForeachBatchSink(root);

      // Only try to extract output datasets for normal sinks (not foreachBatch)
      if (!isForeachBatchSink) {
        outputDatasets = extractOutputDatasets(root, conf);
      }

      // Create/update the streaming event data
      StreamingEventData eventData =
          eventDataByQueryId.computeIfAbsent(
              queryId, id -> new StreamingEventData(event, Instant.now()));

      // Update the event data with new information
      eventData.lastUpdated = Instant.now();
      eventData.event = event;
      eventData.operation = operation;
      eventData.inputDatasets.addAll(inputDatasets);
      eventData.isForeachBatchSink = isForeachBatchSink;

      // Set batch/run ID if available
      if (event.batchId() >= 0) {
        eventData.batchId = event.batchId();
      }

      // Record the query name if available - important for correlation
      if (event.name() != null && !event.name().isEmpty()) {
        eventData.queryName = event.name();
      }

      if (!isForeachBatchSink) {
        eventData.outputDatasets.addAll(outputDatasets);
      }

      // If we have a sink path, register it for correlation
      if (sinkPath != null) {
        sinkPathToQueryId.put(sinkPath, queryId);
        eventData.sinkPath = sinkPath;
        log.debug("Registered sink path '{}' for query ID: {}", sinkPath, queryId);
      }

      // Try to correlate with microbatch events based on extracted metadata
      correlateWithMicroBatchEvents(eventData);

      // Check if DataProcessInstance support is enabled in the config
      boolean emitDataProcessInstance =
          conf.getOpenLineageConf() != null
              && conf.getOpenLineageConf().isEmitDataProcessInstance();

      // Generate MCPs based on the event data
      if (isForeachBatchSink) {
        // For ForeachBatchSink, create a data processing job with just inputs
        if (!eventData.inputDatasets.isEmpty()) {
          log.info("Event has ForeachBatchSink with inputs, generating data processing job MCPs");
          if (emitDataProcessInstance) {
            return SparkStreamingEventToDatahub.generateMcpFromStreamingProgressEvent(
                event, conf, schemaMap, eventData.inputDatasets, new HashSet<>(), operation);
          } else {
            return generateMcpsForForeachBatch(
                event, conf, schemaMap, eventData.inputDatasets, operation);
          }
        }
      } else if (!eventData.inputDatasets.isEmpty() && !eventData.outputDatasets.isEmpty()) {
        // For regular sinks, if we have both inputs and outputs, generate unified MCPs
        log.info("Event has both inputs and outputs, generating unified MCPs");
        // Create a unified job with a DataProcessInstance for this batch if enabled
        return SparkStreamingEventToDatahub.generateMcpFromStreamingProgressEvent(
            event,
            conf,
            schemaMap,
            eventData.inputDatasets,
            eventData.outputDatasets,
            eventData.operation);
      }

      // If we don't have a complete picture yet, just generate normal MCPs
      log.debug("Event doesn't have complete lineage yet, generating standard MCPs");
      return SparkStreamingEventToDatahub.generateMcpFromStreamingProgressEvent(
          event, conf, schemaMap);

    } catch (Exception e) {
      log.error("Error processing streaming event", e);
      // Fall back to standard processing
      return SparkStreamingEventToDatahub.generateMcpFromStreamingProgressEvent(
          event, conf, schemaMap);
    }
  }

  /**
   * Attempts to correlate a streaming event with microbatch events based on query ID and other
   * metadata.
   */
  private void correlateWithMicroBatchEvents(StreamingEventData eventData) {
    try {
      // Look for related microbatch events from logs
      for (Map.Entry<String, StreamingEventData> entry : eventDataByQueryId.entrySet()) {
        if (entry.getKey().equals(eventData.event.id().toString())) {
          // This is the same query ID, no need to correlate
          continue;
        }

        StreamingEventData otherEvent = entry.getValue();

        // Try to correlate based on matching query name
        if (eventData.queryName != null && eventData.queryName.equals(otherEvent.queryName)) {
          log.info("Correlated events based on matching query name: {}", eventData.queryName);
          mergeEventData(eventData, otherEvent);
          continue;
        }

        // Try to correlate based on matching sink path
        if (eventData.sinkPath != null && eventData.sinkPath.equals(otherEvent.sinkPath)) {
          log.info("Correlated events based on matching sink path: {}", eventData.sinkPath);
          mergeEventData(eventData, otherEvent);
          continue;
        }

        // Try to correlate based on batch ID
        if (eventData.batchId > 0 && eventData.batchId == otherEvent.batchId) {
          log.info("Correlated events based on matching batch ID: {}", eventData.batchId);
          mergeEventData(eventData, otherEvent);
          continue;
        }
      }
    } catch (Exception e) {
      log.warn("Error while correlating with microbatch events: {}", e.getMessage());
    }
  }

  /** Merges data from another event into this event. */
  private void mergeEventData(StreamingEventData target, StreamingEventData source) {
    // Merge datasets
    target.inputDatasets.addAll(source.inputDatasets);
    target.outputDatasets.addAll(source.outputDatasets);

    // Merge operation info
    if (target.operation == null || target.operation.equals("write")) {
      target.operation = source.operation;
    }

    // Merge flags
    target.hasJoin = target.hasJoin || source.hasJoin;
    target.hasWindow = target.hasWindow || source.hasWindow;
    target.hasAggregate = target.hasAggregate || source.hasAggregate;
    target.hasWatermark = target.hasWatermark || source.hasWatermark;

    // Merge properties - only if target doesn't have them
    if (target.sinkPath == null) target.sinkPath = source.sinkPath;
    if (target.sourcePath == null) target.sourcePath = source.sourcePath;
    if (target.tableName == null) target.tableName = source.tableName;
    if (target.watermark == null) target.watermark = source.watermark;
    if (target.queryName == null) target.queryName = source.queryName;

    // If target doesn't have batchId, copy from source
    if (target.batchId == 0 && source.batchId > 0) {
      target.batchId = source.batchId;
    }

    // Copy log messages if target doesn't have them
    if (target.microBatchStartMessage == null)
      target.microBatchStartMessage = source.microBatchStartMessage;
    if (target.microBatchCommitMessage == null)
      target.microBatchCommitMessage = source.microBatchCommitMessage;
    if (target.deltaSinkWriteMessage == null)
      target.deltaSinkWriteMessage = source.deltaSinkWriteMessage;
    if (target.logicalPlanMessage == null) target.logicalPlanMessage = source.logicalPlanMessage;

    // Add interesting messages
    target.interestingMessages.addAll(source.interestingMessages);
  }

  /** Checks if the sink is a ForeachBatchSink from the query JSON. */
  private boolean isForeachBatchSink(JsonElement root) {
    try {
      if (root.getAsJsonObject().has("sink")) {
        JsonObject sink = root.getAsJsonObject().get("sink").getAsJsonObject();
        if (sink.has("description")) {
          String sinkDescription = sink.get("description").getAsString();
          return "ForeachBatchSink".equals(sinkDescription);
        }
      }
    } catch (Exception e) {
      log.warn("Error checking for ForeachBatchSink", e);
    }
    return false;
  }

  /** Generates MCPs for a ForeachBatchSink event, treating it as a data processing job. */
  private List<MetadataChangeProposalWrapper> generateMcpsForForeachBatch(
      StreamingQueryProgress event,
      SparkLineageConf conf,
      Map<String, MetadataChangeProposalWrapper> schemaMap,
      Set<DatasetUrn> inputDatasets,
      String operation) {

    List<MetadataChangeProposalWrapper> mcps = new ArrayList<>();

    try {
      // Use the same flow name logic as regular streaming events
      String flowName = conf.getOpenLineageConf().getPipelineName();
      if (flowName == null || flowName.trim().isEmpty()) {
        if (conf.getSparkAppContext() != null && conf.getSparkAppContext().getAppName() != null) {
          flowName = conf.getSparkAppContext().getAppName();
        } else {
          flowName = "spark_streaming_foreachbatch";
        }
      }

      // Create a job name that indicates this is a foreachBatch processing job
      String jobName = "foreachBatch_processor_" + event.id().toString();

      // Create a flow URN
      DataFlowUrn flowUrn;
      if (conf.getSparkAppContext() != null && conf.getSparkAppContext().getAppId() != null) {
        String platformInstance = conf.getSparkAppContext().getAppId();
        flowUrn = new DataFlowUrn("spark", flowName, platformInstance);
      } else {
        flowUrn = new DataFlowUrn("spark", flowName, "default");
      }

      // Create a job URN
      DataJobUrn jobUrn = new DataJobUrn(flowUrn, jobName);

      // Create flow info
      DataFlowInfo dataFlowInfo = new DataFlowInfo();
      dataFlowInfo.setName(flowName);
      dataFlowInfo.setDescription("Spark Streaming Flow with foreachBatch: " + flowName);

      // Create job info
      DataJobInfo dataJobInfo = new DataJobInfo();
      dataJobInfo.setName(jobName);
      dataJobInfo.setType(DataJobInfo.Type.create("SPARK_FOREACHBATCH"));
      dataJobInfo.setDescription("Spark Streaming ForeachBatch Processor Job: " + jobName);

      // Add custom properties with useful information
      StringMap jobCustomProperties = new StringMap();
      jobCustomProperties.put("batchId", Long.toString(event.batchId()));
      jobCustomProperties.put("inputRowsPerSecond", Double.toString(event.inputRowsPerSecond()));
      jobCustomProperties.put(
          "processedRowsPerSecond", Double.toString(event.processedRowsPerSecond()));
      jobCustomProperties.put("numInputRows", Long.toString(event.numInputRows()));
      jobCustomProperties.put("queryId", event.id().toString());
      jobCustomProperties.put("operationType", operation);
      dataJobInfo.setCustomProperties(jobCustomProperties);

      // Create job input/output aspect
      DataJobInputOutput dataJobInputOutput = new DataJobInputOutput();

      // Add input datasets
      DatasetUrnArray inputDatasetUrnArray = new DatasetUrnArray();
      for (DatasetUrn inputUrn : inputDatasets) {
        inputDatasetUrnArray.add(inputUrn);

        // Add dataset MCPs if materialization is enabled
        if (conf.getOpenLineageConf().isMaterializeDataset()) {
          mcps.add(SparkStreamingEventToDatahub.generateDatasetMcp(inputUrn));
          if (conf.getOpenLineageConf().isIncludeSchemaMetadata()
              && schemaMap.containsKey(inputUrn.toString())) {
            mcps.add(schemaMap.get(inputUrn.toString()));
          }
        }
      }
      dataJobInputOutput.setInputDatasets(inputDatasetUrnArray);

      // No output datasets (since foreachBatch is custom processing)
      dataJobInputOutput.setOutputDatasets(new DatasetUrnArray());

      // Add flow and job MCPs
      mcps.add(
          MetadataChangeProposalWrapper.create(
              b -> b.entityType("dataFlow").entityUrn(flowUrn).upsert().aspect(dataFlowInfo)));

      mcps.add(
          MetadataChangeProposalWrapper.create(
              b -> b.entityType("dataJob").entityUrn(jobUrn).upsert().aspect(dataJobInfo)));

      mcps.add(
          MetadataChangeProposalWrapper.create(
              b -> b.entityType("dataJob").entityUrn(jobUrn).upsert().aspect(dataJobInputOutput)));

      log.info("Generated {} MCPs for foreachBatch processing job", mcps.size());

    } catch (Exception e) {
      log.error("Error generating MCPs for foreachBatch", e);
    }

    return mcps;
  }

  /** Extracts the sink path from a streaming query progress event JSON. */
  private String extractSinkPath(JsonElement root) {
    try {
      if (root.getAsJsonObject().has("sink")) {
        JsonObject sink = root.getAsJsonObject().get("sink").getAsJsonObject();
        if (sink.has("description")) {
          String sinkDescription = sink.get("description").getAsString();
          return StringUtils.substringBetween(sinkDescription, "[", "]");
        }
      }
    } catch (Exception e) {
      log.warn("Failed to extract sink path", e);
    }
    return null;
  }

  /** Extracts input datasets from a streaming query progress event JSON. */
  private Set<DatasetUrn> extractInputDatasets(JsonElement root, SparkLineageConf conf) {
    Set<DatasetUrn> datasets = new HashSet<>();
    try {
      if (root.getAsJsonObject().has("sources")) {
        JsonElement sources = root.getAsJsonObject().get("sources");
        if (sources.isJsonArray()) {
          for (JsonElement source : sources.getAsJsonArray()) {
            if (source.isJsonObject() && source.getAsJsonObject().has("description")) {
              String description = source.getAsJsonObject().get("description").getAsString();
              Optional<DatasetUrn> urn =
                  SparkStreamingEventToDatahub.generateUrnFromStreamingDescription(
                      description, conf);
              urn.ifPresent(datasets::add);
            }
          }
        }
      }
    } catch (Exception e) {
      log.warn("Failed to extract input datasets", e);
    }
    return datasets;
  }

  /** Extracts output datasets from a streaming query progress event JSON. */
  private Set<DatasetUrn> extractOutputDatasets(JsonElement root, SparkLineageConf conf) {
    Set<DatasetUrn> datasets = new HashSet<>();
    try {
      if (root.getAsJsonObject().has("sink")) {
        JsonObject sink = root.getAsJsonObject().get("sink").getAsJsonObject();
        if (sink.has("description")) {
          String sinkDescription = sink.get("description").getAsString();
          Optional<DatasetUrn> urn =
              SparkStreamingEventToDatahub.generateUrnFromStreamingDescription(
                  sinkDescription, conf);
          urn.ifPresent(datasets::add);
        }
      }
    } catch (Exception e) {
      log.warn("Failed to extract output datasets", e);
    }
    return datasets;
  }

  /** Detects the operation type from the streaming query JSON. */
  private String detectOperationType(JsonElement root) {
    try {
      if (root.getAsJsonObject().has("sink")) {
        JsonObject sink = root.getAsJsonObject().get("sink").getAsJsonObject();
        if (sink.has("description")) {
          String desc = sink.get("description").getAsString().toLowerCase();
          String json = root.toString().toLowerCase();

          if (desc.contains("merge") || json.contains("mergeinto") || json.contains("merge into")) {
            return "merge_into";
          } else if (desc.contains("delta") && desc.contains("sink")) {
            return "delta_write";
          } else if (desc.contains("append")) {
            return "append";
          } else if (desc.contains("overwrite")) {
            return "overwrite";
          } else if (desc.contains("update")) {
            return "update";
          } else if (desc.contains("delete")) {
            return "delete";
          }
        }
      }
      return "write";
    } catch (Exception e) {
      log.warn("Error detecting operation type", e);
      return "stream";
    }
  }

  /** Removes expired event data to prevent memory leaks. */
  private void cleanupExpiredEvents() {
    Instant now = Instant.now();
    List<String> expiredQueryIds = new ArrayList<>();

    // Find expired events
    for (Map.Entry<String, StreamingEventData> entry : eventDataByQueryId.entrySet()) {
      if (entry.getValue().lastUpdated.plus(Duration.ofMillis(expirationMs)).isBefore(now)) {
        expiredQueryIds.add(entry.getKey());
        log.debug("Expiring event data for query ID: {}", entry.getKey());
      }
    }

    // Remove expired events
    for (String queryId : expiredQueryIds) {
      StreamingEventData data = eventDataByQueryId.remove(queryId);
      if (data.sinkPath != null) {
        sinkPathToQueryId.remove(data.sinkPath);
      }
    }

    if (!expiredQueryIds.isEmpty()) {
      log.info("Cleaned up {} expired streaming events", expiredQueryIds.size());
    }
  }

  /** Data class to hold information about a streaming event. */
  private static class StreamingEventData {
    private StreamingQueryProgress event;
    private Instant lastUpdated;
    private String operation;
    private String sinkPath;
    private String microBatchStartMessage;
    private String microBatchCommitMessage;
    private String deltaSinkWriteMessage;
    private String tableName;
    private String logicalPlanMessage;
    private String progressReportMessage;
    private String progressJson;
    private boolean hasJoin;
    private String joinType;
    private boolean hasWindow;
    private boolean hasAggregate;
    private String sourceInfo;
    private String sourcePath;
    private String sinkTable;
    private final Set<DatasetUrn> inputDatasets = new HashSet<>();
    private final Set<DatasetUrn> outputDatasets = new HashSet<>();
    private final List<String> interestingMessages = new ArrayList<>();
    private boolean isForeachBatchSink;
    private Map<String, String> jobMetadata;

    // Progress report metrics
    private long batchId;
    private String timestamp;
    private long numInputRows;
    private double inputRowsPerSecond;
    private double processedRowsPerSecond;
    private boolean hasWatermark;
    private String watermark;

    // Additional metadata
    private String queryName;

    public StreamingEventData(StreamingQueryProgress event, Instant lastUpdated) {
      this.event = event;
      this.lastUpdated = lastUpdated;
    }
  }

  /**
   * Records a MicroBatchExecution start event.
   *
   * @param queryId the query ID from the log
   * @param logMessage the original log message
   */
  public void recordMicroBatchStart(String queryId, String logMessage) {
    log.info("Recording microbatch start for query ID: {}", queryId);

    // Ensure we have an event data object for this query
    StreamingEventData eventData =
        eventDataByQueryId.computeIfAbsent(
            queryId, id -> new StreamingEventData(null, Instant.now()));

    // Update the event data
    eventData.lastUpdated = Instant.now();
    eventData.microBatchStartMessage = logMessage;

    log.debug("Stored microbatch start event for query ID: {}", queryId);
  }

  /**
   * Records a MicroBatchExecution commit event.
   *
   * @param queryId the query ID from the log
   * @param metadata additional metadata extracted from the log
   * @param logMessage the original log message
   */
  public void recordMicroBatchCommit(
      String queryId, Map<String, String> metadata, String logMessage) {
    log.info("Recording microbatch commit for query ID: {}", queryId);

    // Ensure we have an event data object for this query
    StreamingEventData eventData =
        eventDataByQueryId.computeIfAbsent(
            queryId, id -> new StreamingEventData(null, Instant.now()));

    // Update the event data
    eventData.lastUpdated = Instant.now();
    eventData.microBatchCommitMessage = logMessage;

    // Store operation type if available
    if (metadata.containsKey("operation")) {
      eventData.operation = metadata.get("operation");
    }

    // Store sink path if available
    if (metadata.containsKey("sinkPath")) {
      String sinkPath = metadata.get("sinkPath");
      eventData.sinkPath = sinkPath;
      sinkPathToQueryId.put(sinkPath, queryId);
    }

    log.debug("Stored microbatch commit event for query ID: {}", queryId);
  }

  /**
   * Records a delta sink write event.
   *
   * @param queryId the query ID from the log
   * @param metadata additional metadata extracted from the log
   * @param logMessage the original log message
   */
  public void recordDeltaSinkWrite(
      String queryId, Map<String, String> metadata, String logMessage) {
    log.info("Recording delta sink write for query ID: {}", queryId);

    // Ensure we have an event data object for this query
    StreamingEventData eventData =
        eventDataByQueryId.computeIfAbsent(
            queryId, id -> new StreamingEventData(null, Instant.now()));

    // Update the event data
    eventData.lastUpdated = Instant.now();
    eventData.deltaSinkWriteMessage = logMessage;

    // Set operation type if not already set
    if (eventData.operation == null || eventData.operation.equals("write")) {
      eventData.operation = "delta_write";
    }

    // Store sink path if available
    if (metadata.containsKey("sinkPath")) {
      String sinkPath = metadata.get("sinkPath");
      eventData.sinkPath = sinkPath;
      sinkPathToQueryId.put(sinkPath, queryId);
    }

    // Store table name if available
    if (metadata.containsKey("tableName")) {
      eventData.tableName = metadata.get("tableName");
    }

    log.debug("Stored delta sink write event for query ID: {}", queryId);
  }

  /**
   * Records a logical plan event.
   *
   * @param queryId the query ID
   * @param metadata additional metadata extracted from the log
   * @param logMessage the original log message
   */
  public void recordLogicalPlan(String queryId, Map<String, String> metadata, String logMessage) {
    log.info("Recording logical plan for query ID: {}", queryId);

    // Ensure we have an event data object for this query
    StreamingEventData eventData =
        eventDataByQueryId.computeIfAbsent(
            queryId, id -> new StreamingEventData(null, Instant.now()));

    // Update the event data
    eventData.lastUpdated = Instant.now();
    eventData.logicalPlanMessage = logMessage;

    // Store operation type if available
    if (metadata.containsKey("operation")) {
      eventData.operation = metadata.get("operation");
    }

    // Store sink path if available
    if (metadata.containsKey("sinkPath")) {
      String sinkPath = metadata.get("sinkPath");
      eventData.sinkPath = sinkPath;
      sinkPathToQueryId.put(sinkPath, queryId);
    }

    // Store sink table if available
    if (metadata.containsKey("sinkTable")) {
      eventData.sinkTable = metadata.get("sinkTable");
    }

    // Store source info if available
    if (metadata.containsKey("sourceInfo")) {
      eventData.sourceInfo = metadata.get("sourceInfo");
    }

    // Store source path if available
    if (metadata.containsKey("sourcePath")) {
      eventData.sourcePath = metadata.get("sourcePath");
    }

    // Extract logical plan transformations
    extractTransformationsFromLogicalPlan(eventData, metadata);

    log.debug("Stored logical plan for query ID: {}", queryId);

    // Try to generate inputs and outputs based on the logical plan
    tryExtractInputOutputDatasetsFromLogicalPlan(eventData, metadata);
  }

  /**
   * Records a progress report event.
   *
   * @param queryId the query ID from the log
   * @param metadata metadata about the progress report
   * @param logMessage the original log message
   */
  public void recordProgressReport(
      String queryId, Map<String, String> metadata, String logMessage) {
    log.info("Recording progress report for query ID: {}", queryId);

    // Ensure we have an event data object for this query
    StreamingEventData eventData =
        eventDataByQueryId.computeIfAbsent(
            queryId, id -> new StreamingEventData(null, Instant.now()));

    // Update the event data
    eventData.lastUpdated = Instant.now();
    eventData.progressReportMessage = logMessage;

    // Extract metrics if available
    if (metadata.containsKey("batchId")) {
      try {
        eventData.batchId = Long.parseLong(metadata.get("batchId"));
      } catch (NumberFormatException e) {
        log.warn("Invalid batch ID: {}", metadata.get("batchId"));
      }
    }

    if (metadata.containsKey("numInputRows")) {
      try {
        eventData.numInputRows = Long.parseLong(metadata.get("numInputRows"));
      } catch (NumberFormatException e) {
        log.warn("Invalid numInputRows: {}", metadata.get("numInputRows"));
      }
    }

    if (metadata.containsKey("inputRowsPerSecond")) {
      try {
        eventData.inputRowsPerSecond = Double.parseDouble(metadata.get("inputRowsPerSecond"));
      } catch (NumberFormatException e) {
        log.warn("Invalid inputRowsPerSecond: {}", metadata.get("inputRowsPerSecond"));
      }
    }

    if (metadata.containsKey("processedRowsPerSecond")) {
      try {
        eventData.processedRowsPerSecond =
            Double.parseDouble(metadata.get("processedRowsPerSecond"));
      } catch (NumberFormatException e) {
        log.warn("Invalid processedRowsPerSecond: {}", metadata.get("processedRowsPerSecond"));
      }
    }

    log.debug("Stored progress report for query ID: {}", queryId);
  }

  /** Enhances event data with metrics from the progress report. */
  private void enhanceWithProgressMetrics(StreamingEventData eventData, String progressJson) {
    try {
      JsonElement root = new JsonParser().parse(progressJson);

      // Extract batch ID
      if (root.getAsJsonObject().has("batchId")) {
        eventData.batchId = root.getAsJsonObject().get("batchId").getAsLong();
      }

      // Extract timestamp
      if (root.getAsJsonObject().has("timestamp")) {
        String timestamp = root.getAsJsonObject().get("timestamp").getAsString();
        eventData.timestamp = timestamp;
      }

      // Extract performance metrics
      if (root.getAsJsonObject().has("numInputRows")) {
        eventData.numInputRows = root.getAsJsonObject().get("numInputRows").getAsLong();
      }

      if (root.getAsJsonObject().has("inputRowsPerSecond")) {
        eventData.inputRowsPerSecond =
            root.getAsJsonObject().get("inputRowsPerSecond").getAsDouble();
      }

      if (root.getAsJsonObject().has("processedRowsPerSecond")) {
        eventData.processedRowsPerSecond =
            root.getAsJsonObject().get("processedRowsPerSecond").getAsDouble();
      }

      // Extract state operation metrics if available (important for determining operations)
      if (root.getAsJsonObject().has("stateOperators")
          && root.getAsJsonObject().get("stateOperators").isJsonArray()) {
        for (JsonElement operator : root.getAsJsonObject().get("stateOperators").getAsJsonArray()) {
          if (operator.isJsonObject()) {
            String operatorType =
                operator.getAsJsonObject().has("operatorName")
                    ? operator.getAsJsonObject().get("operatorName").getAsString()
                    : null;

            if (operatorType != null) {
              // Look for specific operators that indicate transformations
              if (operatorType.contains("Window") || operatorType.contains("window")) {
                eventData.hasWindow = true;
              } else if (operatorType.contains("Aggregate") || operatorType.contains("aggregate")) {
                eventData.hasAggregate = true;
              } else if (operatorType.contains("Join") || operatorType.contains("join")) {
                eventData.hasJoin = true;
              }

              log.debug("Found state operator: {}", operatorType);
            }
          }
        }
      }

      // Check for watermark which indicates time-based processing
      if (root.getAsJsonObject().has("eventTime")
          && root.getAsJsonObject().get("eventTime").getAsJsonObject().has("watermark")) {
        eventData.hasWatermark = true;
        eventData.watermark =
            root.getAsJsonObject()
                .get("eventTime")
                .getAsJsonObject()
                .get("watermark")
                .getAsString();
      }

      log.info("Enhanced event data with metrics from progress report");
    } catch (Exception e) {
      log.warn("Error enhancing event data with progress metrics: {}", e.getMessage(), e);
    }
  }

  /**
   * Records an interesting message event.
   *
   * @param queryId the query ID from the log
   * @param metadata metadata about the message
   * @param logMessage the original log message
   */
  public void recordInterestingMessage(
      String queryId, Map<String, String> metadata, String logMessage) {
    log.info("Recording interesting message for query ID: {}", queryId);

    // Ensure we have an event data object for this query
    StreamingEventData eventData =
        eventDataByQueryId.computeIfAbsent(
            queryId, id -> new StreamingEventData(null, Instant.now()));

    // Update the event data
    eventData.lastUpdated = Instant.now();
    eventData.interestingMessages.add(logMessage);

    // Check for specific patterns
    if (logMessage.contains("join")) {
      eventData.hasJoin = true;
      if (logMessage.contains("inner join")) {
        eventData.joinType = "inner";
      } else if (logMessage.contains("left join") || logMessage.contains("left outer join")) {
        eventData.joinType = "left";
      } else if (logMessage.contains("right join") || logMessage.contains("right outer join")) {
        eventData.joinType = "right";
      } else if (logMessage.contains("full join") || logMessage.contains("full outer join")) {
        eventData.joinType = "full";
      }
    }

    if (logMessage.contains("window") || logMessage.contains("Window")) {
      eventData.hasWindow = true;
    }

    if (logMessage.contains("aggregate") || logMessage.contains("Aggregate")) {
      eventData.hasAggregate = true;
    }

    if (logMessage.contains("ForeachBatchSink")) {
      eventData.isForeachBatchSink = true;
    }

    // Check for watermark information
    if (logMessage.contains("watermark") || logMessage.contains("Watermark")) {
      eventData.hasWatermark = true;
      // Try to extract watermark details
      String watermarkPrefix = "watermark ";
      int watermarkStart = logMessage.indexOf(watermarkPrefix);
      if (watermarkStart >= 0) {
        int watermarkEnd = logMessage.indexOf("\n", watermarkStart);
        if (watermarkEnd < 0) {
          watermarkEnd = logMessage.length();
        }
        eventData.watermark =
            logMessage.substring(watermarkStart + watermarkPrefix.length(), watermarkEnd).trim();
      }
    }

    log.debug("Stored interesting message for query ID: {}", queryId);
  }

  /** Extract transformations from the logical plan. */
  private void extractTransformationsFromLogicalPlan(
      StreamingEventData eventData, Map<String, String> metadata) {
    // Extract join information
    if (metadata.containsKey("hasJoin") && metadata.get("hasJoin").equals("true")) {
      eventData.hasJoin = true;
      if (metadata.containsKey("joinType")) {
        eventData.joinType = metadata.get("joinType");
      }
    }

    // Extract window information
    if (metadata.containsKey("hasWindow") && metadata.get("hasWindow").equals("true")) {
      eventData.hasWindow = true;
    }

    // Extract aggregate information
    if (metadata.containsKey("hasAggregate") && metadata.get("hasAggregate").equals("true")) {
      eventData.hasAggregate = true;
    }
  }

  /** Try to extract input and output datasets from the logical plan. */
  private void tryExtractInputOutputDatasetsFromLogicalPlan(
      StreamingEventData eventData, Map<String, String> metadata) {
    try {
      // Create a default SparkLineageConf
      SparkLineageConf defaultConf =
          SparkLineageConf.builder()
              .openLineageConf(DatahubOpenlineageConfig.builder().build())
              .build();

      // Use logical plan to extract input datasets
      if (metadata.containsKey("sourceType") && metadata.containsKey("sourcePath")) {
        String sourceType = metadata.get("sourceType");
        String sourcePath = metadata.get("sourcePath");

        if (sourceType.equals("DeltaSource") && !sourcePath.isEmpty()) {
          // Create a dataset URN for the source
          Optional<DatasetUrn> sourceUrn =
              SparkStreamingEventToDatahub.generateUrnFromStreamingDescription(
                  "DeltaSource[" + sourcePath + "]", defaultConf);

          if (sourceUrn.isPresent()) {
            eventData.inputDatasets.add(sourceUrn.get());
            log.info("Added input dataset from logical plan: {}", sourceUrn.get());
          }
        }
      }

      // Use logical plan to extract output datasets
      if (metadata.containsKey("sinkType") && metadata.containsKey("sinkPath")) {
        String sinkType = metadata.get("sinkType");
        String sinkPath = metadata.get("sinkPath");

        if (sinkType.equals("DeltaSink") && !sinkPath.isEmpty()) {
          // Create a dataset URN for the sink
          Optional<DatasetUrn> sinkUrn =
              SparkStreamingEventToDatahub.generateUrnFromStreamingDescription(
                  "DeltaSink[" + sinkPath + "]", defaultConf);

          if (sinkUrn.isPresent()) {
            eventData.outputDatasets.add(sinkUrn.get());
            log.info("Added output dataset from logical plan: {}", sinkUrn.get());
          }
        }
      }
    } catch (Exception e) {
      log.warn("Error extracting datasets from logical plan: {}", e.getMessage(), e);
    }
  }

  /** Extract source and sink information from the progress JSON. */
  private void extractInfoFromProgressJson(StreamingEventData eventData) {
    try {
      // Create a default SparkLineageConf
      SparkLineageConf defaultConf =
          SparkLineageConf.builder()
              .openLineageConf(DatahubOpenlineageConfig.builder().build())
              .build();

      String progressJson = eventData.progressJson;

      // Extract source information
      if (progressJson.contains("\"sources\"")) {
        int sourcesStart = progressJson.indexOf("\"sources\"");
        if (sourcesStart > 0) {
          int sourcesArrayStart = progressJson.indexOf("[", sourcesStart);
          int sourcesArrayEnd = findMatchingCloseBracket(progressJson, sourcesArrayStart);

          if (sourcesArrayStart > 0 && sourcesArrayEnd > sourcesArrayStart) {
            String sourcesArray = progressJson.substring(sourcesArrayStart, sourcesArrayEnd + 1);

            // Extract source descriptions
            int descStart = 0;
            while ((descStart = sourcesArray.indexOf("\"description\"", descStart)) > 0) {
              int valueStart = sourcesArray.indexOf(":", descStart) + 1;
              int valueEnd = sourcesArray.indexOf(",", valueStart);
              if (valueEnd < 0) valueEnd = sourcesArray.indexOf("}", valueStart);

              if (valueStart > 0 && valueEnd > valueStart) {
                String description = sourcesArray.substring(valueStart, valueEnd).trim();
                // Remove quotes
                description = description.replaceAll("^\"|\"$", "");

                if (!description.isEmpty()) {
                  // Try to create a dataset Urn from the source description
                  Optional<DatasetUrn> sourceUrn =
                      SparkStreamingEventToDatahub.generateUrnFromStreamingDescription(
                          description, defaultConf);

                  if (sourceUrn.isPresent()) {
                    eventData.inputDatasets.add(sourceUrn.get());
                    log.info("Added input dataset from progress JSON: {}", sourceUrn.get());
                  }
                }
              }

              descStart = valueEnd;
            }
          }
        }
      }

      // Extract sink information
      if (progressJson.contains("\"sink\"")) {
        int sinkStart = progressJson.indexOf("\"sink\"");
        if (sinkStart > 0) {
          int sinkObjStart = progressJson.indexOf("{", sinkStart);
          int sinkObjEnd = findMatchingCloseBracket(progressJson, sinkObjStart);

          if (sinkObjStart > 0 && sinkObjEnd > sinkObjStart) {
            String sinkObj = progressJson.substring(sinkObjStart, sinkObjEnd + 1);

            // Extract sink description
            int descStart = sinkObj.indexOf("\"description\"");
            if (descStart > 0) {
              int valueStart = sinkObj.indexOf(":", descStart) + 1;
              int valueEnd = sinkObj.indexOf(",", valueStart);
              if (valueEnd < 0) valueEnd = sinkObj.indexOf("}", valueStart);

              if (valueStart > 0 && valueEnd > valueStart) {
                String description = sinkObj.substring(valueStart, valueEnd).trim();
                // Remove quotes
                description = description.replaceAll("^\"|\"$", "");

                if (!description.isEmpty() && !description.equals("ForeachBatchSink")) {
                  // Try to create a dataset Urn from the sink description
                  Optional<DatasetUrn> sinkUrn =
                      SparkStreamingEventToDatahub.generateUrnFromStreamingDescription(
                          description, defaultConf);

                  if (sinkUrn.isPresent()) {
                    eventData.outputDatasets.add(sinkUrn.get());
                    log.info("Added output dataset from progress JSON: {}", sinkUrn.get());
                  }
                }
              }
            }
          }
        }
      }
    } catch (Exception e) {
      log.warn("Error extracting info from progress JSON: {}", e.getMessage(), e);
    }
  }

  /** Find the matching close bracket for a given open bracket position. */
  private int findMatchingCloseBracket(String text, int openPos) {
    if (openPos < 0) return -1;

    char open = text.charAt(openPos);
    char close;

    if (open == '{') close = '}';
    else if (open == '[') close = ']';
    else if (open == '(') close = ')';
    else return -1;

    int count = 1;
    for (int i = openPos + 1; i < text.length(); i++) {
      char c = text.charAt(i);
      if (c == open) count++;
      else if (c == close) {
        count--;
        if (count == 0) return i;
      }
    }

    return -1;
  }

  /** Query information tracking class. */
  private static class QueryInfo {
    final String queryId;
    String queryName;
    long lastUpdateTime;
    private long currentBatchId = -1;
    long batchStartTime;
    long batchEndTime;
    BatchState batchState = BatchState.UNKNOWN;
    String operation = "unknown";
    final Set<DatasetUrn> inputDatasets = new HashSet<>();
    final Set<DatasetUrn> outputDatasets = new HashSet<>();
    final Map<String, String> metrics = new HashMap<>();

    public QueryInfo(String queryId) {
      this.queryId = queryId;
      this.lastUpdateTime = System.currentTimeMillis();
    }

    public long getCurrentBatchId() {
      return currentBatchId;
    }

    public void setCurrentBatchId(long batchId) {
      if (batchId >= 0) {
        this.currentBatchId = batchId;
      }
    }

    public boolean hasBatchInfo() {
      return currentBatchId >= 0;
    }

    public Set<DatasetUrn> getInputDatasets() {
      return new HashSet<>(inputDatasets);
    }

    public Set<DatasetUrn> getOutputDatasets() {
      return new HashSet<>(outputDatasets);
    }

    public String getOperation() {
      return operation;
    }
  }

  /** Batch state enum. */
  private enum BatchState {
    UNKNOWN,
    STARTED,
    PROCESSING,
    COMMITTED,
    FAILED
  }

  /**
   * Records output table information for a streaming query.
   *
   * @param queryId the query ID
   * @param metadata table metadata including catalogName, databaseName, tableName
   */
  public void recordOutputTable(String queryId, Map<String, String> metadata) {
    log.info("Recording output table for query ID: {}", queryId);

    // Get the event data for this query
    StreamingEventData eventData =
        eventDataByQueryId.computeIfAbsent(
            queryId, id -> new StreamingEventData(null, Instant.now()));

    // Update the event data
    eventData.lastUpdated = Instant.now();

    try {
      // Extract table info
      String catalog = metadata.get("catalogName");
      String database = metadata.get("databaseName");
      String table = metadata.get("tableName");

      if (catalog != null && database != null && table != null) {
        // Create dataset name according to DataHub conventions
        String datasetName;
        if ("jonnydq".equalsIgnoreCase(catalog)) {
          // For jonnydq catalog, include the catalog in the dataset name
          datasetName = String.format("%s.%s.%s", catalog, database, table);
          log.info("Using jonnydq-specific dataset name format: {}", datasetName);
        } else {
          // For other catalogs, use standard database.table format
          datasetName = String.format("%s.%s", database, table);
        }
        eventData.sinkTable = datasetName;

        // Get platform name and fabric type from configuration
        String platformName = getDeltaPlatformAlias();
        FabricType fabricType = getFabricType();

        // Create dataset Urn
        DatasetUrn outputUrn =
            new DatasetUrn(new DataPlatformUrn(platformName), datasetName, fabricType);

        // Add to outputs
        eventData.outputDatasets.add(outputUrn);

        // Store location if available
        if (metadata.containsKey("location")) {
          eventData.sinkPath = metadata.get("location");
          sinkPathToQueryId.put(eventData.sinkPath, queryId);
        }

        log.info(
            "Added output table {}:{}:{} to query {}",
            platformName,
            datasetName,
            fabricType,
            queryId);
      }
    } catch (Exception e) {
      log.error("Error creating output dataset Urn: {}", e.getMessage(), e);
    }
  }

  /**
   * Records catalog table information for a streaming query.
   *
   * @param queryId the query ID
   * @param metadata catalog table metadata
   */
  public void recordCatalogTable(String queryId, Map<String, String> metadata) {
    log.info("Recording catalog table for query ID: {}", queryId);

    try {
      // Validate required fields
      String catalog = metadata.get("catalogName");
      String database = metadata.get("databaseName");
      String table = metadata.get("tableName");

      if (catalog == null || database == null || table == null) {
        log.warn(
            "Missing required catalog metadata fields - catalog: {}, database: {}, table: {}",
            catalog,
            database,
            table);
        return;
      }

      log.info(
          "Catalog table details - catalog: {}, database: {}, table: {}", catalog, database, table);

      // Store the catalog table metadata
      catalogTableByQueryId.put(queryId, new HashMap<>(metadata));

      // If we have an existing StreamingEventData, update it directly with this information
      StreamingEventData eventData = eventDataByQueryId.get(queryId);
      if (eventData != null) {
        try {
          // Create dataset name according to DataHub conventions
          String datasetName;
          if ("jonnydq".equalsIgnoreCase(catalog)) {
            // For jonnydq catalog, include the catalog in the dataset name
            datasetName = String.format("%s.%s.%s", catalog, database, table);
            log.info("Using jonnydq-specific dataset name format: {}", datasetName);
          } else {
            // For other catalogs, use standard database.table format
            datasetName = String.format("%s.%s", database, table);
          }
          eventData.sinkTable = datasetName;

          // Determine the platform using our helper method
          String platform = getHivePlatformName();
          log.info("Using platform name: {} for catalog table", platform);

          // Create dataset Urn
          DatasetUrn outputUrn =
              new DatasetUrn(new DataPlatformUrn(platform), datasetName, getFabricType());

          // Add to outputs
          eventData.outputDatasets.add(outputUrn);

          log.info("Added output dataset directly to StreamingEventData: {}", outputUrn);

          // Store location if available
          if (metadata.containsKey("location")) {
            eventData.sinkPath = metadata.get("location");
            sinkPathToQueryId.put(eventData.sinkPath, queryId);
          }
        } catch (Exception e) {
          log.error(
              "Error adding catalog table info to existing event data: {}", e.getMessage(), e);
        }
      }

      // Check if we already have progress information for this query
      processPendingStreamingEvents(queryId);
    } catch (Exception e) {
      log.error("Error processing catalog table information: {}", e.getMessage(), e);
    }
  }

  /**
   * Records ForeachBatchSink information for a streaming query.
   *
   * @param queryId the query ID
   * @param progressReport the JSON progress report
   */
  public void recordForeachBatchSink(String queryId, String progressReport) {
    log.info("Recording ForeachBatchSink for query ID: {}", queryId);

    // Store the progress report
    foreachBatchSinkByQueryId.put(queryId, progressReport);

    // Check if we already have catalog table information for this query
    processPendingStreamingEvents(queryId);
  }

  /**
   * Processes any pending streaming events if we now have enough information.
   *
   * @param queryId the query ID
   */
  private void processPendingStreamingEvents(String queryId) {
    // We'll process if we have any additional information beyond the basic event
    boolean shouldProcess = false;

    // We consider catalog table information to be sufficient by itself
    if (catalogTableByQueryId.containsKey(queryId)) {
      shouldProcess = true;
      log.info("Processing streaming events with catalog table information");
    }

    // We also consider ForeachBatchSink information to be sufficient by itself if we have event
    // data
    if (foreachBatchSinkByQueryId.containsKey(queryId) && eventDataByQueryId.containsKey(queryId)) {
      shouldProcess = true;
      log.info("Processing streaming events with ForeachBatchSink information");
    }

    // We need event data to process
    if (!eventDataByQueryId.containsKey(queryId)) {
      log.info("No event data available for query {}, skipping processing", queryId);
      return;
    }

    if (!shouldProcess) {
      log.info("Not enough information to process events for query {}", queryId);
      return;
    }

    try {
      // Get the streaming event data
      StreamingEventData eventData = eventDataByQueryId.get(queryId);

      // If we have catalog table information, use it
      if (catalogTableByQueryId.containsKey(queryId)) {
        Map<String, String> catalogMetadata = catalogTableByQueryId.get(queryId);
        String catalogName = catalogMetadata.get("catalogName");
        String databaseName = catalogMetadata.get("databaseName");
        String tableName = catalogMetadata.get("tableName");

        log.info("Processing with catalog table: {}.{}.{}", catalogName, databaseName, tableName);

        // Create output dataset Urn for the Hive table
        String datasetName;
        if ("jonnydq".equalsIgnoreCase(catalogName)) {
          // For jonnydq catalog, include the catalog in the dataset name
          datasetName = String.format("%s.%s.%s", catalogName, databaseName, tableName);
          log.info("Using jonnydq-specific dataset name format: {}", datasetName);
        } else {
          // For other catalogs, use standard database.table format
          datasetName = String.format("%s.%s", databaseName, tableName);
        }

        // Determine the platform using our helper method
        String platform = getHivePlatformName();
        log.info("Using platform name: {} for catalog table", platform);

        // Create the output dataset Urn
        DatasetUrn outputDatasetUrn =
            new DatasetUrn(new DataPlatformUrn(platform), datasetName, getFabricType());

        log.info("Created output dataset Urn for table: {}", outputDatasetUrn);

        // Add it to the output datasets for this streaming event
        eventData.outputDatasets.add(outputDatasetUrn);

        // Update the event data
        eventData.lastUpdated = Instant.now();
        eventData.sinkTable = datasetName;

        // If we have progress information, parse it to get the operation type
        String operation = "write";
        if (foreachBatchSinkByQueryId.containsKey(queryId)) {
          String progressReport = foreachBatchSinkByQueryId.get(queryId);
          operation = extractOperationFromProgressReport(progressReport);
        }

        // Create custom metadata for the job
        if (eventData.jobMetadata == null) {
          eventData.jobMetadata = new HashMap<>();
        }
        eventData.jobMetadata.put("operation", operation);
        eventData.jobMetadata.put("outputTable", datasetName);
        eventData.jobMetadata.put("catalogName", catalogName);
        eventData.operation = operation;

        log.info(
            "Updated streaming event data with output table information and operation: {}",
            operation);
      }
    } catch (Exception e) {
      log.error("Error processing streaming event with catalog table: {}", e.getMessage(), e);
    }
  }

  /**
   * Extracts the operation type from a progress report JSON.
   *
   * @param progressReport the JSON progress report
   * @return the operation type
   */
  private String extractOperationFromProgressReport(String progressReport) {
    try {
      if (progressReport == null || progressReport.isEmpty()) {
        return "write";
      }

      // Try to parse the JSON
      JsonElement root = new JsonParser().parse(progressReport);

      // Look for operation hints in the JSON
      if (root.isJsonObject()) {
        JsonObject rootObj = root.getAsJsonObject();

        // Check for "stateOperators" which might indicate the operation type
        if (rootObj.has("stateOperators") && rootObj.get("stateOperators").isJsonArray()) {
          for (JsonElement operator : rootObj.get("stateOperators").getAsJsonArray()) {
            if (operator.isJsonObject()) {
              JsonObject opObj = operator.getAsJsonObject();

              // Look for customMetrics that might indicate a merge
              if (opObj.has("customMetrics") && opObj.get("customMetrics").isJsonObject()) {
                String metricsStr = opObj.get("customMetrics").toString().toLowerCase();
                if (metricsStr.contains("merge") || metricsStr.contains("update")) {
                  return "merge_into";
                }
              }
            }
          }
        }

        // If we have a Delta sink, it's likely a Delta operation
        String jsonStr = rootObj.toString().toLowerCase();
        if (jsonStr.contains("delta") && jsonStr.contains("sink")) {
          if (jsonStr.contains("merge") || jsonStr.contains("update")) {
            return "merge_into";
          }
          return "delta_write";
        }
      }

      // Default to "write"
      return "write";
    } catch (Exception e) {
      log.warn("Error extracting operation from progress report: {}", e.getMessage());
      return "write";
    }
  }

  /**
   * Generates lineage from accumulated streaming event data
   *
   * @param queryId the query ID
   * @return list of MCPs
   */
  public List<MetadataChangeProposalWrapper> generateLineageFromStreamingData(String queryId) {
    // Check if we have data for this query
    if (!eventDataByQueryId.containsKey(queryId)) {
      log.warn("No streaming event data found for query ID: {}", queryId);
      return Collections.emptyList();
    }

    // Get the event data
    StreamingEventData eventData = eventDataByQueryId.get(queryId);

    // Check if we have enough data to generate lineage
    if (eventData.event == null) {
      log.warn("No progress event found for query ID: {}", queryId);
      return Collections.emptyList();
    }

    // Check if we have configuration
    if (conf == null) {
      log.warn("No configuration available, cannot generate lineage");
      return Collections.emptyList();
    }

    // Generate lineage
    try {
      // Determine operation type from job metadata
      String operation = "write";
      if (eventData.jobMetadata != null && eventData.jobMetadata.containsKey("operation")) {
        operation = eventData.jobMetadata.get("operation");
      }

      log.info("Generating lineage for query {} with operation {}", queryId, operation);

      // If we have output datasets from catalog table information, use those
      if (eventData.outputDatasets != null && !eventData.outputDatasets.isEmpty()) {
        log.info("Using output datasets from catalog table: {}", eventData.outputDatasets);

        // Generate MCPs using the explicit input and output datasets
        return SparkStreamingEventToDatahub.generateMcpFromStreamingProgressEvent(
            eventData.event,
            conf,
            Collections.emptyMap(), // We don't have schema info at this point
            eventData.inputDatasets != null ? eventData.inputDatasets : Collections.emptySet(),
            eventData.outputDatasets,
            operation);
      } else {
        // Fall back to standard lineage generation
        log.info("No catalog table information available, using standard lineage generation");
        return SparkStreamingEventToDatahub.generateMcpFromStreamingProgressEvent(
            eventData.event, conf, Collections.emptyMap());
      }
    } catch (Exception e) {
      log.error("Error generating lineage from streaming data: {}", e.getMessage(), e);
      return Collections.emptyList();
    }
  }
}
