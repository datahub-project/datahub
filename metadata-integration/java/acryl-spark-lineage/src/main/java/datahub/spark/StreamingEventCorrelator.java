package datahub.spark;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.common.DatasetUrnArray;
import com.linkedin.data.template.StringMap;
import com.linkedin.datajob.DataFlowInfo;
import com.linkedin.datajob.DataJobInfo;
import com.linkedin.datajob.DataJobInputOutput;
import datahub.event.MetadataChangeProposalWrapper;
import datahub.spark.conf.SparkLineageConf;
import datahub.spark.converter.SparkStreamingEventToDatahub;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.streaming.StreamingQueryProgress;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Maintains correlation between Spark streaming events to ensure consistent job lineage.
 * This class tracks streaming queries and their associated delta sinks to create unified jobs
 * that show both inputs and outputs together.
 */
@Slf4j
public class StreamingEventCorrelator {
    private static final long DEFAULT_EXPIRATION_MS = Duration.ofMinutes(10).toMillis();
    
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
    
    /**
     * Creates a new StreamingEventCorrelator with default expiration timeout.
     */
    public StreamingEventCorrelator() {
        this(DEFAULT_EXPIRATION_MS);
        
        // Schedule regular cleanup of expired query information
        scheduler.scheduleAtFixedRate(
            this::cleanupExpiredEvents, 
            DEFAULT_EXPIRATION_MS, 
            DEFAULT_EXPIRATION_MS, 
            TimeUnit.MILLISECONDS);
    }
    
    /**
     * Creates a new StreamingEventCorrelator with custom expiration timeout.
     * 
     * @param expirationMs duration in milliseconds after which unmatched events expire
     */
    public StreamingEventCorrelator(long expirationMs) {
        this.expirationMs = expirationMs;
        log.info("Initialized StreamingEventCorrelator with expiration: {} ms", expirationMs);
    }
    
    /**
     * Processes a streaming query progress event, correlates it with other events if possible,
     * and returns the metadata change proposals to emit.
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
            StreamingEventData eventData = eventDataByQueryId.computeIfAbsent(
              queryId, 
              id -> new StreamingEventData(event, Instant.now())
            );
            
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
            boolean emitDataProcessInstance = conf.getOpenLineageConf() != null && 
                conf.getOpenLineageConf().isEmitDataProcessInstance();
            
            // Generate MCPs based on the event data
            if (isForeachBatchSink) {
                // For ForeachBatchSink, create a data processing job with just inputs
                if (!eventData.inputDatasets.isEmpty()) {
                    log.info("Event has ForeachBatchSink with inputs, generating data processing job MCPs");
                    if (emitDataProcessInstance) {
                        return SparkStreamingEventToDatahub.generateMcpFromStreamingProgressEvent(
                            event, conf, schemaMap, eventData.inputDatasets, new HashSet<>(), operation);
                    } else {
                        return generateMcpsForForeachBatch(event, conf, schemaMap, eventData.inputDatasets, operation);
                    }
                }
            } else if (!eventData.inputDatasets.isEmpty() && !eventData.outputDatasets.isEmpty()) {
                // For regular sinks, if we have both inputs and outputs, generate unified MCPs
                log.info("Event has both inputs and outputs, generating unified MCPs");
                // Create a unified job with a DataProcessInstance for this batch if enabled
                return SparkStreamingEventToDatahub.generateMcpFromStreamingProgressEvent(
                    event, conf, schemaMap, eventData.inputDatasets, eventData.outputDatasets, eventData.operation);
            }
            
            // If we don't have a complete picture yet, just generate normal MCPs
            log.debug("Event doesn't have complete lineage yet, generating standard MCPs");
            return SparkStreamingEventToDatahub.generateMcpFromStreamingProgressEvent(event, conf, schemaMap);
            
        } catch (Exception e) {
            log.error("Error processing streaming event", e);
            // Fall back to standard processing
            return SparkStreamingEventToDatahub.generateMcpFromStreamingProgressEvent(event, conf, schemaMap);
        }
    }
    
    /**
     * Attempts to correlate a streaming event with microbatch events based on query ID and other metadata.
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
    
    /**
     * Merges data from another event into this event.
     */
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
        if (target.microBatchStartMessage == null) target.microBatchStartMessage = source.microBatchStartMessage;
        if (target.microBatchCommitMessage == null) target.microBatchCommitMessage = source.microBatchCommitMessage;
        if (target.deltaSinkWriteMessage == null) target.deltaSinkWriteMessage = source.deltaSinkWriteMessage;
        if (target.logicalPlanMessage == null) target.logicalPlanMessage = source.logicalPlanMessage;
        
        // Add interesting messages
        target.interestingMessages.addAll(source.interestingMessages);
    }
    
    /**
     * Checks if the sink is a ForeachBatchSink from the query JSON.
     */
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
    
    /**
     * Generates MCPs for a ForeachBatchSink event, treating it as a data processing job.
     */
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
            jobCustomProperties.put("processedRowsPerSecond", Double.toString(event.processedRowsPerSecond()));
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
                    if (conf.getOpenLineageConf().isIncludeSchemaMetadata() &&
                        schemaMap.containsKey(inputUrn.toString())) {
                        mcps.add(schemaMap.get(inputUrn.toString()));
                    }
                }
            }
            dataJobInputOutput.setInputDatasets(inputDatasetUrnArray);
            
            // No output datasets (since foreachBatch is custom processing)
            dataJobInputOutput.setOutputDatasets(new DatasetUrnArray());
            
            // Add flow and job MCPs
            mcps.add(MetadataChangeProposalWrapper.create(
                b -> b.entityType("dataFlow").entityUrn(flowUrn).upsert().aspect(dataFlowInfo)));
            
            mcps.add(MetadataChangeProposalWrapper.create(
                b -> b.entityType("dataJob").entityUrn(jobUrn).upsert().aspect(dataJobInfo)));
            
            mcps.add(MetadataChangeProposalWrapper.create(
                b -> b.entityType("dataJob").entityUrn(jobUrn).upsert().aspect(dataJobInputOutput)));
            
            log.info("Generated {} MCPs for foreachBatch processing job", mcps.size());
            
        } catch (Exception e) {
            log.error("Error generating MCPs for foreachBatch", e);
        }
        
        return mcps;
    }
    
    /**
     * Extracts the sink path from a streaming query progress event JSON.
     */
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
    
    /**
     * Extracts input datasets from a streaming query progress event JSON.
     */
    private Set<DatasetUrn> extractInputDatasets(JsonElement root, SparkLineageConf conf) {
        Set<DatasetUrn> datasets = new HashSet<>();
        try {
            if (root.getAsJsonObject().has("sources")) {
                JsonElement sources = root.getAsJsonObject().get("sources");
                if (sources.isJsonArray()) {
                    for (JsonElement source : sources.getAsJsonArray()) {
                        if (source.isJsonObject() && source.getAsJsonObject().has("description")) {
                            String description = source.getAsJsonObject().get("description").getAsString();
                            Optional<DatasetUrn> urn = SparkStreamingEventToDatahub.generateUrnFromStreamingDescription(description, conf);
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
    
    /**
     * Extracts output datasets from a streaming query progress event JSON.
     */
    private Set<DatasetUrn> extractOutputDatasets(JsonElement root, SparkLineageConf conf) {
        Set<DatasetUrn> datasets = new HashSet<>();
        try {
            if (root.getAsJsonObject().has("sink")) {
                JsonObject sink = root.getAsJsonObject().get("sink").getAsJsonObject();
                if (sink.has("description")) {
                    String sinkDescription = sink.get("description").getAsString();
                    Optional<DatasetUrn> urn = SparkStreamingEventToDatahub.generateUrnFromStreamingDescription(sinkDescription, conf);
                    urn.ifPresent(datasets::add);
                }
            }
        } catch (Exception e) {
            log.warn("Failed to extract output datasets", e);
        }
        return datasets;
    }
    
    /**
     * Detects the operation type from the streaming query JSON.
     */
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
    
    /**
     * Removes expired event data to prevent memory leaks.
     */
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
    
    /**
     * Data class to hold information about a streaming query event.
     */
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
        StreamingEventData eventData = eventDataByQueryId.computeIfAbsent(
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
    public void recordMicroBatchCommit(String queryId, Map<String, String> metadata, String logMessage) {
        log.info("Recording microbatch commit for query ID: {}", queryId);
        
        // Ensure we have an event data object for this query
        StreamingEventData eventData = eventDataByQueryId.computeIfAbsent(
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
    public void recordDeltaSinkWrite(String queryId, Map<String, String> metadata, String logMessage) {
        log.info("Recording delta sink write for query ID: {}", queryId);
        
        // Ensure we have an event data object for this query
        StreamingEventData eventData = eventDataByQueryId.computeIfAbsent(
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
        StreamingEventData eventData = eventDataByQueryId.computeIfAbsent(
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
     * @param queryId the query ID
     * @param metadata additional metadata extracted from the log
     * @param logMessage the original log message
     */
    public void recordProgressReport(String queryId, Map<String, String> metadata, String logMessage) {
        log.info("Recording progress report for query ID: {}", queryId);
        
        // Ensure we have an event data object for this query
        StreamingEventData eventData = eventDataByQueryId.computeIfAbsent(
            queryId, id -> new StreamingEventData(null, Instant.now()));
        
        // Update the event data
        eventData.lastUpdated = Instant.now();
        eventData.progressReportMessage = logMessage;
        
        // Store progress JSON if available
        if (metadata.containsKey("progressJson")) {
            String progressJson = metadata.get("progressJson");
            eventData.progressJson = progressJson;
            
            // Try to extract source and sink information from the progress JSON
            extractInfoFromProgressJson(eventData);
            
            // Try to enhance event data with metrics from the progress report
            enhanceWithProgressMetrics(eventData, progressJson);
        }
        
        log.debug("Stored progress report for query ID: {}", queryId);
    }

    /**
     * Enhances event data with metrics from the progress report.
     */
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
                eventData.inputRowsPerSecond = root.getAsJsonObject().get("inputRowsPerSecond").getAsDouble();
            }
            
            if (root.getAsJsonObject().has("processedRowsPerSecond")) {
                eventData.processedRowsPerSecond = root.getAsJsonObject().get("processedRowsPerSecond").getAsDouble();
            }
            
            // Extract state operation metrics if available (important for determining operations)
            if (root.getAsJsonObject().has("stateOperators") && root.getAsJsonObject().get("stateOperators").isJsonArray()) {
                for (JsonElement operator : root.getAsJsonObject().get("stateOperators").getAsJsonArray()) {
                    if (operator.isJsonObject()) {
                        String operatorType = operator.getAsJsonObject().has("operatorName") ?
                                operator.getAsJsonObject().get("operatorName").getAsString() : null;
                        
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
            if (root.getAsJsonObject().has("eventTime") && 
                root.getAsJsonObject().get("eventTime").getAsJsonObject().has("watermark")) {
                eventData.hasWatermark = true;
                eventData.watermark = root.getAsJsonObject().get("eventTime")
                        .getAsJsonObject().get("watermark").getAsString();
            }
            
            log.info("Enhanced event data with metrics from progress report");
        } catch (Exception e) {
            log.warn("Error enhancing event data with progress metrics: {}", e.getMessage(), e);
        }
    }

    /**
     * Records an interesting message.
     * 
     * @param queryId the query ID
     * @param metadata additional metadata extracted from the log
     * @param logMessage the original log message
     */
    public void recordInterestingMessage(String queryId, Map<String, String> metadata, String logMessage) {
        log.info("Recording interesting message for query ID: {}", queryId);
        
        // Ensure we have an event data object for this query
        StreamingEventData eventData = eventDataByQueryId.computeIfAbsent(
            queryId, id -> new StreamingEventData(null, Instant.now()));
        
        // Update the event data
        eventData.lastUpdated = Instant.now();
        eventData.interestingMessages.add(logMessage);
        
        log.debug("Stored interesting message for query ID: {}", queryId);
    }

    /**
     * Extract transformations from the logical plan.
     */
    private void extractTransformationsFromLogicalPlan(StreamingEventData eventData, Map<String, String> metadata) {
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

    /**
     * Try to extract input and output datasets from the logical plan.
     */
    private void tryExtractInputOutputDatasetsFromLogicalPlan(StreamingEventData eventData, Map<String, String> metadata) {
        try {
            // Create a default SparkLineageConf
            SparkLineageConf defaultConf = SparkLineageConf.builder()
                    .openLineageConf(DatahubOpenlineageConfig.builder().build())
                    .build();
            
            // Use logical plan to extract input datasets
            if (metadata.containsKey("sourceType") && metadata.containsKey("sourcePath")) {
                String sourceType = metadata.get("sourceType");
                String sourcePath = metadata.get("sourcePath");
                
                if (sourceType.equals("DeltaSource") && !sourcePath.isEmpty()) {
                    // Create a dataset URN for the source
                    Optional<DatasetUrn> sourceUrn = SparkStreamingEventToDatahub.generateUrnFromStreamingDescription(
                        "DeltaSource[" + sourcePath + "]", 
                        defaultConf);
                    
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
                    Optional<DatasetUrn> sinkUrn = SparkStreamingEventToDatahub.generateUrnFromStreamingDescription(
                        "DeltaSink[" + sinkPath + "]", 
                        defaultConf);
                    
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

    /**
     * Extract source and sink information from the progress JSON.
     */
    private void extractInfoFromProgressJson(StreamingEventData eventData) {
        try {
            // Create a default SparkLineageConf
            SparkLineageConf defaultConf = SparkLineageConf.builder()
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
                                    // Try to create a dataset URN from the source description
                                    Optional<DatasetUrn> sourceUrn = SparkStreamingEventToDatahub.generateUrnFromStreamingDescription(
                                        description, 
                                        defaultConf);
                                    
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
                                    // Try to create a dataset URN from the sink description
                                    Optional<DatasetUrn> sinkUrn = SparkStreamingEventToDatahub.generateUrnFromStreamingDescription(
                                        description, 
                                        defaultConf);
                                    
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

    /**
     * Find the matching close bracket for a given open bracket position.
     */
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

    /**
     * Query information tracking class.
     */
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
    
    /**
     * Batch state enum.
     */
    private enum BatchState {
        UNKNOWN,
        STARTED,
        PROCESSING,
        COMMITTED,
        FAILED
    }
} 