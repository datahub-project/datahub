package datahub.spark.converter;

import static io.datahubproject.openlineage.utils.DatahubUtils.*;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.linkedin.common.DatasetUrnArray;
import com.linkedin.common.TimeStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.datajob.DataFlowInfo;
import com.linkedin.datajob.DataJobInfo;
import com.linkedin.datajob.DataJobInputOutput;
import com.linkedin.datajob.EditableDataFlowProperties;
import com.linkedin.datajob.EditableDataJobProperties;
import com.linkedin.dataprocess.DataProcessInstanceProperties;
import com.linkedin.dataprocess.DataProcessInstanceInput;
import com.linkedin.dataprocess.DataProcessInstanceOutput;
import com.linkedin.dataprocess.DataProcessInstanceRelationships;
import com.linkedin.dataprocess.DataProcessType;
import datahub.event.MetadataChangeProposalWrapper;
import datahub.spark.conf.SparkLineageConf;
import io.datahubproject.openlineage.dataset.HdfsPathDataset;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.streaming.StreamingQueryProgress;

@Slf4j
public class SparkStreamingEventToDatahub {
  private SparkStreamingEventToDatahub() {}

  public static final String DELTA_LAKE_PLATFORM = "delta-lake";
  public static final String FILE_PLATFORM = "file";
  public static final String KAFKA_PLATFORM = "kafka";

  public static List<MetadataChangeProposalWrapper> generateMcpFromStreamingProgressEvent(
      StreamingQueryProgress event,
      SparkLineageConf conf,
      Map<String, MetadataChangeProposalWrapper> schemaMap) {
    List<MetadataChangeProposalWrapper> mcps = new ArrayList<>();
    
    try {
      // Validate and log configuration status
      if (conf == null) {
        log.error("SparkLineageConf is null, cannot generate MCPs for streaming event");
        return mcps;
      }
      
      if (conf.getSparkAppContext() == null) {
        log.warn("SparkAppContext is null in SparkLineageConf - some features may not work properly");
      } else {
        // Log SparkConf status to help with debugging
        if (conf.getSparkAppContext().getConf() == null) {
          log.warn("SparkConf is null in SparkAppContext - Databricks detection will be disabled");
        } else {
          log.debug("SparkConf is available with {} settings", 
                   conf.getSparkAppContext().getConf().getAll().length);
        }
      }
      
      // Get application/flow name - this should be consistent with batch mode
      // Check if there's a configured pipeline name first (just like in batch mode)
      String flowName = conf.getOpenLineageConf().getPipelineName();
      if (flowName == null || flowName.trim().isEmpty()) {
        // If no explicit pipeline name, use application name or cluster ID
        // This ensures the flow is consistent across streaming and batch
        flowName = determineFlowName(conf);
      }
      
      // Now determine a job name that will be consistent across runs
      String jobName = determineStreamingJobName(event, conf);
      
      // The pipeline name should combine flow and job in a way consistent with batch mode
      String pipelineName;
      if (flowName != null && !flowName.isEmpty()) {
        pipelineName = String.format("%s.%s", flowName, jobName);
      } else {
        pipelineName = jobName;
      }
      
      log.info("Using flowName: {}, jobName: {}, pipelineName: {}", flowName, jobName, pipelineName);

      DataFlowInfo dataFlowInfo = new DataFlowInfo();
      dataFlowInfo.setName(flowName);
      dataFlowInfo.setDescription("Spark Streaming Flow: " + flowName);
      
      StringMap flowCustomProperties = new StringMap();

      Long appStartTime;
      if (conf.getSparkAppContext() != null) {
        appStartTime = conf.getSparkAppContext().getStartTime();
        if (appStartTime != null) {
          flowCustomProperties.put("createdAt", appStartTime.toString());
          flowCustomProperties.put("id", event.id().toString());
          dataFlowInfo.setCreated(new TimeStamp().setTime(appStartTime));
        }
      }

      flowCustomProperties.put("plan", event.json());
      dataFlowInfo.setCustomProperties(flowCustomProperties);

      // Use the same flow creation logic as batch mode
      DataFlowUrn flowUrn;
      if (conf.getSparkAppContext() != null && conf.getSparkAppContext().getAppId() != null) {
        // Extract cluster/namespace from the conf
        String namespace = conf.getSparkAppContext().getAppId();
        
        // For Databricks, try to get a stable namespace
        String platformInstance = "default";
        if (isDatabricksEnvironment(conf)) {
          String dbPlatformInstance = extractDatabricksPlatformInstance(conf);
          if (dbPlatformInstance != null && !dbPlatformInstance.isEmpty()) {
            platformInstance = dbPlatformInstance;
          }
        }
        
        // Create the flow URN using the same pattern as batch mode (platform, flowName, platformInstance)
        flowUrn = new DataFlowUrn("spark", flowName, platformInstance);
      } else {
        // Fallback to simpler URN creation if app context isn't available
        flowUrn = createValidFlowUrn(conf, event, flowName);
      }

      log.debug("Creating streaming flow URN with namespace: {}, name: {}", 
                flowUrn.getOrchestratorEntity(),
                flowUrn.getFlowIdEntity());

      MetadataChangeProposalWrapper dataflowMcp =
          MetadataChangeProposalWrapper.create(
              b -> b.entityType("dataFlow").entityUrn(flowUrn).upsert().aspect(dataFlowInfo));
      mcps.add(dataflowMcp);

      // Create the job URN using the same flow URN as above
      DataJobUrn jobUrn = jobUrn(flowUrn, jobName);
      
      DataJobInfo dataJobInfo = new DataJobInfo();
      dataJobInfo.setName(jobName);
      dataJobInfo.setType(DataJobInfo.Type.create("SPARK"));
      dataJobInfo.setDescription("Spark Streaming Job: " + jobName);

      StringMap jobCustomProperties = new StringMap();
      jobCustomProperties.put("batchId", Long.toString(event.batchId()));
      jobCustomProperties.put("inputRowsPerSecond", Double.toString(event.inputRowsPerSecond()));
      jobCustomProperties.put(
          "processedRowsPerSecond", Double.toString(event.processedRowsPerSecond()));
      jobCustomProperties.put("numInputRows", Long.toString(event.numInputRows()));
      dataJobInfo.setCustomProperties(jobCustomProperties);

      MetadataChangeProposalWrapper dataJobMcp =
          MetadataChangeProposalWrapper.create(
              b -> b.entityType("dataJob").entityUrn(jobUrn).upsert().aspect(dataJobInfo));
      mcps.add(dataJobMcp);

      DataJobInputOutput dataJobInputOutput = new DataJobInputOutput();

      JsonElement root = new JsonParser().parse(event.json());
      DatasetUrnArray inputDatasetUrnArray = new DatasetUrnArray();
      
      try {
        if (root.getAsJsonObject().has("sources") && 
            root.getAsJsonObject().get("sources").isJsonArray()) {
          for (JsonElement source : root.getAsJsonObject().get("sources").getAsJsonArray()) {
            try {
              if (source.isJsonObject() && source.getAsJsonObject().has("description")) {
                String description = source.getAsJsonObject().get("description").getAsString();
                Optional<DatasetUrn> urn =
                    SparkStreamingEventToDatahub.generateUrnFromStreamingDescription(description, conf);
                if (urn.isPresent()) {
                  if (inputDatasetUrnArray.contains(urn.get())) {
                    log.debug("We already have dataset {} in the list, skipping it.", urn.get());
                    continue;
                  }
                  
                  inputDatasetUrnArray.add(urn.get());
                  if (conf.getOpenLineageConf().isMaterializeDataset()) {
                    MetadataChangeProposalWrapper datasetMcp = generateDatasetMcp(urn.get());
                    mcps.add(datasetMcp);
                    if (conf.getOpenLineageConf().isIncludeSchemaMetadata()
                        && schemaMap.containsKey(urn.get().toString())) {
                      mcps.add(schemaMap.get(urn.get().toString()));
                    }
                  }
                }
              }
            } catch (Exception e) {
              log.warn("Error processing source element: {}", e.getMessage());
              // Continue with next source
            }
          }
        }
      } catch (Exception e) {
        log.warn("Error processing sources array: {}", e.getMessage());
      }

      DatasetUrnArray outputDatasetUrnArray = new DatasetUrnArray();
      try {
        if (root.getAsJsonObject().has("sink")) {
          JsonObject sink = root.getAsJsonObject().get("sink").getAsJsonObject();
          
          // Add detailed logging about the sink structure
          log.info("Processing streaming sink: {}", sink);
          
          if (sink.has("description")) {
            String sinkDescription = sink.get("description").getAsString();
            log.info("Found sink description: '{}'", sinkDescription);
            
            // Check if this might be a merge operation by examining the query JSON
            String operation = detectOperationType(root);
            log.info("Detected operation type: {}", operation);
            
            if (operation.contains("merge")) {
              log.info("This appears to be a merge operation - special handling may be needed");
              // For debugging, log the entire query plan 
              log.info("Full query plan JSON for merge operation: {}", event.json());
            }
            
            Optional<DatasetUrn> urn =
                SparkStreamingEventToDatahub.generateUrnFromStreamingDescription(sinkDescription, conf);
            
            if (urn.isPresent()) {
              log.info("Successfully generated sink URN: {}", urn.get());
              MetadataChangeProposalWrapper datasetMcp = generateDatasetMcp(urn.get());
              outputDatasetUrnArray.add(urn.get());
              mcps.add(datasetMcp);
              if (conf.getOpenLineageConf().isIncludeSchemaMetadata()
                  && schemaMap.containsKey(urn.get().toString())) {
                mcps.add(schemaMap.get(urn.get().toString()));
              }
            } else {
              log.warn("Failed to generate URN from sink description: '{}'", sinkDescription);
            }
          } else {
            log.warn("Sink object does not contain a 'description' field: {}", sink);
          }
        } else {
          log.warn("No 'sink' object found in streaming query JSON");
        }
      } catch (Exception e) {
        log.warn("Error processing sink element: {}", e.getMessage(), e);
      }

      dataJobInputOutput.setInputDatasets(inputDatasetUrnArray);
      dataJobInputOutput.setOutputDatasets(outputDatasetUrnArray);

      MetadataChangeProposalWrapper inputOutputMcp =
          MetadataChangeProposalWrapper.create(
              b -> b.entityType("dataJob").entityUrn(jobUrn).upsert().aspect(dataJobInputOutput));

      mcps.add(inputOutputMcp);
    } catch (Exception e) {
      log.error("Unexpected error processing streaming event: {}", e.getMessage(), e);
      // Return any MCPs we managed to create before the error
      if (mcps.isEmpty()) {
        log.warn("No metadata could be extracted from streaming event due to errors");
      } else {
        log.info("Returning {} MCPs that were successfully created before error", mcps.size());
      }
    }
    
    return mcps;
  }

  /**
   * Determines a flow name that will be consistent with batch mode.
   * This ensures the flow URN is the same whether in streaming or batch mode.
   */
  private static String determineFlowName(SparkLineageConf conf) {
    try {
      // Try to get app name or app ID first
      if (conf.getSparkAppContext() != null) {
        if (conf.getSparkAppContext().getAppName() != null && 
            !conf.getSparkAppContext().getAppName().isEmpty()) {
          return conf.getSparkAppContext().getAppName();
        } else if (conf.getSparkAppContext().getAppId() != null) {
          // Extract app name part from app ID
          String appId = conf.getSparkAppContext().getAppId();
          if (appId.contains(".")) {
            return appId.substring(0, appId.indexOf('.'));
          }
          return appId;
        }
      }
      
      // If in Databricks, try to get a stable identifier
      if (isDatabricksEnvironment(conf)) {
        if (conf.getSparkAppContext() != null && 
            conf.getSparkAppContext().getConf() != null) {
          SparkConf sparkConf = conf.getSparkAppContext().getConf();
          
          // Try notebook path - often a good identifier in Databricks
          if (sparkConf.contains("spark.databricks.notebook.path")) {
            String notebookPath = sparkConf.get("spark.databricks.notebook.path");
            // Extract notebook name without path
            if (notebookPath.contains("/")) {
              return notebookPath.substring(notebookPath.lastIndexOf('/') + 1);
            }
            return notebookPath;
          }
          
          // Try to get cluster or job info
          if (sparkConf.contains("spark.databricks.clusterUsageTags.clusterId")) {
            return sparkConf.get("spark.databricks.clusterUsageTags.clusterId");
          }
        }
      }
      
      // Default fallback
      return "spark_streaming_job";
    } catch (Exception e) {
      log.warn("Error determining flow name: {}", e.getMessage());
      return "spark_streaming_job";
    }
  }
  
  /**
   * Determines a job name that will be consistent across runs.
   * This focuses on the specific streaming operation rather than the application.
   */
  private static String determineStreamingJobName(StreamingQueryProgress event, SparkLineageConf conf) {
    try {
      // First check if the query has a name - this is the most stable identifier
      if (event.name() != null && !event.name().isEmpty()) {
        return event.name();
      }
      
      // Parse the streaming query JSON to extract more details
      JsonElement root = new JsonParser().parse(event.json());
      
      // Get sink information to identify the target
      String sinkType = null;
      String sinkPath = null;
      if (root.getAsJsonObject().has("sink")) {
        JsonObject sink = root.getAsJsonObject().get("sink").getAsJsonObject();
        if (sink.has("description")) {
          String sinkDescription = sink.get("description").getAsString();
          sinkType = sinkDescription.split("\\[")[0];
          sinkPath = StringUtils.substringBetween(sinkDescription, "[", "]");
        }
      }
      
      // Extract table name for better identification
      String tableName = extractTableNameFromPath(sinkPath);
      
      // Get operation type (e.g., "write", "merge")
      String operation = detectOperationType(root);
      
      // Build a consistent job name
      if (tableName != null) {
        return String.format("%s_%s_%s", operation, sinkType, tableName.replace(".", "_"));
      } else if (sinkType != null && sinkPath != null) {
        // Create a hash of the path for stability across runs
        String pathHash = String.valueOf(Math.abs(sinkPath.hashCode() % 10000));
        return String.format("%s_%s_%s", operation, sinkType, pathHash);
      }
      
      // Fallback to query ID - less ideal but still consistent for a given query
      return "streaming_query_" + event.id();
      
    } catch (Exception e) {
      log.warn("Error determining job name: {}", e.getMessage());
      // Use query ID as fallback
      return "streaming_query_" + event.id();
    }
  }
  
  /**
   * Extracts a table name from a path if possible.
   */
  private static String extractTableNameFromPath(String path) {
    if (path == null) {
      return null;
    }
    
    try {
      // For Delta tables, the path often contains the table name
      if (path.contains("/tables/")) {
        String[] parts = path.split("/tables/");
        if (parts.length > 1) {
          return parts[1].replace("/", ".");
        }
      }
      // For warehouse paths - extract DB and table name
      else if (path.contains("/warehouse/")) {
        // Look for the pattern /warehouse/db.db/table/
        if (path.contains(".db/")) {
          int dbIndex = path.lastIndexOf(".db/");
          if (dbIndex > 0) {
            String dbName = path.substring(path.lastIndexOf("/warehouse/") + 11, dbIndex);
            String tableNamePart = path.substring(dbIndex + 4).replaceAll("/+$", "");
            return dbName + "." + tableNamePart;
          }
        }
      }
    } catch (Exception e) {
      log.warn("Error extracting table name from path: {}", e.getMessage());
    }
    return null;
  }

  public static Optional<DatasetUrn> generateUrnFromStreamingDescription(
      String description, SparkLineageConf sparkLineageConf) {
    try {
      // Special handling for ForeachBatchSink
      if ("ForeachBatchSink".equals(description)) {
        log.info("Found ForeachBatchSink - this represents custom processing logic, not a real dataset");
        // For ForeachBatchSink, we don't create a dataset URN as it's not a real dataset
        // Instead, we return empty to indicate no dataset should be created
        return Optional.empty();
      }
      
      String pattern = "(.*?)\\[(.*)]";
      Pattern r = Pattern.compile(pattern);
      Matcher m = r.matcher(description);
      if (m.find()) {
        String namespace = m.group(1);
        String platform = getDatahubPlatform(namespace);
        String path = m.group(2);
        log.debug("Streaming description Platform: {}, Path: {}", platform, path);
        if (platform.equals(KAFKA_PLATFORM)) {
          path = getKafkaTopicFromPath(m.group(2));
        } else if (platform.equals(FILE_PLATFORM) || platform.equals(DELTA_LAKE_PLATFORM)) {
          try {
            DatasetUrn urn =
                HdfsPathDataset.create(new URI(path), sparkLineageConf.getOpenLineageConf()).urn();
            return Optional.of(urn);
          } catch (InstantiationException e) {
            log.warn("InstantiationException when creating HdfsPathDataset: {}", e.getMessage());
            return Optional.empty();
          } catch (URISyntaxException e) {
            log.warn("Failed to parse path {}: {}", path, e.getMessage());
            return Optional.empty();
          }
        }
        return Optional.of(
            new DatasetUrn(
                new DataPlatformUrn(platform),
                path,
                sparkLineageConf.getOpenLineageConf().getFabricType()));
      } else {
        log.debug("No match found in description: {}", description);
        return Optional.empty();
      }
    } catch (Exception e) {
      log.warn("Error generating URN from streaming description '{}': {}", 
              description, e.getMessage());
      return Optional.empty();
    }
  }

  public static String getDatahubPlatform(String namespace) {
    switch (namespace) {
      case "KafkaV2":
        return "kafka";
      case "DeltaSink":
        return "delta-lake";
      case "CloudFilesSource":
        return "dbfs";
      case "FileSink":
      case "FileStreamSource":
        return "file";
      default:
        return namespace;
    }
  }

  public static String getKafkaTopicFromPath(String path) {
    return StringUtils.substringBetween(path, "[", "]");
  }

  /**
   * Creates a valid DataFlowUrn with fallbacks to prevent null parameters.
   * Uses information from OpenLineage and Spark where available.
   */
  private static DataFlowUrn createValidFlowUrn(
      SparkLineageConf conf, 
      StreamingQueryProgress event, 
      String pipelineName) {
    
    // Validate platform instance
    String platformInstance = getOpenLineagePlatformInstance(conf);
    if (StringUtils.isBlank(platformInstance)) {
      // Use SparkContext information if available
      if (conf.getSparkAppContext() != null) {
        try {
          // Check for Databricks-specific environment
          if (isDatabricksEnvironment(conf)) {
            platformInstance = extractDatabricksPlatformInstance(conf);
            log.info("Using Databricks platform instance: {}", platformInstance);
          } else if (conf.getSparkAppContext().getConf() != null) {
            // Try to extract the platform from Spark configuration
            String sparkMaster = conf.getSparkAppContext().getConf().get("spark.master", "");
            if (sparkMaster.startsWith("k8s://")) {
              platformInstance = "spark_k8s";
            } else if (sparkMaster.startsWith("yarn")) {
              platformInstance = "spark_yarn";
            } else {
              platformInstance = "default"; // Use "default" to match batch mode
            }
            log.info("Using platform instance '{}' derived from spark.master", platformInstance);
          } else {
            platformInstance = "default"; // Use "default" to match batch mode
            log.info("SparkConf is null, using default platform instance: {}", platformInstance);
          }
        } catch (Exception e) {
          log.warn("Could not extract platform from Spark configuration", e);
          platformInstance = "default"; // Use "default" to match batch mode
        }
      } else {
        log.warn("Platform instance is null or empty, using 'default' as fallback for URN creation");
        platformInstance = "default"; // Use "default" to match batch mode
      }
    }

    // Validate pipeline name
    if (StringUtils.isBlank(pipelineName)) {
      log.warn("Pipeline name is null or empty, this should have been handled earlier");
      // Create a fallback name using the event ID to ensure uniqueness
      pipelineName = "spark_streaming_" + event.id();
    }

    return new DataFlowUrn("spark", pipelineName, platformInstance);
  }
  
  /**
   * Safely extracts the platform instance from OpenLineage configuration.
   * Uses direct access and avoids reflection when possible.
   */
  private static String getOpenLineagePlatformInstance(SparkLineageConf conf) {
    if (conf == null || conf.getOpenLineageConf() == null) {
      return null;
    }
    
    try {
      // Direct method call without reflection
      return conf.getOpenLineageConf().getPlatformInstance();
    } catch (Exception e) {
      log.warn("Error accessing platform instance via OpenLineage configuration", e);
      return null;
    }
  }
  
  /**
   * Checks if the current environment is Databricks.
   */
  private static boolean isDatabricksEnvironment(SparkLineageConf conf) {
    if (conf.getSparkAppContext() == null || conf.getSparkAppContext().getConf() == null) {
      return false;
    }
    
    // Check for Databricks-specific properties
    boolean hasDbProperty = conf.getSparkAppContext().getConf().contains("spark.databricks.clusterUsageTags.clusterId");
    
    // Check for DATABRICKS_RUNTIME_VERSION env var
    String dbRuntime = System.getenv("DATABRICKS_RUNTIME_VERSION");
    boolean hasDbEnv = dbRuntime != null && !dbRuntime.isEmpty();
    
    return hasDbProperty || hasDbEnv;
  }
  
  /**
   * Extracts Databricks-specific information to create a detailed platform instance.
   * Focuses on stable identifiers that remain constant across runs of the same job.
   */
  private static String extractDatabricksPlatformInstance(SparkLineageConf conf) {
    StringBuilder platformBuilder = new StringBuilder("databricks");
    
    // Check if SparkConf is available
    if (conf.getSparkAppContext() == null || conf.getSparkAppContext().getConf() == null) {
      return platformBuilder.toString();
    }
    
    // Try to get workspace ID - this is stable across all runs in the same workspace
    String workspaceId = conf.getSparkAppContext().getConf().get("spark.databricks.workspaceId", "");
    if (!workspaceId.isEmpty()) {
      platformBuilder.append("_ws_").append(workspaceId);
    }
    
    // Check if it's a job cluster
    String clusterTags = conf.getSparkAppContext().getConf().get("spark.databricks.clusterUsageTags.clusterAllTags", "");
    if (clusterTags.contains("JobId:")) {
      // This is a job cluster - try to get the job definition ID (not the run ID)
      // In Databricks, jobId refers to the job definition, which is stable
      Pattern jobIdPattern = Pattern.compile("JobId:(\\d+)");
      Matcher matcher = jobIdPattern.matcher(clusterTags);
      if (matcher.find()) {
        String jobId = matcher.group(1);
        platformBuilder.append("_job_").append(jobId);
        
        // Try to extract the job name if available, which is more stable than cluster ID
        // for job clusters (which are ephemeral)
        String jobName = extractJobNameFromTags(clusterTags);
        if (jobName != null && !jobName.isEmpty()) {
          // Sanitize job name to be URN-safe
          jobName = jobName.replaceAll("[^a-zA-Z0-9]", "_").toLowerCase();
          platformBuilder.append("_").append(jobName);
        }
      }
    } else {
      // This is a permanent cluster, so the cluster ID is a stable identifier
      String clusterId = conf.getSparkAppContext().getConf().get("spark.databricks.clusterUsageTags.clusterId", "");
      if (!clusterId.isEmpty()) {
        // Use just the first part of the cluster ID to keep it shorter
        if (clusterId.length() > 8) {
          clusterId = clusterId.substring(0, 8);
        }
        platformBuilder.append("_cluster_").append(clusterId);
      }
    }
    
    return platformBuilder.toString();
  }
  
  /**
   * Attempts to extract a job name from Databricks cluster tags.
   */
  private static String extractJobNameFromTags(String clusterTags) {
    // Look for JobName tag in cluster tags
    Pattern jobNamePattern = Pattern.compile("JobName:([^,]+)");
    Matcher matcher = jobNamePattern.matcher(clusterTags);
    if (matcher.find()) {
      return matcher.group(1);
    }
    
    // Alternative approach: check for notebook path which is often part of the job
    Pattern notebookPattern = Pattern.compile("NotebookPath:([^,]+)");
    matcher = notebookPattern.matcher(clusterTags);
    if (matcher.find()) {
      String notebookPath = matcher.group(1);
      // Extract just the notebook name from the path
      int lastSlash = notebookPath.lastIndexOf("/");
      if (lastSlash >= 0 && lastSlash < notebookPath.length() - 1) {
        return notebookPath.substring(lastSlash + 1);
      }
      return notebookPath;
    }
    
    return null;
  }
  
  /**
   * Attempts to detect the operation type from the query JSON.
   */
  private static String detectOperationType(JsonElement root) {
    try {
      // Check if query JSON contains hints about the operation
      if (root.getAsJsonObject().has("sink")) {
        JsonObject sink = root.getAsJsonObject().get("sink").getAsJsonObject();
        
        // Check sink description for operation hints
        if (sink.has("description")) {
          String desc = sink.get("description").getAsString().toLowerCase();
          
          // Special handling for ForeachBatchSink
          if (desc.equals("foreachbatchsink")) {
            log.info("Detected ForeachBatchSink operation");
            return "foreachBatch";
          }
          
          if (desc.contains("merge") || desc.contains("delta")) {
            // Look for specific MERGE operation in JSON
            String json = root.toString().toLowerCase();
            if (json.contains("mergeinto") || json.contains("merge into")) {
              log.info("Detected MERGE operation from JSON content");
              return "merge_into";
            } else if (json.contains("delta")) {
              log.info("Delta operation detected, checking for specific operation type");
              // Delta operations might not explicitly say "merge" but could be doing merges
              if (desc.contains("delta") && desc.contains("sink")) {
                return "delta_write";
              }
            }
          }
          if (desc.contains("append")) return "append";
          if (desc.contains("overwrite")) return "overwrite";
          if (desc.contains("update")) return "update";
          if (desc.contains("delete")) return "delete";
        }
      }
      
      // Default to "write" if we can't detect a specific operation
      return "write";
    } catch (Exception e) {
      log.warn("Error detecting operation type: {}", e.getMessage());
      return "stream";
    }
  }

  /**
   * Generate MCPs for a streaming query with explicitly provided input and output datasets.
   */
  public static List<MetadataChangeProposalWrapper> generateMcpFromStreamingProgressEvent(
      StreamingQueryProgress event,
      SparkLineageConf conf,
      Map<String, MetadataChangeProposalWrapper> schemaMap,
      Set<DatasetUrn> inputDatasets,
      Set<DatasetUrn> outputDatasets,
      String operation) {
    
    List<MetadataChangeProposalWrapper> mcps = new ArrayList<>();
    
    try {
      // First generate the standard data job MCPs
      mcps.addAll(generateMcpFromStreamingProgressEvent(event, conf, schemaMap));
      
      // Skip DataProcessInstance generation if the feature is disabled
      if (conf.getOpenLineageConf() != null && !conf.getOpenLineageConf().isEmitDataProcessInstance()) {
        log.info("DataProcessInstance emission is disabled in config, skipping...");
        return mcps;
      }
      
      // We need to find the DataJobUrn that was created
      String flowName = conf.getOpenLineageConf().getPipelineName();
      if (flowName == null || flowName.trim().isEmpty()) {
        flowName = determineFlowName(conf);
      }
      String jobName = determineStreamingJobName(event, conf);
      
      // Create flow URN
      DataFlowUrn flowUrn;
      if (conf.getSparkAppContext() != null && conf.getSparkAppContext().getAppId() != null) {
        String platformInstance = "default";
        if (isDatabricksEnvironment(conf)) {
          String dbPlatformInstance = extractDatabricksPlatformInstance(conf);
          if (dbPlatformInstance != null && !dbPlatformInstance.isEmpty()) {
            platformInstance = dbPlatformInstance;
          }
        }
        flowUrn = new DataFlowUrn("spark", flowName, platformInstance);
      } else {
        flowUrn = createValidFlowUrn(conf, event, flowName);
      }
      
      // Create job URN
      DataJobUrn jobUrn = jobUrn(flowUrn, jobName);
      
      // Generate MCPs for the DataProcessInstance representing this microbatch
      mcps.addAll(generateDataProcessInstanceMcps(event, jobUrn, inputDatasets, outputDatasets, operation));
      
      log.info("Successfully generated DataProcessInstance for Spark streaming microbatch {}", event.batchId());
      
      return mcps;
    } catch (Exception e) {
      log.error("Error generating MCPs with DataProcessInstance: {}", e.getMessage(), e);
      // Fall back to standard MCPs if there's an error
      return generateMcpFromStreamingProgressEvent(event, conf, schemaMap);
    }
  }
  
  /**
   * Extracts a human-readable name from a dataset URN.
   */
  private static String extractDatasetName(DatasetUrn urn) {
    String[] parts = urn.getDatasetNameEntity().split("/");
    return parts[parts.length - 1];
  }
  
  /**
   * Generate a MCP for a dataset entity
   */
  public static MetadataChangeProposalWrapper generateDatasetMcp(DatasetUrn urn) {
    try {
      // Create a minimal dataset aspect
      com.linkedin.dataset.DatasetProperties datasetProperties = new com.linkedin.dataset.DatasetProperties();
      datasetProperties.setName(extractDatasetName(urn));
      datasetProperties.setDescription("Dataset referenced in Spark Structured Streaming");
      
      // Create the MCP
      return MetadataChangeProposalWrapper.create(
          b -> b.entityType("dataset").entityUrn(urn).upsert().aspect(datasetProperties));
    } catch (Exception e) {
      log.error("Error generating dataset MCP for URN {}: {}", urn, e.getMessage(), e);
      throw new RuntimeException("Failed to generate dataset MCP", e);
    }
  }
  
  /**
   * Create a DataProcessInstance URN from a streaming query and batch ID.
   * @param event The streaming query progress event
   * @param batchId The batch ID to use for the instance
   * @return A unique URN for the DataProcessInstance
   */
  public static Urn createDataProcessInstanceUrn(StreamingQueryProgress event, long batchId) {
    try {
      // Create a unique identifier using query ID and batch ID
      String id = String.format("%s-%d", event.id().toString(), batchId);
      
      // Create and return the URN
      return Urn.createFromString("urn:li:dataProcessInstance:" + id);
    } catch (Exception e) {
      log.error("Error creating DataProcessInstance URN", e);
      // Fallback to using UUID if there's an issue
      return Urn.createFromTuple("dataProcessInstance", UUID.randomUUID().toString());
    }
  }
  
  /**
   * Generate MCPs for a DataProcessInstance entity that represents a microbatch execution.
   * 
   * @param event Streaming query progress event
   * @param jobUrn The DataJob URN associated with this process
   * @param inputDatasets Input datasets for the microbatch
   * @param outputDatasets Output datasets for the microbatch
   * @param operation The operation type (e.g., write, merge)
   * @return List of MCPs for the DataProcessInstance
   */
  public static List<MetadataChangeProposalWrapper> generateDataProcessInstanceMcps(
      StreamingQueryProgress event,
      DataJobUrn jobUrn,
      Set<DatasetUrn> inputDatasets,
      Set<DatasetUrn> outputDatasets,
      String operation) {
    
    List<MetadataChangeProposalWrapper> mcps = new ArrayList<>();
    
    try {
      // Create a unique URN for this microbatch execution
      Urn dpiUrn = createDataProcessInstanceUrn(event, event.batchId());
      
      // Create an audit stamp for the current time
      AuditStamp auditStamp = new AuditStamp()
          .setTime(System.currentTimeMillis())
          .setActor(Urn.createFromString("urn:li:corpuser:sparkStreaming"));
      
      // Create properties for the instance
      DataProcessInstanceProperties properties = new DataProcessInstanceProperties();
      properties.setName(String.format("Spark Streaming Microbatch %d", event.batchId()));
      properties.setType(DataProcessType.STREAMING);
      properties.setCreated(auditStamp);
      
      StringMap customProperties = new StringMap();
      customProperties.put("batchId", Long.toString(event.batchId()));
      customProperties.put("inputRowsPerSecond", Double.toString(event.inputRowsPerSecond()));
      customProperties.put("processedRowsPerSecond", Double.toString(event.processedRowsPerSecond()));
      customProperties.put("numInputRows", Long.toString(event.numInputRows()));
      customProperties.put("operation", operation);
      properties.setCustomProperties(customProperties);
      
      // Create the relationship to parent job with required upstreamInstances
      DataProcessInstanceRelationships relationships = new DataProcessInstanceRelationships();
      relationships.setParentTemplate(jobUrn);
      
      // Initialize the upstreamInstances field with an empty array since it's required
      UrnArray upstreamInstances = new UrnArray();
      relationships.setUpstreamInstances(upstreamInstances);
      
      // Create input aspect with dataset URNs
      DataProcessInstanceInput input = new DataProcessInstanceInput();
      UrnArray inputUrns = new UrnArray();
      inputDatasets.forEach(urn -> inputUrns.add(urn));
      input.setInputs(inputUrns);
      
      // Create output aspect with dataset URNs
      DataProcessInstanceOutput output = new DataProcessInstanceOutput();
      UrnArray outputUrns = new UrnArray();
      outputDatasets.forEach(urn -> outputUrns.add(urn));
      output.setOutputs(outputUrns);
      
      // Add MCPs for all aspects
      mcps.add(MetadataChangeProposalWrapper.create(
          b -> b.entityType("dataProcessInstance").entityUrn(dpiUrn).upsert().aspect(properties)));
      
      mcps.add(MetadataChangeProposalWrapper.create(
          b -> b.entityType("dataProcessInstance").entityUrn(dpiUrn).upsert().aspect(relationships)));
      
      mcps.add(MetadataChangeProposalWrapper.create(
          b -> b.entityType("dataProcessInstance").entityUrn(dpiUrn).upsert().aspect(input)));
      
      mcps.add(MetadataChangeProposalWrapper.create(
          b -> b.entityType("dataProcessInstance").entityUrn(dpiUrn).upsert().aspect(output)));
      
      return mcps;
    } catch (Exception e) {
      log.error("Error generating DataProcessInstance MCPs", e);
      return Collections.emptyList();
    }
  }
}
