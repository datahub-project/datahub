package datahub.spark;

import static com.linkedin.metadata.Constants.*;
import static datahub.spark.converter.SparkStreamingEventToDatahub.*;
import static io.datahubproject.openlineage.converter.OpenLineageToDataHub.*;
import static io.datahubproject.openlineage.utils.DatahubUtils.*;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.UrnArray;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.JacksonDataTemplateCodec;
import com.linkedin.data.template.StringMap;
import com.linkedin.dataprocess.DataProcessInstanceRelationships;
import com.linkedin.domain.Domains;
import com.linkedin.mxe.MetadataChangeProposal;
import datahub.client.Emitter;
import datahub.client.file.FileEmitter;
import datahub.client.kafka.KafkaEmitter;
import datahub.client.rest.RestEmitter;
import datahub.client.s3.S3Emitter;
import datahub.event.EventFormatter;
import datahub.event.MetadataChangeProposalWrapper;
import datahub.spark.conf.FileDatahubEmitterConfig;
import datahub.spark.conf.KafkaDatahubEmitterConfig;
import datahub.spark.conf.RestDatahubEmitterConfig;
import datahub.spark.conf.S3DatahubEmitterConfig;
import datahub.spark.conf.SparkLineageConf;
import io.datahubproject.openlineage.converter.OpenLineageToDataHub;
import io.datahubproject.openlineage.dataset.DatahubDataset;
import io.datahubproject.openlineage.dataset.DatahubJob;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.spark.agent.EventEmitter;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.sql.streaming.StreamingQueryProgress;

@Slf4j
public class DatahubEventEmitter extends EventEmitter {
  private final AtomicBoolean streaming = new AtomicBoolean(false);

  private final List<DatahubJob> _datahubJobs = new LinkedList<>();
  private final Map<String, MetadataChangeProposalWrapper> schemaMap = new HashMap<>();
  private SparkLineageConf datahubConf;
  private static final int DEFAULT_TIMEOUT_SEC = 10;
  private final ObjectMapper objectMapper;
  private final JacksonDataTemplateCodec dataTemplateCodec;

  // Add listener fields
  public StreamingQueryListener microBatchListener;
  public StreamingQueryListener continuousListener;

  private final EventFormatter eventFormatter = new EventFormatter();

  // Add streaming event correlator
  private final StreamingEventCorrelator streamingCorrelator = new StreamingEventCorrelator();

  // Track the SparkSession for later use
  private SparkSession sparkSession;

  public DatahubEventEmitter(SparkOpenLineageConfig config, String applicationJobName)
      throws URISyntaxException {
    super(config, applicationJobName);
    objectMapper = new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    objectMapper
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());
    dataTemplateCodec = new JacksonDataTemplateCodec(objectMapper.getFactory());

    // Get current SparkSession
    try {
      this.sparkSession = SparkSession.active();
    } catch (Exception e) {
      log.warn("Could not get active SparkSession, streaming listeners will not be registered", e);
    }

    // Initialize listeners
    initStreamingListeners();
  }

  /** Initialize streaming query listeners. */
  private void initStreamingListeners() {
    log.info("Initializing streaming listeners");

    // Create simple listeners that forward events to the emit method
    this.microBatchListener =
        new StreamingQueryListener() {
          @Override
          public void onQueryStarted(StreamingQueryListener.QueryStartedEvent event) {
            log.info("Micro-batch query started: {}", event.id());
          }

          @Override
          public void onQueryProgress(StreamingQueryListener.QueryProgressEvent event) {
            log.info("Micro-batch query progress: {}", event.progress().id());
            try {
              emit(event.progress());
            } catch (Exception e) {
              log.error("Error emitting micro-batch progress event", e);
            }
          }

          @Override
          public void onQueryTerminated(StreamingQueryListener.QueryTerminatedEvent event) {
            log.info("Micro-batch query terminated: {}", event.id());
          }
        };

    this.continuousListener =
        new StreamingQueryListener() {
          @Override
          public void onQueryStarted(StreamingQueryListener.QueryStartedEvent event) {
            log.info("Continuous query started: {}", event.id());
          }

          @Override
          public void onQueryProgress(StreamingQueryListener.QueryProgressEvent event) {
            log.info("Continuous query progress: {}", event.progress().id());
            try {
              emit(event.progress());
            } catch (Exception e) {
              log.error("Error emitting continuous progress event", e);
            }
          }

          @Override
          public void onQueryTerminated(StreamingQueryListener.QueryTerminatedEvent event) {
            log.info("Continuous query terminated: {}", event.id());
          }
        };
  }

  /** Register streaming query listeners with the SparkSession. */
  private void registerStreamingListeners() {
    if (sparkSession == null) {
      log.warn("Cannot register streaming listeners: SparkSession is null");
      return;
    }

    log.info("Registering streaming query listeners with SparkSession");

    try {
      // Check if listeners are already registered
      AtomicBoolean alreadyRegistered = new AtomicBoolean(false);
      try {
        // Use reflection to access the internal listener list
        Field listenersField = sparkSession.streams().getClass().getDeclaredField("listeners");
        listenersField.setAccessible(true);
        Object listeners = listenersField.get(sparkSession.streams());

        if (listeners != null) {
          Method sizeMethod = listeners.getClass().getMethod("size");
          int size = (int) sizeMethod.invoke(listeners);
          log.info("Current streaming listeners count: {}", size);

          // If we already have listeners, we might be re-registering
          if (size > 0) {
            log.warn("Streaming listeners already exist, checking for duplicates");
            alreadyRegistered.set(true);
          }
        }
      } catch (Exception e) {
        log.debug("Could not check existing listeners: {}", e.getMessage());
      }

      if (!alreadyRegistered.get()) {
        // Register listeners with Spark's streaming context
        sparkSession.streams().addListener(microBatchListener);
        sparkSession.streams().addListener(continuousListener);
        log.info("Successfully registered streaming query listeners");
      } else {
        log.info("Skipping listener registration to avoid duplicates");
      }
    } catch (Exception e) {
      log.error("Error registering streaming query listeners", e);
    }
  }

  private Optional<Emitter> getEmitter() {
    Optional<Emitter> emitter = Optional.empty();
    if (datahubConf.getDatahubEmitterConfig() != null) {
      if (datahubConf.getDatahubEmitterConfig() instanceof RestDatahubEmitterConfig) {
        RestDatahubEmitterConfig datahubRestEmitterConfig =
            (RestDatahubEmitterConfig) datahubConf.getDatahubEmitterConfig();
        emitter = Optional.of(new RestEmitter(datahubRestEmitterConfig.getRestEmitterConfig()));
      } else if (datahubConf.getDatahubEmitterConfig() instanceof KafkaDatahubEmitterConfig) {
        KafkaDatahubEmitterConfig datahubKafkaEmitterConfig =
            (KafkaDatahubEmitterConfig) datahubConf.getDatahubEmitterConfig();
        try {
          emitter =
              Optional.of(
                  new KafkaEmitter(
                      datahubKafkaEmitterConfig.getKafkaEmitterConfig(),
                      datahubKafkaEmitterConfig.getMcpTopic()));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      } else if (datahubConf.getDatahubEmitterConfig() instanceof FileDatahubEmitterConfig) {
        FileDatahubEmitterConfig datahubFileEmitterConfig =
            (FileDatahubEmitterConfig) datahubConf.getDatahubEmitterConfig();
        emitter = Optional.of(new FileEmitter(datahubFileEmitterConfig.getFileEmitterConfig()));
      } else if (datahubConf.getDatahubEmitterConfig() instanceof S3DatahubEmitterConfig) {
        S3DatahubEmitterConfig datahubFileEmitterConfig =
            (S3DatahubEmitterConfig) datahubConf.getDatahubEmitterConfig();
        try {
          emitter = Optional.of(new S3Emitter(datahubFileEmitterConfig.getS3EmitterConfig()));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      } else {
        log.error(
            "DataHub Transport {} not recognized. DataHub Lineage emission will not work",
            RestDatahubEmitterConfig.class.getName());
      }
    } else {
      log.error("No Transport set. DataHub Lineage emission will not work");
    }
    return emitter;
  }

  public Optional<DatahubJob> convertOpenLineageRunEventToDatahubJob(OpenLineage.RunEvent event) {
    Optional<DatahubJob> datahubJob = Optional.empty();
    try {
      log.debug("Emitting lineage: {}", OpenLineageClientUtils.toJson(event));
      if (!isStreaming()) {
        datahubJob =
            Optional.ofNullable(convertRunEventToJob(event, datahubConf.getOpenLineageConf()));
        if (!datahubJob.isPresent()) {
          return datahubJob;
        }
        log.info(
            "Converted Job: {}, from {}", datahubJob.get(), OpenLineageClientUtils.toJson(event));
        _datahubJobs.add(datahubJob.get());
        return datahubJob;
      }
    } catch (IOException | URISyntaxException e) {
      throw new RuntimeException("Error: " + e.getMessage(), e);
    }
    return datahubJob;
  }

  public void emit(OpenLineage.RunEvent event) {
    long startTime = System.currentTimeMillis();
    // We have to serialize and deserialize the event to make sure the event is in the correct
    // format
    event = OpenLineageClientUtils.runEventFromJson(OpenLineageClientUtils.toJson(event));
    Optional<DatahubJob> job = convertOpenLineageRunEventToDatahubJob(event);
    if (!job.isPresent()) {
      return;
    }

    if (!datahubConf.getTags().isEmpty()) {
      GlobalTags tags = OpenLineageToDataHub.generateTags(datahubConf.getTags());
      job.get().setFlowGlobalTags(tags);
    }

    if (!datahubConf.getDomains().isEmpty()) {
      Domains domains = OpenLineageToDataHub.generateDomains(datahubConf.getDomains());
      job.get().setFlowDomains(domains);
    }

    if (!datahubConf.isCoalesceEnabled()) {
      log.info("Emitting lineage");
      try {
        emitMcps(job.get().toMcps(datahubConf.getOpenLineageConf()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      log.debug(
          "Emitting non-coalesced lineage completed successfully: {}",
          OpenLineageClientUtils.toJson(event));
    }
    if (datahubConf.isCoalesceEnabled() && datahubConf.isEmitCoalescePeriodically()) {
      log.info("Emitting coalesced lineage periodically");
      emitCoalesced();
      log.debug(
          "Emitting coalesced lineage completed successfully: {}",
          OpenLineageClientUtils.toJson(event));
    }
    log.debug(
        "Successfully completed emitting mcps in {} ms", System.currentTimeMillis() - startTime);
  }

  public void emitCoalesced() {
    if (!_datahubJobs.isEmpty()) {
      if (log.isDebugEnabled()) {
        log.debug("Emitting {} jobs", _datahubJobs.size());
      }
      try {
        emitMcps(generateCoalescedMcps());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      _datahubJobs.clear();
    }
  }

  public List<MetadataChangeProposal> generateCoalescedMcps() throws IOException {
    Map<String, DatahubJob> datahubJobs = new HashMap<>();
    for (DatahubJob job : _datahubJobs) {
      DatahubJob storedDatahubJob = datahubJobs.get(job.getJobUrn().toString());

      if (storedDatahubJob == null) {
        // First time we see this job, just store it
        datahubJobs.put(job.getJobUrn().toString(), job);
      } else {
        // We already have this job, merge properties
        mergeDatasets(storedDatahubJob.getInSet(), job.getInSet());
        mergeDatasets(storedDatahubJob.getOutSet(), job.getOutSet());
        mergeCustomProperties(job, storedDatahubJob);

        // Merge DataProcessInstance information if present
        mergeDataProcessInstance(job, storedDatahubJob);
      }
    }

    List<MetadataChangeProposal> mcps = new ArrayList<>();
    for (DatahubJob job : datahubJobs.values()) {
      mcps.addAll(job.toMcps(datahubConf.getOpenLineageConf()));
    }

    return mcps;
  }

  private static void mergeDatasets(
      Set<DatahubDataset> storedDatahubJob, Set<DatahubDataset> datahubJob) {
    for (DatahubDataset dataset : datahubJob) {
      Optional<DatahubDataset> existing =
          storedDatahubJob.stream()
              .filter(ds -> ds.getUrn().toString().equals(dataset.getUrn().toString()))
              .findFirst();
      if (!existing.isPresent()) {
        storedDatahubJob.add(dataset);
      }
    }
  }

  /**
   * Merge DataProcessInstance information from one job into another. This ensures we keep track of
   * all the process instances across runs.
   */
  private static void mergeDataProcessInstance(DatahubJob datahubJob, DatahubJob storedDatahubJob) {
    // If both jobs have DataProcessInstance information
    if (datahubJob.getDataProcessInstanceUrn() != null
        && storedDatahubJob.getDataProcessInstanceUrn() != null
        && datahubJob.getDataProcessInstanceRelationships() != null
        && storedDatahubJob.getDataProcessInstanceRelationships() != null) {

      // Get the relationships from both jobs
      DataProcessInstanceRelationships newRelationships =
          datahubJob.getDataProcessInstanceRelationships();
      DataProcessInstanceRelationships storedRelationships =
          storedDatahubJob.getDataProcessInstanceRelationships();

      // If both have upstream instances defined
      if (newRelationships.hasUpstreamInstances() && storedRelationships.hasUpstreamInstances()) {
        // Create a new merged array
        UrnArray mergedUpstreamInstances = new UrnArray();

        // Add all URNs from the stored job
        storedRelationships.getUpstreamInstances().forEach(mergedUpstreamInstances::add);

        // Add any new URNs that don't already exist
        newRelationships
            .getUpstreamInstances()
            .forEach(
                urn -> {
                  if (!mergedUpstreamInstances.contains(urn)) {
                    mergedUpstreamInstances.add(urn);
                  }
                });

        // Update the stored relationships with the merged array
        storedRelationships.setUpstreamInstances(mergedUpstreamInstances);
      } else if (newRelationships.hasUpstreamInstances()) {
        // If only the new job has upstream instances, use those
        storedRelationships.setUpstreamInstances(newRelationships.getUpstreamInstances());
      }
    } else if (datahubJob.getDataProcessInstanceUrn() != null
        && datahubJob.getDataProcessInstanceRelationships() != null) {
      // If only the new job has process instance info, copy it to the stored job
      storedDatahubJob.setDataProcessInstanceUrn(datahubJob.getDataProcessInstanceUrn());
      storedDatahubJob.setDataProcessInstanceRelationships(
          datahubJob.getDataProcessInstanceRelationships());
      storedDatahubJob.setDataProcessInstanceProperties(
          datahubJob.getDataProcessInstanceProperties());
      storedDatahubJob.setDataProcessInstanceRunEvent(datahubJob.getDataProcessInstanceRunEvent());
    }
  }

  private void mergeCustomProperties(DatahubJob datahubJob, DatahubJob storedDatahubJob) {

    if (datahubJob.getJobInfo() != null && storedDatahubJob.getJobInfo() != null) {
      StringMap customProperties = storedDatahubJob.getJobInfo().getCustomProperties();
      if (customProperties == null) {
        customProperties = new StringMap();
      }
      Map<String, String> mergedProperties = new HashMap<>(customProperties);
      if (datahubJob.getJobInfo().hasCustomProperties()) {
        mergedProperties.putAll(datahubJob.getJobInfo().getCustomProperties());
      }
      storedDatahubJob.getJobInfo().setCustomProperties(new StringMap(mergedProperties));
    }

    if (datahubJob.getDataFlowInfo() != null && storedDatahubJob.getDataFlowInfo() != null) {
      StringMap customProperties = storedDatahubJob.getDataFlowInfo().getCustomProperties();
      if (customProperties == null) {
        customProperties = new StringMap();
      }
      Map<String, String> mergedProperties = new HashMap<>(customProperties);
      if (datahubJob.getDataFlowInfo().hasCustomProperties()) {
        mergedProperties.putAll(datahubJob.getDataFlowInfo().getCustomProperties());
      }

      if (datahubConf.getSparkAppContext() != null) {
        if (datahubConf.getSparkAppContext().getSparkUser() != null) {
          mergedProperties.put("sparkUser", datahubConf.getSparkAppContext().getSparkUser());
        }

        if (datahubConf.getSparkAppContext().getAppId() != null) {
          mergedProperties.put("appId", datahubConf.getSparkAppContext().getAppId());
        }

        if (datahubConf.getSparkAppContext().getDatabricksTags() != null) {
          mergedProperties.putAll(datahubConf.getSparkAppContext().getDatabricksTags());
        }
      }
      storedDatahubJob.getDataFlowInfo().setCustomProperties(new StringMap(mergedProperties));
    }
  }

  /** Process a streaming query progress event */
  public void emit(StreamingQueryProgress event) throws URISyntaxException {
    if (datahubConf == null) {
      log.warn("DataHub configuration is not set, skipping streaming event emission");
      return;
    }

    try {
      log.info("Processing streaming query progress: {}", event.id());

      // Use the StreamingEventCorrelator to handle correlation between events
      List<MetadataChangeProposalWrapper> mcps =
          streamingCorrelator.processEvent(event, datahubConf, schemaMap);

      List<MetadataChangeProposal> formattedMcps = new ArrayList<>();
      for (MetadataChangeProposalWrapper mcp : mcps) {
        try {
          formattedMcps.add(eventFormatter.convert(mcp));
        } catch (IOException e) {
          log.error("Failed to convert metadata change proposal: {}", e.getMessage());
        }
      }

      emitMcps(formattedMcps);
      log.info("Successfully emitted streaming query progress event for {}", event.id());
    } catch (Exception e) {
      log.error("Error processing streaming query progress event", e);
    }
  }

  /** Process a microbatch start event */
  public void processMicroBatchStart(String queryId, String logMessage) {
    if (datahubConf == null) {
      log.warn("DataHub configuration is not set, skipping microbatch event");
      return;
    }

    try {
      log.info("Processing microbatch start for query ID: {}", queryId);
      streamingCorrelator.recordMicroBatchStart(queryId, logMessage);
    } catch (Exception e) {
      log.error("Error processing microbatch start event", e);
    }
  }

  /** Process a microbatch commit event */
  public void processMicroBatchCommit(
      String queryId, Map<String, String> metadata, String logMessage) {
    if (datahubConf == null) {
      log.warn("DataHub configuration is not set, skipping microbatch commit event");
      return;
    }

    try {
      log.info("Processing microbatch commit for query ID: {}, metadata: {}", queryId, metadata);
      streamingCorrelator.recordMicroBatchCommit(queryId, metadata, logMessage);
    } catch (Exception e) {
      log.error("Error processing microbatch commit event", e);
    }
  }

  /** Process a delta sink write event */
  public void processDeltaSinkWrite(
      String queryId, Map<String, String> metadata, String logMessage) {
    if (datahubConf == null) {
      log.warn("DataHub configuration is not set, skipping delta sink write event");
      return;
    }

    try {
      log.info("Processing delta sink write for query ID: {}, metadata: {}", queryId, metadata);
      streamingCorrelator.recordDeltaSinkWrite(queryId, metadata, logMessage);
    } catch (Exception e) {
      log.error("Error processing delta sink write event", e);
    }
  }

  /**
   * Process a logical plan event
   *
   * @param queryId the query ID
   * @param metadata metadata about the logical plan
   * @param logMessage the original log message
   */
  public void processMicroBatchLogicalPlan(
      String queryId, Map<String, String> metadata, String logMessage) {
    if (datahubConf == null) {
      log.warn("DataHub configuration is not set, skipping logical plan event");
      return;
    }

    try {
      log.info("Processing logical plan for query ID: {}", queryId);
      streamingCorrelator.recordLogicalPlan(queryId, metadata, logMessage);
    } catch (Exception e) {
      log.error("Error processing logical plan event", e);
    }
  }

  /**
   * Process a progress report event
   *
   * @param queryId the query ID
   * @param metadata metadata about the progress report
   * @param logMessage the original log message
   */
  public void processMicroBatchProgress(
      String queryId, Map<String, String> metadata, String logMessage) {
    if (datahubConf == null) {
      log.warn("DataHub configuration is not set, skipping progress report event");
      return;
    }

    try {
      log.info("Processing progress report for query ID: {}", queryId);
      streamingCorrelator.recordProgressReport(queryId, metadata, logMessage);
    } catch (Exception e) {
      log.error("Error processing progress report event", e);
    }
  }

  /**
   * Process other interesting messages
   *
   * @param queryId the query ID
   * @param metadata metadata about the message
   * @param logMessage the original log message
   */
  public void processInterestingMessage(
      String queryId, Map<String, String> metadata, String logMessage) {
    if (datahubConf == null) {
      log.warn("DataHub configuration is not set, skipping interesting message");
      return;
    }

    try {
      log.info("Processing interesting message for query ID: {}", queryId);
      streamingCorrelator.recordInterestingMessage(queryId, metadata, logMessage);
    } catch (Exception e) {
      log.error("Error processing interesting message", e);
    }
  }

  /**
   * Process a catalog table or output information message
   *
   * @param queryId the query ID
   * @param metadata metadata about the output table
   * @param logMessage the original log message
   */
  public void processOutputTable(String queryId, Map<String, String> metadata, String logMessage) {
    if (datahubConf == null) {
      log.warn("DataHub configuration is not set, skipping output table processing");
      return;
    }

    try {
      log.info("Processing output table information for query ID: {}", queryId);

      // Extract relevant details from metadata
      String type = metadata.get("type");

      if ("catalogTable".equals(type)) {
        // Handle catalog table information from MicroBatchExecution logs
        String catalogName = metadata.get("catalogName");
        String databaseName = metadata.get("databaseName");
        String tableName = metadata.get("tableName");
        String location = metadata.get("location");

        log.info(
            "Found catalog table: {}.{}.{} at location {}",
            catalogName,
            databaseName,
            tableName,
            location);

        streamingCorrelator.recordCatalogTable(queryId, metadata);
      } else if ("foreachBatchSink".equals(type)) {
        // Handle foreachBatchSink information from ProgressReporter logs
        // This is where we link the streaming query to an output table
        String progressReport = metadata.get("progressReport");

        log.info("Found ForeachBatchSink with progress report for query {}", queryId);
        streamingCorrelator.recordForeachBatchSink(queryId, progressReport);
      }
    } catch (Exception e) {
      log.error("Error processing output table: {}", e.getMessage(), e);
    }
  }

  /**
   * Process a logical plan log message from a streaming query.
   *
   * @param queryId the query ID
   * @param metadata metadata extracted from the log
   * @param logMessage the original log message
   */
  public void processLogicalPlan(String queryId, Map<String, String> metadata, String logMessage) {
    if (streamingCorrelator != null) {
      streamingCorrelator.recordLogicalPlan(queryId, metadata, logMessage);
    }
  }

  /**
   * Process a progress report log message from a streaming query.
   *
   * @param queryId the query ID
   * @param metadata metadata extracted from the log
   * @param logMessage the original log message
   */
  public void processProgressReport(
      String queryId, Map<String, String> metadata, String logMessage) {
    if (streamingCorrelator != null) {
      streamingCorrelator.recordProgressReport(queryId, metadata, logMessage);
    }
  }

  /**
   * Process sink information captured from StreamingQueryListener. This information is used to
   * better associate streaming jobs with Hive tables.
   *
   * @param queryId the streaming query ID
   * @param metadata sink metadata containing sink description and other details
   */
  public void processSinkInformation(String queryId, Map<String, String> metadata) {
    try {
      log.info("Processing sink information for query ID: {}", queryId);

      if (metadata != null && metadata.containsKey("sinkDescription")) {
        String sinkDescription = metadata.get("sinkDescription");
        log.debug("Sink description: {}", sinkDescription);

        // Try multiple patterns to extract table information from various sink formats

        // Pattern 1: Full catalog.database.table format with brackets
        // Example: "HiveTableSink[jonnydq.default.stream_orders]"
        Pattern fullTablePattern = Pattern.compile(".*\\[(.*?)\\.(.*?)\\.(.*?)\\].*");
        Matcher matcher = fullTablePattern.matcher(sinkDescription);

        if (matcher.find() && matcher.groupCount() >= 3) {
          String catalog = matcher.group(1);
          String database = matcher.group(2);
          String table = matcher.group(3);

          log.info(
              "Extracted table info (full format) - Catalog: {}, Database: {}, Table: {}",
              catalog,
              database,
              table);

          // Create a metadata map with the extracted information
          Map<String, String> tableMetadata = new HashMap<>();
          tableMetadata.put("type", "catalogTable");
          tableMetadata.put("catalogName", catalog);
          tableMetadata.put("databaseName", database);
          tableMetadata.put("tableName", table);

          // Pass to the StreamingEventCorrelator
          if (this.streamingCorrelator != null) {
            this.streamingCorrelator.recordCatalogTable(queryId, tableMetadata);
            log.info("Recorded catalog table for streaming query: {}", queryId);
          } else {
            log.warn("StreamingEventCorrelator is null, cannot record catalog table");
          }
          return;
        }

        // Pattern 2: Database.table format with brackets
        // Example: "JDBCSink[default.stream_orders]"
        Pattern dbTablePattern = Pattern.compile(".*\\[(.*?)\\.(.*?)\\].*");
        matcher = dbTablePattern.matcher(sinkDescription);

        if (matcher.find() && matcher.groupCount() >= 2) {
          String database = matcher.group(1);
          String table = matcher.group(2);

          log.info(
              "Extracted table info (db.table format) - Database: {}, Table: {}", database, table);

          // Create a metadata map with the extracted information
          Map<String, String> tableMetadata = new HashMap<>();
          tableMetadata.put("type", "catalogTable");
          tableMetadata.put("catalogName", "jonnydq"); // Use the specified catalog
          tableMetadata.put("databaseName", database);
          tableMetadata.put("tableName", table);

          // Pass to the StreamingEventCorrelator
          if (this.streamingCorrelator != null) {
            this.streamingCorrelator.recordCatalogTable(queryId, tableMetadata);
            log.info("Recorded catalog table for streaming query: {}", queryId);
          } else {
            log.warn("StreamingEventCorrelator is null, cannot record catalog table");
          }
          return;
        }

        // Pattern 3: Look for format with "database=X, table=Y" or similar
        Pattern keyValuePattern =
            Pattern.compile(
                ".*(?:database|db)=\\s*['\"](.*?)['\"].*(?:table|tbl)=\\s*['\"](.*?)['\"].*",
                Pattern.CASE_INSENSITIVE);
        matcher = keyValuePattern.matcher(sinkDescription);

        if (matcher.find() && matcher.groupCount() >= 2) {
          String database = matcher.group(1);
          String table = matcher.group(2);

          log.info(
              "Extracted table info (key-value format) - Database: {}, Table: {}", database, table);

          // Create a metadata map with the extracted information
          Map<String, String> tableMetadata = new HashMap<>();
          tableMetadata.put("type", "catalogTable");
          tableMetadata.put("catalogName", "jonnydq"); // Use the specified catalog
          tableMetadata.put("databaseName", database);
          tableMetadata.put("tableName", table);

          // Pass to the StreamingEventCorrelator
          if (this.streamingCorrelator != null) {
            this.streamingCorrelator.recordCatalogTable(queryId, tableMetadata);
            log.info("Recorded catalog table for streaming query: {}", queryId);
          } else {
            log.warn("StreamingEventCorrelator is null, cannot record catalog table");
          }
          return;
        }

        // Pattern 4: Just a table name in brackets
        // Example: "Sink[stream_orders]"
        Pattern singleTablePattern = Pattern.compile(".*\\[([^\\.]*)\\].*");
        matcher = singleTablePattern.matcher(sinkDescription);

        if (matcher.find() && matcher.groupCount() >= 1) {
          String table = matcher.group(1);

          // Only process if it doesn't contain special characters that would indicate it's not a
          // table
          if (!table.contains("/") && !table.contains("\\") && !table.contains(":")) {
            log.info("Extracted table info (single table format) - Table: {}", table);

            // Create a metadata map with the extracted information
            Map<String, String> tableMetadata = new HashMap<>();
            tableMetadata.put("type", "catalogTable");
            tableMetadata.put("catalogName", "jonnydq"); // Use the specified catalog
            tableMetadata.put("databaseName", "default"); // Assume default database
            tableMetadata.put("tableName", table);

            // Pass to the StreamingEventCorrelator
            if (this.streamingCorrelator != null) {
              this.streamingCorrelator.recordCatalogTable(queryId, tableMetadata);
              log.info("Recorded catalog table for streaming query: {}", queryId);
            } else {
              log.warn("StreamingEventCorrelator is null, cannot record catalog table");
            }
            return;
          }
        }

        log.warn("Could not extract table information from sink description: {}", sinkDescription);
      } else {
        log.warn("Missing sink description in metadata");
      }
    } catch (Exception e) {
      log.error("Error processing sink information: {}", e.getMessage(), e);
    }
  }

  protected void emitMcps(List<MetadataChangeProposal> mcps) {
    Optional<Emitter> emitter = getEmitter();
    if (emitter.isPresent()) {
      mcps.stream()
          .map(
              mcp -> {
                try {
                  if (this.datahubConf.isLogMcps()) {
                    DataMap map = mcp.data();
                    String serializedMCP = dataTemplateCodec.mapToString(map);
                    log.info("emitting mcpw: {}", serializedMCP);
                  } else {
                    log.info(
                        "emitting aspect: {} for urn: {}", mcp.getAspectName(), mcp.getEntityUrn());
                  }
                  return emitter.get().emit(mcp);
                } catch (IOException ioException) {
                  log.error("Failed to emit metadata to DataHub", ioException);
                  return null;
                }
              })
          .filter(Objects::nonNull)
          .collect(Collectors.toList())
          .forEach(
              future -> {
                try {
                  log.info(future.get(DEFAULT_TIMEOUT_SEC, TimeUnit.SECONDS).toString());
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                  // log error, but don't impact thread
                  log.error("Failed to emit metadata to DataHub", e);
                }
              });
      try {
        emitter.get().close();
      } catch (IOException e) {
        log.error("Issue while closing emitter" + e);
      }
    }
  }

  public void setConfig(SparkLineageConf sparkConfig) {
    this.datahubConf = sparkConfig;

    // Update the streaming correlator config
    if (streamingCorrelator != null) {
      streamingCorrelator.setConfig(sparkConfig);
    }

    // Register the streaming listeners
    registerStreamingListeners();

    // Install the MicroBatchLogInterceptor to capture log events from streaming queries
    boolean interceptorInstalled = MicroBatchLogInterceptor.install(this);
    if (interceptorInstalled) {
      log.info("Successfully installed MicroBatchLogInterceptor");
    } else {
      log.warn(
          "Failed to install MicroBatchLogInterceptor - some streaming lineage features may be limited");
    }
  }

  public boolean isStreaming() {
    return streaming.get();
  }

  public void setStreaming(boolean enabled) {
    streaming.set(enabled);
  }

  /**
   * Flush any pending streaming events for a query
   *
   * @param queryId the query ID
   */
  public void flushStreamingEvents(String queryId) {
    if (datahubConf == null) {
      log.warn("DataHub configuration is not set, skipping flush");
      return;
    }

    try {
      log.info("Flushing streaming events for query ID: {}", queryId);

      // Generate lineage from accumulated data
      List<MetadataChangeProposalWrapper> mcps =
          streamingCorrelator.generateLineageFromStreamingData(queryId);

      if (mcps.isEmpty()) {
        log.warn("No MCPs generated for query {}", queryId);
        return;
      }

      log.info("Generated {} MCPs for query {}", mcps.size(), queryId);

      // Emit the MCPs
      Optional<Emitter> emitter = getEmitter();
      if (emitter.isPresent()) {
        for (MetadataChangeProposalWrapper mcp : mcps) {
          try {
            log.debug("Emitting MCP: {}", mcp);
            emitter.get().emit(eventFormatter.convert(mcp));
          } catch (Exception e) {
            log.error("Error emitting MCP: {}", e.getMessage(), e);
          }
        }
      } else {
        log.warn("No emitter available in configuration, cannot send MCPs");
      }
    } catch (Exception e) {
      log.error("Error flushing streaming events: {}", e.getMessage(), e);
    }
  }
}
