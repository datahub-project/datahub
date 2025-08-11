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
import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.JacksonDataTemplateCodec;
import com.linkedin.data.template.StringMap;
import com.linkedin.dataprocess.DataProcessInstanceRelationships;
import com.linkedin.dataprocess.RunResultType;
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
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
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

  private final EventFormatter eventFormatter = new EventFormatter();

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

    if (isStreaming()) {
      log.info("Streaming mode is enabled. Skipping lineage emission.");
      return;
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
          "Collecting coalesced lineage periodically completed successfully: {}",
          OpenLineageClientUtils.toJson(event));
    }
    long elapsedTime = System.currentTimeMillis() - startTime;
    log.info("Collecting lineage successfully in {} ms", elapsedTime);
  }

  public void emitCoalesced() {
    long startTime = System.currentTimeMillis();

    if (isStreaming()) {
      log.info("Streaming mode is enabled. Skipping lineage emission.");
      return;
    }

    if (datahubConf.isCoalesceEnabled()) {
      List<MetadataChangeProposal> mcps = generateCoalescedMcps();
      log.info("Emitting Coalesced lineage completed successfully");
      emitMcps(mcps);
    }
    long elapsedTime = System.currentTimeMillis() - startTime;
    log.info("Emitting coalesced lineage completed in {} ms", elapsedTime);
  }

  public List<MetadataChangeProposal> generateCoalescedMcps() {
    List<MetadataChangeProposal> mcps = new ArrayList<>();

    if (_datahubJobs.isEmpty()) {
      log.warn("No lineage events to emit. Maybe the spark job finished prematurely?");
      return mcps;
    }

    DatahubJob datahubJob = DatahubJob.builder().build();
    AtomicLong minStartTime = new AtomicLong(Long.MAX_VALUE);
    AtomicLong maxEndTime = new AtomicLong();
    _datahubJobs.forEach(
        storedDatahubJob -> {
          log.info("Merging job stored job {} with {}", storedDatahubJob, datahubJob);
          DataJobUrn jobUrn =
              jobUrn(
                  storedDatahubJob.getFlowUrn(), storedDatahubJob.getFlowUrn().getFlowIdEntity());
          datahubJob.setJobUrn(jobUrn);
          datahubJob.setFlowUrn(storedDatahubJob.getFlowUrn());
          datahubJob.setFlowPlatformInstance(storedDatahubJob.getFlowPlatformInstance());
          if ((datahubJob.getJobInfo() == null) && (storedDatahubJob.getJobInfo() != null)) {
            datahubJob.setJobInfo(storedDatahubJob.getJobInfo());
            datahubJob.getJobInfo().setName(storedDatahubJob.getFlowUrn().getFlowIdEntity());
          }
          if (storedDatahubJob.getJobInfo() != null
              && storedDatahubJob.getJobInfo().getCustomProperties() != null) {
            if (datahubJob.getJobInfo().getCustomProperties() == null) {
              datahubJob
                  .getJobInfo()
                  .setCustomProperties(storedDatahubJob.getJobInfo().getCustomProperties());
            } else {
              Map<String, String> mergedProperties =
                  Stream.of(
                          datahubJob.getJobInfo().getCustomProperties(),
                          storedDatahubJob.getJobInfo().getCustomProperties())
                      .flatMap(map -> map.entrySet().stream())
                      .collect(
                          Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v1));
              datahubJob.getJobInfo().setCustomProperties(new StringMap(mergedProperties));
            }
          }
          if (datahubJob.getDataFlowInfo() == null) {
            datahubJob.setDataFlowInfo(storedDatahubJob.getDataFlowInfo());
          }

          if (storedDatahubJob.getStartTime() < minStartTime.get()) {
            minStartTime.set(storedDatahubJob.getStartTime());
          }

          if (storedDatahubJob.getEndTime() > maxEndTime.get()) {
            maxEndTime.set(storedDatahubJob.getEndTime());
          }

          mergeDatasets(storedDatahubJob.getOutSet(), datahubJob.getOutSet());

          mergeDatasets(storedDatahubJob.getInSet(), datahubJob.getInSet());

          mergeDataProcessInstance(datahubJob, storedDatahubJob);

          mergeCustomProperties(datahubJob, storedDatahubJob);
        });

    datahubJob.setStartTime(minStartTime.get());
    datahubJob.setEndTime(maxEndTime.get());
    if (!datahubConf.getTags().isEmpty()) {
      GlobalTags tags = OpenLineageToDataHub.generateTags(datahubConf.getTags());
      datahubJob.setFlowGlobalTags(tags);
    }

    if (!datahubConf.getDomains().isEmpty()) {
      Domains domains = OpenLineageToDataHub.generateDomains(datahubConf.getDomains());
      datahubJob.setFlowDomains(domains);
    }
    try {
      if (datahubConf.getOpenLineageConf().getParentJobUrn() != null) {
        datahubJob.getParentJobs().add(datahubConf.getOpenLineageConf().getParentJobUrn());
      }
    } catch (ClassCastException e) {
      log.warn(
          datahubConf.getOpenLineageConf().getParentJobUrn()
              + " is not a valid Datajob URN. Skipping setting up upstream job.");
    }

    log.info("Generating MCPs for job: {}", datahubJob);
    try {
      return datahubJob.toMcps(datahubConf.getOpenLineageConf());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void mergeDatasets(
      Set<DatahubDataset> storedDatahubJob, Set<DatahubDataset> datahubJob) {
    for (DatahubDataset dataset : storedDatahubJob) {
      Optional<DatahubDataset> oldDataset =
          datahubJob.stream().filter(ds -> ds.getUrn().equals(dataset.getUrn())).findFirst();
      if (oldDataset.isPresent()) {
        if (dataset.getSchemaMetadata() != null) {
          oldDataset.get().setSchemaMetadata(dataset.getSchemaMetadata());
        }
        if (dataset.getLineage() != null) {
          oldDataset.get().setLineage(dataset.getLineage());
        }
      } else {
        datahubJob.add(dataset);
      }
    }
  }

  private static void mergeDataProcessInstance(DatahubJob datahubJob, DatahubJob storedDatahubJob) {
    // To merge multiple events into one DataProcess we should do the following steps:
    // 1. A run is only in SUCCESS if all the process instance status are SUCCESS
    // 2. A run is in failed state if any of the run events is in FAILED/UNKNOWN/SKIPPED state
    //
    // We should set as id the first event to make sure it won't change if we ingest periodically
    // coalesced data
    // Todo: Status can be SUCCESS only if all the process instance status are SUCCESS
    if (datahubJob.getDataProcessInstanceUrn() == null) {
      datahubJob.setDataProcessInstanceUrn(storedDatahubJob.getDataProcessInstanceUrn());
    }

    if (storedDatahubJob.getEventTime() > datahubJob.getEventTime()) {
      datahubJob.setEventTime(storedDatahubJob.getEventTime());
      datahubJob.setDataProcessInstanceProperties(
          storedDatahubJob.getDataProcessInstanceProperties());
      DataProcessInstanceRelationships dataProcessInstanceRelationships =
          new DataProcessInstanceRelationships();
      dataProcessInstanceRelationships.setParentTemplate(datahubJob.getJobUrn());
      dataProcessInstanceRelationships.setUpstreamInstances(new UrnArray());
      datahubJob.setDataProcessInstanceRelationships(dataProcessInstanceRelationships);
    }
    log.info("DataProcessInstanceRunEvent: {}", storedDatahubJob.getDataProcessInstanceRunEvent());
    if ((storedDatahubJob.getDataProcessInstanceRunEvent() != null)
        && (storedDatahubJob.getDataProcessInstanceRunEvent().getResult() != null)) {
      RunResultType result =
          storedDatahubJob.getDataProcessInstanceRunEvent().getResult().getType();
      if (datahubJob.getDataProcessInstanceRunEvent() == null) {
        datahubJob.setDataProcessInstanceRunEvent(
            storedDatahubJob.getDataProcessInstanceRunEvent());
      } else if (result == RunResultType.FAILURE) {
        datahubJob.setDataProcessInstanceRunEvent(
            storedDatahubJob.getDataProcessInstanceRunEvent());
      }
    }
    log.info("DataProcessInstanceRunEvent: {}", datahubJob.getDataProcessInstanceRunEvent());
  }

  private void mergeCustomProperties(DatahubJob datahubJob, DatahubJob storedDatahubJob) {
    if (storedDatahubJob.getDataFlowInfo().getCustomProperties() != null) {
      if (datahubJob.getDataFlowInfo().getCustomProperties() == null) {
        datahubJob
            .getDataFlowInfo()
            .setCustomProperties(storedDatahubJob.getDataFlowInfo().getCustomProperties());
      } else {
        Map<String, String> mergedProperties =
            Stream.of(
                    datahubJob.getDataFlowInfo().getCustomProperties(),
                    storedDatahubJob.getDataFlowInfo().getCustomProperties())
                .flatMap(map -> map.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v1));
        mergedProperties.put("finishedAt", ZonedDateTime.now(ZoneOffset.UTC).toString());

        if (datahubConf.getSparkAppContext() != null) {
          if (datahubConf.getSparkAppContext().getStartTime() != null) {
            mergedProperties.put(
                "startedAt",
                ZonedDateTime.ofInstant(
                        Instant.ofEpochMilli(datahubConf.getSparkAppContext().getStartTime()),
                        ZoneOffset.UTC)
                    .toString());
          }
          if (datahubConf.getSparkAppContext().getAppAttemptId() != null) {
            mergedProperties.put("attemptId", datahubConf.getSparkAppContext().getAppAttemptId());
          }
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
        datahubJob.getDataFlowInfo().setCustomProperties(new StringMap(mergedProperties));
      }
    }
  }

  public void emit(StreamingQueryProgress event) throws URISyntaxException {
    List<MetadataChangeProposal> mcps = new ArrayList<>();
    for (MetadataChangeProposalWrapper mcpw :
        generateMcpFromStreamingProgressEvent(event, datahubConf, schemaMap)) {
      try {
        mcps.add(eventFormatter.convert(mcpw));
      } catch (IOException e) {
        log.error("Failed to convert mcpw to mcp", e);
      }
    }
    emitMcps(mcps);
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
  }

  public boolean isStreaming() {
    return streaming.get();
  }

  public void setStreaming(boolean enabled) {
    streaming.set(enabled);
  }
}
