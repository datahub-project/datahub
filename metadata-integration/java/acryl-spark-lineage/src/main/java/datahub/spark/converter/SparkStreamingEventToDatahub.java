package datahub.spark.converter;

import static io.datahubproject.openlineage.utils.DatahubUtils.*;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.linkedin.common.DatasetUrnArray;
import com.linkedin.common.TimeStamp;
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
import io.datahubproject.openlineage.dataset.HdfsPathDataset;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
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

    DataFlowInfo dataFlowInfo = new DataFlowInfo();
    dataFlowInfo.setName(conf.getOpenLineageConf().getPipelineName());
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

    DataFlowUrn flowUrn =
        flowUrn(
            conf.getOpenLineageConf().getPlatformInstance(),
            conf.getOpenLineageConf().getPipelineName());
    MetadataChangeProposalWrapper dataflowMcp =
        MetadataChangeProposalWrapper.create(
            b -> b.entityType("dataFlow").entityUrn(flowUrn).upsert().aspect(dataFlowInfo));
    mcps.add(dataflowMcp);

    DataJobInfo dataJobInfo = new DataJobInfo();
    dataJobInfo.setName(conf.getOpenLineageConf().getPipelineName());
    dataJobInfo.setType(DataJobInfo.Type.create("SPARK"));

    StringMap jobCustomProperties = new StringMap();
    jobCustomProperties.put("batchId", Long.toString(event.batchId()));
    jobCustomProperties.put("inputRowsPerSecond", Double.toString(event.inputRowsPerSecond()));
    jobCustomProperties.put(
        "processedRowsPerSecond", Double.toString(event.processedRowsPerSecond()));
    jobCustomProperties.put("numInputRows", Long.toString(event.numInputRows()));
    dataJobInfo.setCustomProperties(jobCustomProperties);

    DataJobUrn jobUrn = jobUrn(flowUrn, conf.getOpenLineageConf().getPipelineName());
    MetadataChangeProposalWrapper dataJobMcp =
        MetadataChangeProposalWrapper.create(
            b -> b.entityType("dataJob").entityUrn(jobUrn).upsert().aspect(dataJobInfo));
    mcps.add(dataJobMcp);

    DataJobInputOutput dataJobInputOutput = new DataJobInputOutput();

    JsonElement root = new JsonParser().parse(event.json());
    DatasetUrnArray inputDatasetUrnArray = new DatasetUrnArray();
    for (JsonElement source : root.getAsJsonObject().get("sources").getAsJsonArray()) {
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

    DatasetUrnArray outputDatasetUrnArray = new DatasetUrnArray();
    String sinkDescription =
        root.getAsJsonObject().get("sink").getAsJsonObject().get("description").getAsString();
    Optional<DatasetUrn> urn =
        SparkStreamingEventToDatahub.generateUrnFromStreamingDescription(sinkDescription, conf);
    if (urn.isPresent()) {
      MetadataChangeProposalWrapper datasetMcp = generateDatasetMcp(urn.get());
      outputDatasetUrnArray.add(urn.get());
      mcps.add(datasetMcp);
      if (conf.getOpenLineageConf().isIncludeSchemaMetadata()
          && schemaMap.containsKey(urn.get().toString())) {
        mcps.add(schemaMap.get(urn.get().toString()));
      }
    }

    dataJobInputOutput.setInputDatasets(inputDatasetUrnArray);
    dataJobInputOutput.setOutputDatasets(outputDatasetUrnArray);

    MetadataChangeProposalWrapper inputOutputMcp =
        MetadataChangeProposalWrapper.create(
            b -> b.entityType("dataJob").entityUrn(jobUrn).upsert().aspect(dataJobInputOutput));

    mcps.add(inputOutputMcp);
    return (mcps);
  }

  public static Optional<DatasetUrn> generateUrnFromStreamingDescription(
      String description, SparkLineageConf sparkLineageConf) {
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
          return Optional.empty();
        } catch (URISyntaxException e) {
          log.error("Failed to parse path {}", path, e);
          return Optional.empty();
        }
      }
      return Optional.of(
          new DatasetUrn(
              new DataPlatformUrn(platform),
              path,
              sparkLineageConf.getOpenLineageConf().getFabricType()));
    } else {
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
}
