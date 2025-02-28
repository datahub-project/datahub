package io.datahubproject.openlineage.dataset;

import static io.datahubproject.openlineage.converter.OpenLineageToDataHub.*;

import com.linkedin.common.DataJobUrnArray;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.DatasetUrnArray;
import com.linkedin.common.Edge;
import com.linkedin.common.EdgeArray;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.DataTemplate;
import com.linkedin.data.template.StringMap;
import com.linkedin.datajob.DataFlowInfo;
import com.linkedin.datajob.DataJobInfo;
import com.linkedin.datajob.DataJobInputOutput;
import com.linkedin.dataprocess.DataProcessInstanceInput;
import com.linkedin.dataprocess.DataProcessInstanceOutput;
import com.linkedin.dataprocess.DataProcessInstanceProperties;
import com.linkedin.dataprocess.DataProcessInstanceRelationships;
import com.linkedin.dataprocess.DataProcessInstanceRunEvent;
import com.linkedin.dataset.FineGrainedLineage;
import com.linkedin.dataset.FineGrainedLineageArray;
import com.linkedin.dataset.Upstream;
import com.linkedin.dataset.UpstreamArray;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.domain.Domains;
import com.linkedin.metadata.aspect.patch.builder.DataJobInputOutputPatchBuilder;
import com.linkedin.metadata.aspect.patch.builder.GlobalTagsPatchBuilder;
import com.linkedin.metadata.aspect.patch.builder.UpstreamLineagePatchBuilder;
import com.linkedin.metadata.key.DatasetKey;
import com.linkedin.mxe.MetadataChangeProposal;
import datahub.event.EventFormatter;
import datahub.event.MetadataChangeProposalWrapper;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

@EqualsAndHashCode
@Getter
@Setter
@Builder
@ToString
@Slf4j
public class DatahubJob {
  public static final String DATASET_ENTITY_TYPE = "dataset";
  public static final String DATA_FLOW_ENTITY_TYPE = "dataFlow";
  public static final String DATA_PROCESS_INSTANCE_ENTITY_TYPE = "dataProcessInstance";
  public static final String DATAFLOW_ENTITY_TYPE = "dataflow";
  public static final String DATAJOB_ENTITY_TYPE = "dataJob";
  DataFlowUrn flowUrn;
  DataFlowInfo dataFlowInfo;
  DataJobUrn jobUrn;
  DataJobInfo jobInfo;
  Ownership flowOwnership;
  GlobalTags flowGlobalTags;
  Domains flowDomains;
  DataPlatformInstance flowPlatformInstance;
  DataProcessInstanceRunEvent dataProcessInstanceRunEvent;
  DataProcessInstanceProperties dataProcessInstanceProperties;
  DataProcessInstanceRelationships dataProcessInstanceRelationships;
  Urn dataProcessInstanceUrn;

  final Set<DatahubDataset> inSet = new TreeSet<>(new DataSetComparator());
  final Set<DatahubDataset> outSet = new TreeSet<>(new DataSetComparator());
  final Set<DataJobUrn> parentJobs = new TreeSet<>(new DataJobUrnComparator());
  final Map<String, String> datasetProperties = new HashMap<>();
  long startTime;
  long endTime;
  long eventTime;
  final EventFormatter eventFormatter = new EventFormatter();

  public static MetadataChangeProposalWrapper materializeDataset(DatasetUrn datasetUrn) {
    DatasetKey datasetAspect = new DatasetKey().setOrigin(datasetUrn.getOriginEntity());
    datasetAspect
        .setName(datasetUrn.getDatasetNameEntity())
        .setPlatform(new DataPlatformUrn(datasetUrn.getPlatformEntity().getPlatformNameEntity()));

    return MetadataChangeProposalWrapper.create(
        b ->
            b.entityType(DATASET_ENTITY_TYPE).entityUrn(datasetUrn).upsert().aspect(datasetAspect));
  }

  public List<MetadataChangeProposal> toMcps(DatahubOpenlineageConfig config) throws IOException {
    List<MetadataChangeProposal> mcps = new ArrayList<>();

    // Generate and add DataFlow Aspect
    log.info("Generating MCPs for job: {}", jobUrn);
    addAspectToMcps(flowUrn, DATA_FLOW_ENTITY_TYPE, dataFlowInfo, mcps);
    generateStatus(flowUrn, DATA_FLOW_ENTITY_TYPE, mcps);

    // Generate and add PlatformInstance Aspect
    if (flowPlatformInstance != null) {
      addAspectToMcps(flowUrn, DATA_FLOW_ENTITY_TYPE, flowPlatformInstance, mcps);
    }

    // Generate and add Properties Aspect
    StringMap customProperties = new StringMap();
    if (!jobInfo.getCustomProperties().isEmpty()) {
      customProperties.putAll(jobInfo.getCustomProperties());
    }

    if (startTime > 0) {
      customProperties.put("startTime", String.valueOf(Instant.ofEpochMilli(startTime)));
    }

    if (endTime > 0) {
      customProperties.put("endTime", String.valueOf(Instant.ofEpochMilli(endTime)));
    }
    log.info("Setting custom properties for job: {}", jobUrn);
    jobInfo.setCustomProperties(customProperties);
    addAspectToMcps(jobUrn, DATAJOB_ENTITY_TYPE, jobInfo, mcps);
    generateStatus(jobUrn, DATAJOB_ENTITY_TYPE, mcps);

    // Generate and add tags Aspect
    generateFlowGlobalTagsAspect(flowUrn, flowGlobalTags, config, mcps);

    // Generate and add domain Aspect
    generateFlowDomainsAspect(mcps, customProperties);

    log.info(
        "Adding input and output to {} Number of outputs: {}, Number of inputs {}",
        jobUrn,
        outSet.size(),
        inSet.size());

    // Generate Input and Outputs
    Pair<UrnArray, EdgeArray> inputsTuple = processUpstreams(config, mcps);
    UrnArray inputUrnArray = inputsTuple.getLeft();
    EdgeArray inputEdges = inputsTuple.getRight();

    Pair<UrnArray, EdgeArray> outputTuple = processDownstreams(config, mcps);
    UrnArray outputUrnArray = outputTuple.getLeft();
    EdgeArray outputEdges = outputTuple.getRight();

    // Generate and add DataJobInputOutput Aspect
    generateDataJobInputOutputMcp(inputEdges, outputEdges, config, mcps);

    // Generate and add DataProcessInstance Aspect
    generateDataProcessInstanceMcp(inputUrnArray, outputUrnArray, mcps);

    log.info("Mcp generation finished for urn {}", jobUrn);
    return mcps;
  }

  private FineGrainedLineageArray mergeFinegrainedLineages() {
    FineGrainedLineageArray fgls = new FineGrainedLineageArray();

    for (DatahubDataset dataset : inSet) {
      if (dataset.lineage != null && dataset.lineage.getFineGrainedLineages() != null) {
        dataset.lineage.getFineGrainedLineages().stream()
            .filter(Objects::nonNull)
            .forEach(fgls::add);
      }
    }

    for (DatahubDataset dataset : outSet) {
      if (dataset.lineage != null && dataset.lineage.getFineGrainedLineages() != null) {
        dataset.lineage.getFineGrainedLineages().stream()
            .filter(Objects::nonNull)
            .forEach(fgls::add);
      }
    }

    return fgls;
  }

  private void generateDataJobInputOutputMcp(
      EdgeArray inputEdges,
      EdgeArray outputEdges,
      DatahubOpenlineageConfig config,
      List<MetadataChangeProposal> mcps) {

    DataJobInputOutput dataJobInputOutput = new DataJobInputOutput();
    log.info("Adding DataJob edges to {}", jobUrn);
    if (config.isUsePatch() && (!parentJobs.isEmpty() || !inSet.isEmpty() || !outSet.isEmpty())) {
      DataJobInputOutputPatchBuilder dataJobInputOutputPatchBuilder =
          new DataJobInputOutputPatchBuilder().urn(jobUrn);
      for (DatahubDataset dataset : inSet) {
        dataJobInputOutputPatchBuilder.addInputDatasetEdge(dataset.getUrn());
      }
      for (DatahubDataset dataset : outSet) {
        dataJobInputOutputPatchBuilder.addOutputDatasetEdge(dataset.getUrn());
      }
      for (DataJobUrn parentJob : parentJobs) {
        dataJobInputOutputPatchBuilder.addInputDatajobEdge(parentJob);
      }

      FineGrainedLineageArray fgls = mergeFinegrainedLineages();
      fgls.forEach(
          fgl -> {
            Objects.requireNonNull(fgl.getUpstreams())
                .forEach(
                    upstream -> {
                      Objects.requireNonNull(fgl.getDownstreams())
                          .forEach(
                              downstream -> {
                                dataJobInputOutputPatchBuilder.addFineGrainedUpstreamField(
                                    upstream,
                                    fgl.getConfidenceScore(),
                                    StringUtils.defaultIfEmpty(
                                        fgl.getTransformOperation(), "TRANSFORM"),
                                    downstream,
                                    fgl.getQuery());
                              });
                    });
          });

      MetadataChangeProposal dataJobInputOutputMcp = dataJobInputOutputPatchBuilder.build();
      log.info(
          "dataJobInputOutputMcp: {}",
          Objects.requireNonNull(dataJobInputOutputMcp.getAspect())
              .getValue()
              .asString(Charset.defaultCharset()));
      mcps.add(dataJobInputOutputPatchBuilder.build());

    } else {
      FineGrainedLineageArray fgls = mergeFinegrainedLineages();
      dataJobInputOutput.setFineGrainedLineages(fgls);
      dataJobInputOutput.setInputDatasetEdges(inputEdges);
      dataJobInputOutput.setInputDatasets(new DatasetUrnArray());
      dataJobInputOutput.setOutputDatasetEdges(outputEdges);
      dataJobInputOutput.setOutputDatasets(new DatasetUrnArray());
      DataJobUrnArray parentDataJobUrnArray = new DataJobUrnArray();
      parentDataJobUrnArray.addAll(parentJobs);

      log.info(
          "Adding input data jobs {} Number of jobs: {}", jobUrn, parentDataJobUrnArray.size());
      dataJobInputOutput.setInputDatajobs(parentDataJobUrnArray);
      addAspectToMcps(jobUrn, DATAJOB_ENTITY_TYPE, dataJobInputOutput, mcps);
    }
  }

  private void generateDataProcessInstanceMcp(
      UrnArray inputUrnArray, UrnArray outputUrnArray, List<MetadataChangeProposal> mcps) {
    DataProcessInstanceInput dataProcessInstanceInput = new DataProcessInstanceInput();
    dataProcessInstanceInput.setInputs(inputUrnArray);

    DataProcessInstanceOutput dataProcessInstanceOutput = new DataProcessInstanceOutput();
    dataProcessInstanceOutput.setOutputs(outputUrnArray);

    addAspectToMcps(
        dataProcessInstanceUrn, DATA_PROCESS_INSTANCE_ENTITY_TYPE, dataProcessInstanceInput, mcps);
    addAspectToMcps(
        dataProcessInstanceUrn, DATA_PROCESS_INSTANCE_ENTITY_TYPE, dataProcessInstanceOutput, mcps);

    if (dataProcessInstanceProperties != null) {
      log.info("Adding dataProcessInstanceProperties to {}", jobUrn);
      addAspectToMcps(
          dataProcessInstanceUrn,
          DATA_PROCESS_INSTANCE_ENTITY_TYPE,
          dataProcessInstanceProperties,
          mcps);
    }

    generateDataProcessInstanceRunEvent(mcps);
    generateDataProcessInstanceRelationship(mcps);
  }

  private void deleteOldDatasetLineage(
      DatahubDataset dataset, DatahubOpenlineageConfig config, List<MetadataChangeProposal> mcps) {
    if (dataset.getLineage() != null) {
      if (config.isUsePatch()) {
        if (!dataset.getLineage().getUpstreams().isEmpty()) {
          UpstreamLineagePatchBuilder upstreamLineagePatchBuilder =
              new UpstreamLineagePatchBuilder().urn(dataset.getUrn());
          for (Upstream upstream : dataset.getLineage().getUpstreams()) {
            upstreamLineagePatchBuilder.removeUpstream(upstream.getDataset());
          }

          log.info("Removing FineGrainedLineage to {}", dataset.getUrn());
          for (FineGrainedLineage fineGrainedLineage :
              Objects.requireNonNull(dataset.getLineage().getFineGrainedLineages())) {
            for (Urn upstream : Objects.requireNonNull(fineGrainedLineage.getUpstreams())) {
              for (Urn downstream : Objects.requireNonNull(fineGrainedLineage.getDownstreams())) {
                upstreamLineagePatchBuilder.removeFineGrainedUpstreamField(
                    upstream,
                    StringUtils.defaultIfEmpty(
                        fineGrainedLineage.getTransformOperation(), "TRANSFORM"),
                    downstream,
                    null);
              }
            }
          }
          MetadataChangeProposal mcp = upstreamLineagePatchBuilder.build();
          log.info(
              "upstreamLineagePatch: {}",
              mcp.getAspect().getValue().asString(Charset.defaultCharset()));
          mcps.add(mcp);
        }
      } else {
        if (!dataset.getLineage().getUpstreams().isEmpty()) {
          // Remove earlier created UpstreamLineage which most probably was created by the plugin.
          UpstreamLineage upstreamLineage = new UpstreamLineage();
          upstreamLineage.setUpstreams(new UpstreamArray());
          upstreamLineage.setFineGrainedLineages(new FineGrainedLineageArray());
          addAspectToMcps(dataset.getUrn(), DATASET_ENTITY_TYPE, upstreamLineage, mcps);
        }
      }
    }
  }

  private Pair<UrnArray, EdgeArray> processDownstreams(
      DatahubOpenlineageConfig config, List<MetadataChangeProposal> mcps) {
    UrnArray outputUrnArray = new UrnArray();
    EdgeArray outputEdges = new EdgeArray();

    outSet.forEach(
        dataset -> {
          outputUrnArray.add(dataset.getUrn());
          if (config.isMaterializeDataset()) {
            try {
              mcps.add(eventFormatter.convert(materializeDataset(dataset.getUrn())));
              generateStatus(dataset.getUrn(), DATASET_ENTITY_TYPE, mcps);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }

          Edge edge =
              createEdge(
                  dataset.getUrn(),
                  ZonedDateTime.ofInstant(Instant.ofEpochMilli(eventTime), ZoneOffset.UTC));
          outputEdges.add(edge);

          if ((dataset.getSchemaMetadata() != null) && (config.isIncludeSchemaMetadata())) {
            addAspectToMcps(
                dataset.getUrn(), DATASET_ENTITY_TYPE, dataset.getSchemaMetadata(), mcps);
          }

          // Remove lineage which was added by older plugin that set lineage on Datasets and not on
          // DataJobs
          if (config.isRemoveLegacyLineage()) {
            deleteOldDatasetLineage(dataset, config, mcps);
          }
        });

    return Pair.of(outputUrnArray, outputEdges);
  }

  private Pair<UrnArray, EdgeArray> processUpstreams(
      DatahubOpenlineageConfig config, List<MetadataChangeProposal> mcps) {
    UrnArray inputUrnArray = new UrnArray();
    EdgeArray inputEdges = new EdgeArray();

    inSet.forEach(
        dataset -> {
          inputUrnArray.add(dataset.getUrn());
          Edge edge =
              createEdge(
                  dataset.getUrn(),
                  ZonedDateTime.ofInstant(Instant.ofEpochMilli(eventTime), ZoneOffset.UTC));
          inputEdges.add(edge);

          if (config.isMaterializeDataset()) {
            try {
              mcps.add(eventFormatter.convert(materializeDataset(dataset.getUrn())));
              generateStatus(dataset.getUrn(), DATASET_ENTITY_TYPE, mcps);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }

          if (dataset.getSchemaMetadata() != null && config.isIncludeSchemaMetadata()) {
            addAspectToMcps(
                dataset.getUrn(), DATASET_ENTITY_TYPE, dataset.getSchemaMetadata(), mcps);
          }
        });
    return Pair.of(inputUrnArray, inputEdges);
  }

  private void generateFlowDomainsAspect(
      List<MetadataChangeProposal> mcps, StringMap customProperties) {
    if (flowDomains != null) {
      MetadataChangeProposalWrapper domains =
          MetadataChangeProposalWrapper.create(
              b ->
                  b.entityType(DATAFLOW_ENTITY_TYPE)
                      .entityUrn(flowUrn)
                      .upsert()
                      .aspect(flowDomains));
      try {
        mcps.add(eventFormatter.convert(domains));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void generateFlowGlobalTagsAspect(
      Urn flowUrn,
      GlobalTags flowGlobalTags,
      DatahubOpenlineageConfig config,
      List<MetadataChangeProposal> mcps) {
    if (flowGlobalTags != null) {
      if ((config.isUsePatch() && (!flowGlobalTags.getTags().isEmpty()))) {
        GlobalTagsPatchBuilder globalTagsPatchBuilder = new GlobalTagsPatchBuilder().urn(flowUrn);
        for (TagAssociation tag : flowGlobalTags.getTags()) {
          globalTagsPatchBuilder.addTag(tag.getTag(), null);
        }
        globalTagsPatchBuilder.urn(flowUrn);
        mcps.add(globalTagsPatchBuilder.build());
      } else {
        addAspectToMcps(flowUrn, DATA_FLOW_ENTITY_TYPE, flowGlobalTags, mcps);
      }
    }
  }

  private void generateStatus(Urn entityUrn, String entityType, List<MetadataChangeProposal> mcps) {
    Status statusInfo = new Status().setRemoved(false);
    addAspectToMcps(entityUrn, entityType, statusInfo, mcps);
  }

  private void addAspectToMcps(
      Urn entityUrn, String entityType, DataTemplate aspect, List<MetadataChangeProposal> mcps) {
    MetadataChangeProposalWrapper mcpw =
        MetadataChangeProposalWrapper.create(
            b -> b.entityType(entityType).entityUrn(entityUrn).upsert().aspect(aspect));
    try {
      mcps.add(eventFormatter.convert(mcpw));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void generateDataProcessInstanceRelationship(List<MetadataChangeProposal> mcps) {
    if (dataProcessInstanceRelationships != null) {
      log.info("Adding dataProcessInstanceRelationships to {}", jobUrn);
      try {
        mcps.add(
            eventFormatter.convert(
                MetadataChangeProposalWrapper.create(
                    b ->
                        b.entityType(DATA_PROCESS_INSTANCE_ENTITY_TYPE)
                            .entityUrn(dataProcessInstanceUrn)
                            .upsert()
                            .aspect(dataProcessInstanceRelationships))));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void generateDataProcessInstanceRunEvent(List<MetadataChangeProposal> mcps) {
    if (dataProcessInstanceRunEvent != null) {
      log.info("Adding dataProcessInstanceRunEvent to {}", jobUrn);
      try {
        mcps.add(
            eventFormatter.convert(
                MetadataChangeProposalWrapper.create(
                    b ->
                        b.entityType(DATA_PROCESS_INSTANCE_ENTITY_TYPE)
                            .entityUrn(dataProcessInstanceUrn)
                            .upsert()
                            .aspect(dataProcessInstanceRunEvent))));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}

class DataSetComparator implements Comparator<DatahubDataset> {

  @Override
  public int compare(DatahubDataset dataset1, DatahubDataset dataset2) {
    return dataset1.urn.toString().compareTo(dataset2.getUrn().toString());
  }
}

class DataJobUrnComparator implements Comparator<DataJobUrn> {

  @Override
  public int compare(DataJobUrn urn1, DataJobUrn urn2) {
    return urn1.toString().compareTo(urn2.toString());
  }
}
