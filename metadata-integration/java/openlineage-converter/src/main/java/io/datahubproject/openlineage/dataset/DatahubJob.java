package io.datahubproject.openlineage.dataset;

import static io.datahubproject.openlineage.converter.OpenLineageToDataHub.*;

import com.linkedin.common.DataJobUrnArray;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.DataTransformLogic;
import com.linkedin.common.DatasetUrnArray;
import com.linkedin.common.Edge;
import com.linkedin.common.EdgeArray;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.Ownership;
import com.linkedin.common.Siblings;
import com.linkedin.common.Status;
import com.linkedin.common.SubTypes;
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
import com.linkedin.datajob.VersionInfo;
import com.linkedin.dataprocess.DataProcessInstanceInput;
import com.linkedin.dataprocess.DataProcessInstanceOutput;
import com.linkedin.dataprocess.DataProcessInstanceProperties;
import com.linkedin.dataprocess.DataProcessInstanceRelationships;
import com.linkedin.dataprocess.DataProcessInstanceRunEvent;
import com.linkedin.dataset.FineGrainedLineage;
import com.linkedin.dataset.FineGrainedLineageArray;
import com.linkedin.domain.Domains;
import com.linkedin.metadata.aspect.patch.builder.DataJobInputOutputPatchBuilder;
import com.linkedin.metadata.aspect.patch.builder.GlobalTagsPatchBuilder;
import com.linkedin.metadata.aspect.patch.builder.SiblingsPatchBuilder;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.key.DatasetKey;
import com.linkedin.mxe.MetadataChangeProposal;
import datahub.event.MetadataChangeProposalWrapper;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.openlineage.converter.OpenLineageMcpFactory;
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
  VersionInfo flowVersionInfo;
  DataJobUrn jobUrn;
  DataJobInfo jobInfo;
  Ownership flowOwnership;
  Ownership jobOwnership;
  GlobalTags flowGlobalTags;
  GlobalTags jobGlobalTags;
  SubTypes jobSubTypes;
  Domains flowDomains;
  DataPlatformInstance flowPlatformInstance;
  DataPlatformInstance jobPlatformInstance;
  DataTransformLogic dataTransformLogic;
  @Builder.Default boolean emitDataProcessInstance = true;
  DataProcessInstanceRunEvent dataProcessInstanceRunEvent;
  DataProcessInstanceProperties dataProcessInstanceProperties;
  DataProcessInstanceRelationships dataProcessInstanceRelationships;
  Urn dataProcessInstanceUrn;

  final Set<DatahubDataset> inSet = new TreeSet<>(new DataSetComparator());
  final Set<DatahubDataset> outSet = new TreeSet<>(new DataSetComparator());
  final Set<DataJobUrn> parentJobs = new TreeSet<>(new DataJobUrnComparator());
  final Map<DataJobUrn, StringMap> parentJobProperties = new HashMap<>();
  final List<MetadataChangeProposal> extraMcps = new ArrayList<>();
  final Map<String, String> datasetProperties = new HashMap<>();
  long startTime;
  long endTime;
  long eventTime;

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
    // The DataJob needs its own DataPlatformInstance aspect: it inherits the instance via the
    // parent
    // DataFlow URN, but the aspect drives the platform-instance facet/search on the job entity.
    if (jobPlatformInstance != null) {
      addAspectToMcps(jobUrn, DATAJOB_ENTITY_TYPE, jobPlatformInstance, mcps);
    }

    if (flowVersionInfo != null) {
      addAspectToMcps(flowUrn, DATA_FLOW_ENTITY_TYPE, flowVersionInfo, mcps);
    }

    if (flowOwnership != null) {
      addAspectToMcps(flowUrn, DATA_FLOW_ENTITY_TYPE, flowOwnership, mcps);
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

    if (jobOwnership != null) {
      addAspectToMcps(jobUrn, DATAJOB_ENTITY_TYPE, jobOwnership, mcps);
    }

    generateGlobalTagsAspect(jobUrn, DATAJOB_ENTITY_TYPE, jobGlobalTags, config, mcps);

    if (jobSubTypes != null) {
      addAspectToMcps(jobUrn, DATAJOB_ENTITY_TYPE, jobSubTypes, mcps);
    }

    if (dataTransformLogic != null) {
      addAspectToMcps(jobUrn, DATAJOB_ENTITY_TYPE, dataTransformLogic, mcps);
    }

    // Generate and add tags Aspect
    generateGlobalTagsAspect(flowUrn, DATA_FLOW_ENTITY_TYPE, flowGlobalTags, config, mcps);

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

    mcps.addAll(extraMcps);

    // Generate and add DataProcessInstance Aspect
    if (emitDataProcessInstance) {
      generateDataProcessInstanceMcp(inputUrnArray, outputUrnArray, mcps);
    }

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

    // Skip an empty dataJobInputOutput only in PATCH mode. When coalesced emission fires on early
    // events (e.g., START), all sets are empty; without this skip the all-empty case falls through
    // to the UPSERT branch below (the PATCH branch requires a non-empty set), creating the aspect
    // with empty arrays that a later PATCH cannot override, losing edges. In UPSERT mode the
    // reverse
    // holds: emitting the empty aspect is the only way to clear edges a job legitimately no longer
    // has, so it must not be skipped.
    if (config.isUsePatch()
        && inputEdges.isEmpty()
        && outputEdges.isEmpty()
        && parentJobs.isEmpty()) {
      log.info("Skipping empty dataJobInputOutput PATCH for {} - no edges to emit yet", jobUrn);
      return;
    }

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
        Edge edge =
            createEdge(
                parentJob,
                ZonedDateTime.ofInstant(Instant.ofEpochMilli(eventTime), ZoneOffset.UTC));
        if (parentJobProperties.containsKey(parentJob)) {
          edge.setProperties(parentJobProperties.get(parentJob));
        }
        dataJobInputOutputPatchBuilder.addEdge(edge, LineageDirection.UPSTREAM);
      }
      for (FineGrainedLineage fineGrainedLineage : mergeFinegrainedLineages()) {
        if (fineGrainedLineage.getUpstreams() == null
            || fineGrainedLineage.getDownstreams() == null) {
          continue;
        }
        for (Urn upstream : fineGrainedLineage.getUpstreams()) {
          for (Urn downstream : fineGrainedLineage.getDownstreams()) {
            dataJobInputOutputPatchBuilder.addFineGrainedUpstreamField(
                upstream,
                fineGrainedLineage.getConfidenceScore(),
                StringUtils.defaultIfEmpty(fineGrainedLineage.getTransformOperation(), "TRANSFORM"),
                downstream,
                fineGrainedLineage.getQuery());
          }
        }
      }

      MetadataChangeProposal dataJobInputOutputMcp = dataJobInputOutputPatchBuilder.build();
      log.info(
          "dataJobInputOutputMcp: {}",
          Objects.requireNonNull(dataJobInputOutputMcp.getAspect())
              .getValue()
              .asString(Charset.defaultCharset()));
      mcps.add(dataJobInputOutputMcp);

    } else {
      dataJobInputOutput.setFineGrainedLineages(mergeFinegrainedLineages());
      dataJobInputOutput.setInputDatasetEdges(inputEdges);
      dataJobInputOutput.setInputDatasets(new DatasetUrnArray());
      dataJobInputOutput.setOutputDatasetEdges(outputEdges);
      dataJobInputOutput.setOutputDatasets(new DatasetUrnArray());
      EdgeArray inputDatajobEdges = new EdgeArray();
      for (DataJobUrn parentJob : parentJobs) {
        Edge edge =
            createEdge(
                parentJob,
                ZonedDateTime.ofInstant(Instant.ofEpochMilli(eventTime), ZoneOffset.UTC));
        if (parentJobProperties.containsKey(parentJob)) {
          edge.setProperties(parentJobProperties.get(parentJob));
        }
        inputDatajobEdges.add(edge);
      }

      log.info("Adding input data jobs {} Number of jobs: {}", jobUrn, inputDatajobEdges.size());
      dataJobInputOutput.setInputDatajobs(new DataJobUrnArray());
      dataJobInputOutput.setInputDatajobEdges(inputDatajobEdges);
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

  private Pair<UrnArray, EdgeArray> processDownstreams(
      DatahubOpenlineageConfig config, List<MetadataChangeProposal> mcps) {
    UrnArray outputUrnArray = new UrnArray();
    EdgeArray outputEdges = new EdgeArray();

    outSet.forEach(
        dataset -> {
          outputUrnArray.add(dataset.getUrn());
          materializeDatasetAspects(dataset, config, mcps);

          Edge edge =
              createEdge(
                  dataset.getUrn(),
                  ZonedDateTime.ofInstant(Instant.ofEpochMilli(eventTime), ZoneOffset.UTC));
          outputEdges.add(edge);

          if ((dataset.getSchemaMetadata() != null) && (config.isIncludeSchemaMetadata())) {
            addAspectToMcps(
                dataset.getUrn(), DATASET_ENTITY_TYPE, dataset.getSchemaMetadata(), mcps);
          }

          emitDatasetFacetAspects(dataset, config, mcps);
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

          materializeDatasetAspects(dataset, config, mcps);

          if (dataset.getSchemaMetadata() != null && config.isIncludeSchemaMetadata()) {
            addAspectToMcps(
                dataset.getUrn(), DATASET_ENTITY_TYPE, dataset.getSchemaMetadata(), mcps);
          }

          emitDatasetFacetAspects(dataset, config, mcps);
        });
    return Pair.of(inputUrnArray, inputEdges);
  }

  private void materializeDatasetAspects(
      DatahubDataset dataset, DatahubOpenlineageConfig config, List<MetadataChangeProposal> mcps) {
    if (config.isMaterializeDataset()) {
      mcps.add(OpenLineageMcpFactory.convertUnchecked(materializeDataset(dataset.getUrn())));
      addAspectToMcps(
          dataset.getUrn(),
          DATASET_ENTITY_TYPE,
          dataset.getStatus() != null ? dataset.getStatus() : new Status().setRemoved(false),
          mcps);
    } else if (dataset.getStatus() != null) {
      addAspectToMcps(dataset.getUrn(), DATASET_ENTITY_TYPE, dataset.getStatus(), mcps);
    }
  }

  private void emitDatasetFacetAspects(
      DatahubDataset dataset, DatahubOpenlineageConfig config, List<MetadataChangeProposal> mcps) {
    if (dataset.getDatasetProperties() != null) {
      addAspectToMcps(dataset.getUrn(), DATASET_ENTITY_TYPE, dataset.getDatasetProperties(), mcps);
    }

    if (dataset.getDataPlatformInstance() != null) {
      addAspectToMcps(
          dataset.getUrn(), DATASET_ENTITY_TYPE, dataset.getDataPlatformInstance(), mcps);
    }

    if (dataset.getDatasetProfile() != null) {
      addAspectToMcps(dataset.getUrn(), DATASET_ENTITY_TYPE, dataset.getDatasetProfile(), mcps);
    }

    if (dataset.getOperation() != null) {
      addAspectToMcps(dataset.getUrn(), DATASET_ENTITY_TYPE, dataset.getOperation(), mcps);
    }

    if (dataset.getOwnership() != null) {
      addAspectToMcps(dataset.getUrn(), DATASET_ENTITY_TYPE, dataset.getOwnership(), mcps);
    }

    generateGlobalTagsAspect(
        dataset.getUrn(), DATASET_ENTITY_TYPE, dataset.getGlobalTags(), config, mcps);

    if (dataset.getSubTypes() != null) {
      addAspectToMcps(dataset.getUrn(), DATASET_ENTITY_TYPE, dataset.getSubTypes(), mcps);
    }

    emitSiblingsAspect(dataset, config, mcps);
  }

  private void emitSiblingsAspect(
      DatahubDataset dataset, DatahubOpenlineageConfig config, List<MetadataChangeProposal> mcps) {
    Siblings siblings = dataset.getSiblings();
    if (siblings == null || siblings.getSiblings().isEmpty()) {
      return;
    }

    if (config.isUsePatch()) {
      SiblingsPatchBuilder siblingsPatchBuilder = new SiblingsPatchBuilder().urn(dataset.getUrn());
      boolean shouldSetPrimary = siblings.isPrimary();
      for (Urn sibling : siblings.getSiblings()) {
        siblingsPatchBuilder.addSibling(sibling, shouldSetPrimary);
        shouldSetPrimary = false;
      }
      mcps.add(siblingsPatchBuilder.build());
    } else {
      addAspectToMcps(dataset.getUrn(), DATASET_ENTITY_TYPE, siblings, mcps);
    }
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
      mcps.add(OpenLineageMcpFactory.convertUnchecked(domains));
    }
  }

  private void generateGlobalTagsAspect(
      Urn entityUrn,
      String entityType,
      GlobalTags globalTags,
      DatahubOpenlineageConfig config,
      List<MetadataChangeProposal> mcps) {
    if (globalTags != null) {
      if ((config.isUsePatch() && (!globalTags.getTags().isEmpty()))) {
        GlobalTagsPatchBuilder globalTagsPatchBuilder = new GlobalTagsPatchBuilder().urn(entityUrn);
        for (TagAssociation tag : globalTags.getTags()) {
          globalTagsPatchBuilder.addTag(tag.getTag(), null);
        }
        globalTagsPatchBuilder.urn(entityUrn);
        mcps.add(globalTagsPatchBuilder.build());
      } else {
        addAspectToMcps(entityUrn, entityType, globalTags, mcps);
      }
    }
  }

  private void generateStatus(Urn entityUrn, String entityType, List<MetadataChangeProposal> mcps) {
    Status statusInfo = new Status().setRemoved(false);
    addAspectToMcps(entityUrn, entityType, statusInfo, mcps);
  }

  private void addAspectToMcps(
      Urn entityUrn, String entityType, DataTemplate aspect, List<MetadataChangeProposal> mcps) {
    mcps.add(OpenLineageMcpFactory.upsert(entityUrn, entityType, aspect));
  }

  private void generateDataProcessInstanceRelationship(List<MetadataChangeProposal> mcps) {
    if (dataProcessInstanceRelationships != null) {
      log.info("Adding dataProcessInstanceRelationships to {}", jobUrn);
      mcps.add(
          OpenLineageMcpFactory.upsert(
              dataProcessInstanceUrn,
              DATA_PROCESS_INSTANCE_ENTITY_TYPE,
              dataProcessInstanceRelationships));
    }
  }

  private void generateDataProcessInstanceRunEvent(List<MetadataChangeProposal> mcps) {
    if (dataProcessInstanceRunEvent != null) {
      log.info("Adding dataProcessInstanceRunEvent to {}", jobUrn);
      mcps.add(
          OpenLineageMcpFactory.upsert(
              dataProcessInstanceUrn,
              DATA_PROCESS_INSTANCE_ENTITY_TYPE,
              dataProcessInstanceRunEvent));
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
