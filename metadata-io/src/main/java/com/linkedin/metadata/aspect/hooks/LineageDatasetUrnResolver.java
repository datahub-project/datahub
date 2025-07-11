package com.linkedin.metadata.aspect.hooks;

import static com.linkedin.dataset.FineGrainedLineageDownstreamType.FIELD;
import static com.linkedin.dataset.FineGrainedLineageDownstreamType.FIELD_SET;
import static com.linkedin.metadata.Constants.*;

import com.datahub.util.RecordUtils;
import com.linkedin.common.Edge;
import com.linkedin.common.EdgeArray;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.datajob.DataJobInputOutput;
import com.linkedin.dataset.*;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import com.linkedin.platformresource.PlatformResourceInfo;
import com.linkedin.util.Pair;
import java.net.URISyntaxException;
import java.util.*;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Setter
@Getter
@Accessors(chain = true)
public class LineageDatasetUrnResolver extends MutationHook {

  @Nonnull private AspectPluginConfig config;

  private List<String> platforms;

  private Float scoreUrnExists;
  private Float scoreUrnResolvedPlatformResource;
  private Float scoreUrnUnresolved;

  @SneakyThrows
  @Override
  protected Stream<Pair<ChangeMCP, Boolean>> writeMutation(
      @Nonnull Collection<ChangeMCP> changeMCPSColl, @Nonnull RetrieverContext retrieverContext) {
    Set<Integer> mcpsToCheck = new HashSet<>();
    Set<Urn> urnsToCheck = new HashSet<>();

    List<ChangeMCP> changeMCPS = new ArrayList<>(changeMCPSColl);

    for (int i = 0; i < changeMCPS.size(); i++) {
      ChangeMCP mcp = changeMCPS.get(i);
      if (mcp.getAspectName().equals(DATA_JOB_INPUT_OUTPUT_ASPECT_NAME)) {
        DataJobInputOutput dataJobInputOutput = mcp.getAspect(DataJobInputOutput.class);
        Set<Urn> newUrnsToCheck = new HashSet<>();
        checkUrns(retrieverContext, dataJobInputOutput.getInputDatasetEdges(), newUrnsToCheck);
        checkUrns(retrieverContext, dataJobInputOutput.getOutputDatasetEdges(), newUrnsToCheck);
        checkUrns(retrieverContext, dataJobInputOutput.getFineGrainedLineages(), newUrnsToCheck);
        if (!newUrnsToCheck.isEmpty()) {
          mcpsToCheck.add(i);
        }
        urnsToCheck.addAll(newUrnsToCheck);
      }
    }

    Map<Urn, Boolean> existence = retrieverContext.getAspectRetriever().entityExists(urnsToCheck);
    Set<DatasetUrn> existentUrns = new HashSet<>();
    Set<DatasetUrn> urnsToResolve = new HashSet<>();
    for (Map.Entry<Urn, Boolean> entry : existence.entrySet()) {
      DatasetUrn datasetUrn = DatasetUrn.createFromUrn(entry.getKey());
      if (entry.getValue()) {
        existentUrns.add(datasetUrn);
      } else {
        urnsToResolve.add(datasetUrn);
      }
    }

    Map<DatasetUrn, DatasetUrn> resolvedUrns = resolve(urnsToResolve, retrieverContext);
    Set<DatasetUrn> unresolvedUrns = new HashSet<>(urnsToResolve);
    unresolvedUrns.removeAll(resolvedUrns.keySet());

    List<Pair<ChangeMCP, Boolean>> result = new ArrayList<>();

    for (int i = 0; i < changeMCPS.size(); i++) {
      ChangeMCP mcp = changeMCPS.get(i);
      if (!mcpsToCheck.contains(i)) {
        result.add(Pair.of(mcp, false));
        continue;
      }
      boolean modified = false;
      if (mcp.getAspectName().equals(DATA_JOB_INPUT_OUTPUT_ASPECT_NAME)) {
        DataJobInputOutput dataJobInputOutput = mcp.getAspect(DataJobInputOutput.class);
        boolean inputsDatasetsModified =
            checkDatasetUrns(
                dataJobInputOutput.getInputDatasetEdges(),
                existentUrns,
                resolvedUrns,
                unresolvedUrns);
        boolean outputDatasetsModified =
            checkDatasetUrns(
                dataJobInputOutput.getOutputDatasetEdges(),
                existentUrns,
                resolvedUrns,
                unresolvedUrns);
        boolean fineGrainedModified =
            (inputsDatasetsModified || outputDatasetsModified)
                && checkFineGrainedLineages(
                    dataJobInputOutput.getFineGrainedLineages(),
                    existentUrns,
                    resolvedUrns,
                    unresolvedUrns);
        modified = inputsDatasetsModified || outputDatasetsModified || fineGrainedModified;
      }
      result.add(Pair.of(mcp, modified));
    }
    return result.stream();
  }

  private void checkUrns(RetrieverContext retrieverContext, EdgeArray edges, Set<Urn> urnsToCheck) {
    if (edges == null) {
      return;
    }
    edges.forEach(
        e -> {
          try {
            DatasetUrn urn = DatasetUrn.createFromUrn(e.getDestinationUrn());
            if (shouldResolve(urn, retrieverContext)) {
              urnsToCheck.add(urn);
            }
          } catch (URISyntaxException ex) {
            log.error("Ignoring unexpected urn " + e.getDestinationUrn(), ex);
          }
        });
  }

  @SneakyThrows
  private boolean checkDatasetUrns(
      EdgeArray edges,
      Set<DatasetUrn> existentUrns,
      Map<DatasetUrn, DatasetUrn> resolvedUrns,
      Set<DatasetUrn> unresolvedUrns) {
    if (edges == null) {
      return false;
    }
    boolean modified = false;
    for (Edge edge : edges) {
      DatasetUrn urn = DatasetUrn.createFromUrn(edge.getDestinationUrn());
      if (existentUrns.contains(urn)) {
        edge.setConfidenceScore(scoreUrnExists, SetMode.IGNORE_NULL);
      } else if (unresolvedUrns.contains(urn)) {
        edge.setConfidenceScore(scoreUrnUnresolved, SetMode.IGNORE_NULL);
      } else if (resolvedUrns.containsKey(urn)) {
        edge.setDestinationUrn(resolvedUrns.get(urn));
        edge.setConfidenceScore(scoreUrnResolvedPlatformResource, SetMode.IGNORE_NULL);
      } else {
        // no modifications
        continue;
      }
      modified = true;
    }
    return modified;
  }

  private void checkUrns(
      RetrieverContext retrieverContext,
      FineGrainedLineageArray fineGrainedLineages,
      Set<Urn> urnsToCheck) {
    if (fineGrainedLineages == null) {
      return;
    }
    for (FineGrainedLineage fineGrainedLineage : fineGrainedLineages) {
      checkUrns(
          fineGrainedLineage.getDownstreams(),
          Set.of(FIELD, FIELD_SET).contains(fineGrainedLineage.getDownstreamType()),
          retrieverContext,
          urnsToCheck);
      checkUrns(
          fineGrainedLineage.getUpstreams(),
          FineGrainedLineageUpstreamType.FIELD_SET == fineGrainedLineage.getUpstreamType(),
          retrieverContext,
          urnsToCheck);
    }
  }

  @SneakyThrows
  private boolean checkFineGrainedLineages(
      FineGrainedLineageArray fineGrainedLineages,
      Set<DatasetUrn> existentUrns,
      Map<DatasetUrn, DatasetUrn> resolvedUrns,
      Set<DatasetUrn> unresolvedUrns) {
    if (fineGrainedLineages == null) {
      return false;
    }
    boolean modified = false;
    for (FineGrainedLineage fineGrainedLineage : fineGrainedLineages) {
      Optional<UrnArray> updatedDownstreams =
          checkFinegrainedUrns(
              fineGrainedLineage.getDownstreams(),
              Set.of(FIELD, FIELD_SET).contains(fineGrainedLineage.getDownstreamType()),
              existentUrns,
              resolvedUrns,
              unresolvedUrns);
      if (updatedDownstreams.isPresent()) {
        fineGrainedLineage.setDownstreams(updatedDownstreams.get());
        modified = true;
      }

      Optional<UrnArray> updatedUpstreams =
          checkFinegrainedUrns(
              fineGrainedLineage.getUpstreams(),
              FineGrainedLineageUpstreamType.FIELD_SET == fineGrainedLineage.getUpstreamType(),
              existentUrns,
              resolvedUrns,
              unresolvedUrns);
      if (updatedUpstreams.isPresent()) {
        fineGrainedLineage.setUpstreams(updatedUpstreams.get());
        modified = true;
      }
    }
    return modified;
  }

  @SneakyThrows
  private void checkUrns(
      UrnArray urns,
      boolean schemaFields,
      RetrieverContext retrieverContext,
      Set<Urn> urnsToCheck) {
    if (schemaFields) {
      for (Urn fieldUrn : urns) {
        if (!"schemaField".equals(fieldUrn.getEntityType())) {
          // strange, but ignoring here
          continue;
        }
        if (fieldUrn.getEntityKey().getParts().size() != 2) {
          // strange, but ignoring here
          continue;
        }
        DatasetUrn urn = DatasetUrn.createFromString(fieldUrn.getEntityKey().getFirst());
        if (shouldResolve(urn, retrieverContext)) {
          urnsToCheck.add(urn);
        }
      }
    } else {
      for (Urn datasetUrn : urns) {
        if (!"dataset".equals(datasetUrn.getEntityType())) {
          // strange, but ignoring here
          continue;
        }
        DatasetUrn urn = DatasetUrn.createFromUrn(datasetUrn);
        if (shouldResolve(urn, retrieverContext)) {
          urnsToCheck.add(urn);
        }
      }
    }
  }

  @SneakyThrows
  private Optional<UrnArray> checkFinegrainedUrns(
      UrnArray urns,
      boolean schemaFields,
      Set<DatasetUrn> existentUrns,
      Map<DatasetUrn, DatasetUrn> resolvedUrns,
      Set<DatasetUrn> unresolvedUrns) {
    UrnArray result = new UrnArray(urns);
    boolean modified = false;

    if (schemaFields) {
      for (ListIterator<Urn> iter = result.listIterator(); iter.hasNext(); ) {
        Urn fieldUrn = iter.next();
        if (!"schemaField".equals(fieldUrn.getEntityType())) {
          // strange, but ignoring here
          continue;
        }
        if (fieldUrn.getEntityKey().getParts().size() != 2) {
          // strange, but ignoring here
          continue;
        }
        DatasetUrn urn = DatasetUrn.createFromString(fieldUrn.getEntityKey().getFirst());
        if (resolvedUrns.containsKey(urn)) {
          Urn updatedFieldUrn =
              Urn.createFromTuple(
                  "schemaField", resolvedUrns.get(urn), fieldUrn.getEntityKey().get(1));
          iter.set(updatedFieldUrn);
          modified = true;
        }
      }
    } else {
      for (ListIterator<Urn> iter = result.listIterator(); iter.hasNext(); ) {
        Urn datasetUrn = iter.next();
        if (!"dataset".equals(datasetUrn.getEntityType())) {
          // strange, but ignoring here
          continue;
        }
        DatasetUrn urn = DatasetUrn.createFromUrn(datasetUrn);
        if (resolvedUrns.containsKey(urn)) {
          iter.set(resolvedUrns.get(urn));
          modified = true;
        }
      }
    }
    return modified ? Optional.of(result) : Optional.empty();
  }

  private boolean shouldResolve(DatasetUrn urn, RetrieverContext retrieverContext) {
    return platforms != null && platforms.contains(urn.getPlatformEntity().getPlatformNameEntity());
  }

  private static boolean exists(Urn urn, RetrieverContext retrieverContext) {
    Map<Urn, Boolean> existsMap = retrieverContext.getAspectRetriever().entityExists(Set.of(urn));
    return existsMap.get(urn);
  }

  private static Map<DatasetUrn, DatasetUrn> resolve(
      Set<DatasetUrn> datasetUrns, RetrieverContext retrieverContext) {
    Map<Urn, DatasetUrn> resourceUrns = new HashMap<>();
    for (DatasetUrn datasetUrn : datasetUrns) {
      Urn resourceUrn =
          UrnUtils.getUrn(
              String.format(
                  "urn:li:platformResource:%s.%s",
                  datasetUrn.getPlatformEntity().getPlatformNameEntity(),
                  datasetUrn.getDatasetNameEntity()));
      resourceUrns.put(resourceUrn, datasetUrn);
    }

    Map<Urn, Map<String, Aspect>> resourceInfoAspects =
        retrieverContext
            .getAspectRetriever()
            .getLatestAspectObjects(
                resourceUrns.keySet(), Set.of(PLATFORM_RESOURCE_INFO_ASPECT_NAME));

    Map<DatasetUrn, DatasetUrn> result = new HashMap<>();
    for (Map.Entry<Urn, Map<String, Aspect>> entry : resourceInfoAspects.entrySet()) {
      if (entry.getValue() == null) {
        continue;
      }
      Aspect resourceInfoAspect = entry.getValue().get(PLATFORM_RESOURCE_INFO_ASPECT_NAME);
      if (resourceInfoAspect == null) {
        continue;
      }
      PlatformResourceInfo resourceInfo =
          RecordUtils.toRecordTemplate(PlatformResourceInfo.class, resourceInfoAspect.data());

      try {
        DatasetUrn resolvedUrn = DatasetUrn.createFromString(resourceInfo.getPrimaryKey());
        DatasetUrn originalUrn = resourceUrns.get(entry.getKey());
        result.put(originalUrn, resolvedUrn);
      } catch (URISyntaxException e) {
        log.error("Urn format error", e);
      }
    }
    return result;
  }
}
