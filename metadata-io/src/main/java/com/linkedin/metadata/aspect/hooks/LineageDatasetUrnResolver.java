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

  @SneakyThrows
  @Override
  protected Stream<Pair<ChangeMCP, Boolean>> writeMutation(
      @Nonnull Collection<ChangeMCP> changeMCPS, @Nonnull RetrieverContext retrieverContext) {
    List<Pair<ChangeMCP, Boolean>> result = new ArrayList<>();

    for (Iterator<ChangeMCP> iter = changeMCPS.iterator(); iter.hasNext(); ) {
      ChangeMCP mcp = iter.next();
      if (mcp.getAspectName().equals(DATA_JOB_INPUT_OUTPUT_ASPECT_NAME)) {
        DataJobInputOutput dataJobInputOutput = mcp.getAspect(DataJobInputOutput.class);
        boolean inputsDatasetsModified =
            checkDatasetUrns(retrieverContext, dataJobInputOutput.getInputDatasetEdges());
        boolean outputDatasetsModified =
            checkDatasetUrns(retrieverContext, dataJobInputOutput.getOutputDatasetEdges());
        boolean fineGrainedModified =
            (inputsDatasetsModified || outputDatasetsModified)
                && checkFineGrainedLineages(
                    retrieverContext, dataJobInputOutput.getFineGrainedLineages());
        result.add(
            Pair.of(mcp, inputsDatasetsModified || outputDatasetsModified || fineGrainedModified));
      }
    }
    return result.stream();
  }

  @SneakyThrows
  private boolean checkDatasetUrns(RetrieverContext retrieverContext, EdgeArray edges) {
    if (edges == null) {
      return false;
    }
    boolean modified = false;
    for (Edge edge : edges) {
      DatasetUrn urn = DatasetUrn.createFromUrn(edge.getDestinationUrn());

      if (shouldResolve(urn, retrieverContext)) {
        Optional<DatasetUrn> resolvedUrn = resolve(urn, retrieverContext);
        if (resolvedUrn.isEmpty()) {
          continue;
        }
        edge.setDestinationUrn(resolvedUrn.get());
        modified = true;
      }
    }
    return modified;
  }

  @SneakyThrows
  private boolean checkFineGrainedLineages(
      RetrieverContext retrieverContext, FineGrainedLineageArray fineGrainedLineages) {
    if (fineGrainedLineages == null) {
      return false;
    }
    boolean modified = false;
    for (FineGrainedLineage fineGrainedLineage : fineGrainedLineages) {
      Optional<UrnArray> updatedDownstreams =
          checkFinegrainedUrns(
              fineGrainedLineage.getDownstreams(),
              Set.of(FIELD, FIELD_SET).contains(fineGrainedLineage.getDownstreamType()),
              retrieverContext);
      if (updatedDownstreams.isPresent()) {
        fineGrainedLineage.setDownstreams(updatedDownstreams.get());
        modified = true;
      }

      Optional<UrnArray> updatedUpstreams =
          checkFinegrainedUrns(
              fineGrainedLineage.getUpstreams(),
              FineGrainedLineageUpstreamType.FIELD_SET == fineGrainedLineage.getUpstreamType(),
              retrieverContext);
      if (updatedUpstreams.isPresent()) {
        fineGrainedLineage.setUpstreams(updatedUpstreams.get());
        modified = true;
      }
    }
    return modified;
  }

  @SneakyThrows
  private Optional<UrnArray> checkFinegrainedUrns(
      UrnArray urns, boolean schemaFields, RetrieverContext retrieverContext) {
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
        if (shouldResolve(urn, retrieverContext)) {
          Optional<DatasetUrn> resolvedUrn = resolve(urn, retrieverContext);
          if (resolvedUrn.isEmpty()) {
            continue;
          }
          Urn updatedFieldUrn =
              Urn.createFromTuple("schemaField", resolvedUrn.get(), fieldUrn.getEntityKey().get(1));
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
        if (shouldResolve(urn, retrieverContext)) {
          Optional<DatasetUrn> resolvedUrn = resolve(urn, retrieverContext);
          if (resolvedUrn.isEmpty()) {
            continue;
          }
          iter.set(resolvedUrn.get());
          modified = true;
        }
      }
    }
    return modified ? Optional.of(result) : Optional.empty();
  }

  private boolean shouldResolve(DatasetUrn urn, RetrieverContext retrieverContext) {
    if (!platforms.contains(urn.getPlatformEntity().getPlatformNameEntity())) {
      return false;
    }
    Map<Urn, Boolean> existsMap = retrieverContext.getAspectRetriever().entityExists(Set.of(urn));
    return !existsMap.get(urn);
  }

  private static boolean exists(String urnStr, RetrieverContext retrieverContext) {
    Urn urn = UrnUtils.getUrn(urnStr);
    Map<Urn, Boolean> existsMap = retrieverContext.getAspectRetriever().entityExists(Set.of(urn));
    return existsMap.get(urn);
  }

  private static Optional<DatasetUrn> resolve(DatasetUrn urn, RetrieverContext retrieverContext) {
    String platformResourceUrn =
        String.format(
            "urn:li:platformResource:%s.%s",
            urn.getPlatformEntity().getPlatformNameEntity(), urn.getDatasetNameEntity());
    if (!exists(platformResourceUrn, retrieverContext)) {
      return Optional.empty();
    }

    Aspect resourceInfoAspect =
        retrieverContext
            .getAspectRetriever()
            .getLatestAspectObject(
                UrnUtils.getUrn(platformResourceUrn), PLATFORM_RESOURCE_INFO_ASPECT_NAME);
    PlatformResourceInfo resourceInfo =
        RecordUtils.toRecordTemplate(PlatformResourceInfo.class, resourceInfoAspect.data());

    try {
      return Optional.of(DatasetUrn.createFromString(resourceInfo.getPrimaryKey()));
    } catch (URISyntaxException e) {
      log.error("Urn format error", e);
      return Optional.empty();
    }
  }
}
