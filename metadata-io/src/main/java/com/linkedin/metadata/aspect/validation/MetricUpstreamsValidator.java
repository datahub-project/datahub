package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.METRIC_UPSTREAMS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.UPSTREAM_METRICS_ASPECT_NAME;

import com.datahub.context.OperationFingerprint;
import com.datahub.util.RecordUtils;
import com.linkedin.common.Edge;
import com.linkedin.common.UpstreamMetrics;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import com.linkedin.metric.MetricUpstreams;
import com.linkedin.util.Pair;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Rejects {@code metricUpstreams} writes when a column's parent dataset is missing from {@code
 * datasetUpstreams}. Absent or empty {@code fieldUpstreams} is valid. An empty array clears
 * previous column edges.
 */
@Setter
@Getter
@Accessors(chain = true)
public class MetricUpstreamsValidator extends AspectPayloadValidator {
  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();
    AspectRetriever aspectRetriever = retrieverContext.getAspectRetriever();

    Set<Urn> urns = mcpItems.stream().map(BatchItem::getUrn).collect(Collectors.toSet());
    Map<Urn, Map<String, Aspect>> currentAspects =
        urns.isEmpty()
            ? Map.of()
            : aspectRetriever.getLatestAspectObjects(
                operationContext, urns, Set.of(METRIC_UPSTREAMS_ASPECT_NAME));

    Set<Urn> datasetUrns = new HashSet<>();
    mcpItems.forEach(
        item -> {
          Aspect currentAspect =
              currentAspects
                  .getOrDefault(item.getUrn(), Map.of())
                  .get(METRIC_UPSTREAMS_ASPECT_NAME);
          MetricUpstreams proposed = resolveProposedUpstreams(item, aspectRetriever, currentAspect);
          datasetUrns.addAll(datasetUpstreamUrns(proposed));
        });

    Map<Urn, Map<String, Aspect>> datasetAspects =
        datasetUrns.isEmpty()
            ? Map.of()
            : aspectRetriever.getLatestAspectObjects(
                operationContext, datasetUrns, Set.of(UPSTREAM_METRICS_ASPECT_NAME));

    mcpItems.forEach(
        item -> {
          Aspect currentAspect =
              currentAspects
                  .getOrDefault(item.getUrn(), Map.of())
                  .get(METRIC_UPSTREAMS_ASPECT_NAME);
          MetricUpstreams proposed = resolveProposedUpstreams(item, aspectRetriever, currentAspect);
          validateMetricUpstreams(item, proposed, exceptions);
          validateNoDatasetMetricTwoCycle(item, proposed, datasetAspects, exceptions);
        });
    return exceptions.streamAllExceptions();
  }

  @Nullable
  private static MetricUpstreams toUpstreams(@Nullable Aspect aspect) {
    if (aspect == null) {
      return null;
    }
    return RecordUtils.toRecordTemplate(MetricUpstreams.class, aspect.data());
  }

  @Nullable
  private static MetricUpstreams resolveProposedUpstreams(
      @Nonnull BatchItem item,
      @Nonnull AspectRetriever aspectRetriever,
      @Nullable Aspect currentAspect) {
    MetricUpstreams current = toUpstreams(currentAspect);
    // Default ingest builds PatchItemImpl; alternate MCP validation uses ProposedItem (also an
    // MCPItem). Merge against the stored aspect in both cases.
    if (ChangeType.PATCH.equals(item.getChangeType()) && item instanceof MCPItem) {
      PatchItemImpl patchItem =
          item instanceof PatchItemImpl
              ? (PatchItemImpl) item
              : PatchItemImpl.builder()
                  .build(
                      ((MCPItem) item).getMetadataChangeProposal(),
                      item.getAuditStamp(),
                      aspectRetriever.getEntityRegistry());
      return patchItem.applyPatch(current, aspectRetriever).getAspect(MetricUpstreams.class);
    }
    return item.getAspect(MetricUpstreams.class);
  }

  private void validateMetricUpstreams(
      BatchItem item, MetricUpstreams upstreams, ValidationExceptionCollection exceptions) {
    if (upstreams == null
        || !upstreams.hasFieldUpstreams()
        || upstreams.getFieldUpstreams() == null) {
      return;
    }
    if (upstreams.getFieldUpstreams().isEmpty()) {
      return;
    }

    Set<Urn> datasetUrns = datasetUpstreamUrns(upstreams);

    for (Edge fieldEdge : upstreams.getFieldUpstreams()) {
      Urn fieldUrn = fieldEdge.getDestinationUrn();
      if (fieldUrn == null) {
        exceptions.addException(
            AspectValidationException.forItem(
                item, "metricUpstreams.fieldUpstreams edge is missing destinationUrn"));
        continue;
      }
      Optional<Pair<Urn, String>> parsed = SchemaFieldUtils.parseSchemaFieldUrn(fieldUrn);
      if (parsed.isEmpty()) {
        exceptions.addException(
            AspectValidationException.forItem(
                item,
                String.format(
                    "metricUpstreams.fieldUpstreams destination must be a schemaField URN: %s",
                    fieldUrn)));
        continue;
      }
      Urn parentDataset = parsed.get().getFirst();
      if (!datasetUrns.contains(parentDataset)) {
        exceptions.addException(
            AspectValidationException.forItem(
                item,
                String.format(
                    "schemaField %s parent dataset %s is not in metricUpstreams.datasetUpstreams",
                    fieldUrn, parentDataset)));
      }
    }
  }

  private static Set<Urn> datasetUpstreamUrns(@Nullable MetricUpstreams upstreams) {
    Set<Urn> datasetUrns = new HashSet<>();
    if (upstreams == null
        || !upstreams.hasDatasetUpstreams()
        || upstreams.getDatasetUpstreams() == null) {
      return datasetUrns;
    }
    for (Edge datasetEdge : upstreams.getDatasetUpstreams()) {
      if (datasetEdge.getDestinationUrn() != null) {
        datasetUrns.add(datasetEdge.getDestinationUrn());
      }
    }
    return datasetUrns;
  }

  private void validateNoDatasetMetricTwoCycle(
      BatchItem item,
      @Nullable MetricUpstreams proposed,
      Map<Urn, Map<String, Aspect>> datasetAspects,
      ValidationExceptionCollection exceptions) {
    Urn metricUrn = item.getUrn();
    for (Urn datasetUrn : datasetUpstreamUrns(proposed)) {
      Aspect upstreamMetricsAspect =
          datasetAspects.getOrDefault(datasetUrn, Map.of()).get(UPSTREAM_METRICS_ASPECT_NAME);
      if (upstreamMetricsAspect == null) {
        continue;
      }
      UpstreamMetrics upstreamMetrics =
          RecordUtils.toRecordTemplate(UpstreamMetrics.class, upstreamMetricsAspect.data());
      if (!upstreamMetrics.hasMetrics() || upstreamMetrics.getMetrics() == null) {
        continue;
      }
      boolean cycles =
          upstreamMetrics.getMetrics().stream()
              .anyMatch(
                  edge ->
                      edge.getDestinationUrn() != null
                          && edge.getDestinationUrn().equals(metricUrn));
      if (cycles) {
        exceptions.addException(
            AspectValidationException.forItem(
                item,
                String.format(
                    "Metric %s cannot list Dataset %s in metricUpstreams.datasetUpstreams because that Dataset already consumes the Metric via upstreamMetrics",
                    metricUrn, datasetUrn)));
      }
    }
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }
}
