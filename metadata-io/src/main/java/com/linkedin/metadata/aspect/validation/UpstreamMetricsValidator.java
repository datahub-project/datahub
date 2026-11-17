package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.METRIC_ENTITY_NAME;
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
import com.linkedin.metric.MetricUpstreams;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Rejects {@code upstreamMetrics} writes whose destinations are not Metrics, and Dataset↔Metric
 * 2-cycles: a Dataset cannot both consume a Metric here and appear on that Metric's {@code
 * metricUpstreams.datasetUpstreams}.
 */
@Setter
@Getter
@Accessors(chain = true)
public class UpstreamMetricsValidator extends AspectPayloadValidator {
  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();
    AspectRetriever aspectRetriever = retrieverContext.getAspectRetriever();

    Set<Urn> consumerUrns = mcpItems.stream().map(BatchItem::getUrn).collect(Collectors.toSet());
    Map<Urn, Map<String, Aspect>> currentAspects =
        consumerUrns.isEmpty()
            ? Map.of()
            : aspectRetriever.getLatestAspectObjects(
                operationContext, consumerUrns, Set.of(UPSTREAM_METRICS_ASPECT_NAME));

    Set<Urn> destinationMetricUrns = new HashSet<>();
    for (BatchItem item : mcpItems) {
      Aspect currentAspect =
          currentAspects.getOrDefault(item.getUrn(), Map.of()).get(UPSTREAM_METRICS_ASPECT_NAME);
      UpstreamMetrics proposed = resolveProposed(item, aspectRetriever, currentAspect);
      if (proposed == null || !proposed.hasMetrics() || proposed.getMetrics() == null) {
        continue;
      }
      for (Edge edge : proposed.getMetrics()) {
        if (edge.getDestinationUrn() != null
            && METRIC_ENTITY_NAME.equals(edge.getDestinationUrn().getEntityType())) {
          destinationMetricUrns.add(edge.getDestinationUrn());
        }
      }
    }

    Map<Urn, Map<String, Aspect>> metricAspects =
        destinationMetricUrns.isEmpty()
            ? Map.of()
            : aspectRetriever.getLatestAspectObjects(
                operationContext, destinationMetricUrns, Set.of(METRIC_UPSTREAMS_ASPECT_NAME));

    mcpItems.forEach(
        item -> {
          Aspect currentAspect =
              currentAspects
                  .getOrDefault(item.getUrn(), Map.of())
                  .get(UPSTREAM_METRICS_ASPECT_NAME);
          UpstreamMetrics proposed = resolveProposed(item, aspectRetriever, currentAspect);
          validateUpstreamMetrics(item, proposed, metricAspects, exceptions);
        });
    return exceptions.streamAllExceptions();
  }

  @Nullable
  private static UpstreamMetrics toUpstreamMetrics(@Nullable Aspect aspect) {
    if (aspect == null) {
      return null;
    }
    return RecordUtils.toRecordTemplate(UpstreamMetrics.class, aspect.data());
  }

  @Nullable
  static UpstreamMetrics resolveProposed(
      @Nonnull BatchItem item,
      @Nonnull AspectRetriever aspectRetriever,
      @Nullable Aspect currentAspect) {
    UpstreamMetrics current = toUpstreamMetrics(currentAspect);
    if (ChangeType.PATCH.equals(item.getChangeType()) && item instanceof MCPItem) {
      PatchItemImpl patchItem =
          item instanceof PatchItemImpl
              ? (PatchItemImpl) item
              : PatchItemImpl.builder()
                  .build(
                      ((MCPItem) item).getMetadataChangeProposal(),
                      item.getAuditStamp(),
                      aspectRetriever.getEntityRegistry());
      return patchItem.applyPatch(current, aspectRetriever).getAspect(UpstreamMetrics.class);
    }
    return item.getAspect(UpstreamMetrics.class);
  }

  private void validateUpstreamMetrics(
      BatchItem item,
      UpstreamMetrics proposed,
      Map<Urn, Map<String, Aspect>> metricAspects,
      ValidationExceptionCollection exceptions) {
    if (proposed == null || !proposed.hasMetrics() || proposed.getMetrics() == null) {
      return;
    }

    Set<Urn> destinationMetrics = new HashSet<>();
    for (Edge edge : proposed.getMetrics()) {
      Urn destination = edge.getDestinationUrn();
      if (destination == null) {
        exceptions.addException(
            AspectValidationException.forItem(
                item, "upstreamMetrics.metrics edge is missing destinationUrn"));
        continue;
      }
      if (!METRIC_ENTITY_NAME.equals(destination.getEntityType())) {
        exceptions.addException(
            AspectValidationException.forItem(
                item,
                String.format(
                    "upstreamMetrics.metrics destination must be a metric URN: %s", destination)));
        continue;
      }
      destinationMetrics.add(destination);
    }

    if (!DATASET_ENTITY_NAME.equals(item.getUrn().getEntityType())) {
      return;
    }

    for (Urn metricUrn : destinationMetrics) {
      Aspect metricUpstreamsAspect =
          metricAspects.getOrDefault(metricUrn, Map.of()).get(METRIC_UPSTREAMS_ASPECT_NAME);
      if (metricUpstreamsAspect == null) {
        continue;
      }
      MetricUpstreams metricUpstreams =
          RecordUtils.toRecordTemplate(MetricUpstreams.class, metricUpstreamsAspect.data());
      if (!metricUpstreams.hasDatasetUpstreams() || metricUpstreams.getDatasetUpstreams() == null) {
        continue;
      }
      boolean cycles =
          metricUpstreams.getDatasetUpstreams().stream()
              .anyMatch(
                  edge ->
                      edge.getDestinationUrn() != null
                          && edge.getDestinationUrn().equals(item.getUrn()));
      if (cycles) {
        exceptions.addException(
            AspectValidationException.forItem(
                item,
                String.format(
                    "Dataset %s cannot consume Metric %s via upstreamMetrics because that Metric already lists the Dataset in metricUpstreams.datasetUpstreams",
                    item.getUrn(), metricUrn)));
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
