package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.UPSTREAM_METRICS_ASPECT_NAME;

import com.datahub.authorization.AuthorizationSession;
import com.datahub.context.OperationFingerprint;
import com.datahub.util.RecordUtils;
import com.linkedin.common.Edge;
import com.linkedin.common.UpstreamMetrics;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.authorization.EntityAspectAuthorizationUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Requires LINEAGE UPDATE ({@code EDIT_LINEAGE} or {@code EDIT_ENTITY}) on the consumer and each
 * destination Metric on the current or proposed {@code metrics} list, matching GraphQL {@code
 * updateLineage} on edges to add and remove.
 */
@Setter
@Getter
@Accessors(chain = true)
public class UpstreamMetricsAuthorizationValidator extends AbstractAspectAuthorizationValidator {

  @Nonnull private AspectPluginConfig config;

  @Override
  protected List<AspectValidationException> validateItems(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull List<? extends BatchItem> items,
      @Nonnull Collection<? extends BatchItem> batchItems,
      @Nonnull RetrieverContext retrieverContext,
      @Nonnull AuthorizationSession session) {

    AspectRetriever aspectRetriever = retrieverContext.getAspectRetriever();
    Set<Urn> consumerUrns = items.stream().map(BatchItem::getUrn).collect(Collectors.toSet());
    Map<Urn, Map<String, Aspect>> currentAspects =
        consumerUrns.isEmpty()
            ? Map.of()
            : aspectRetriever.getLatestAspectObjects(
                operationContext, consumerUrns, Set.of(UPSTREAM_METRICS_ASPECT_NAME));

    List<AspectValidationException> failures = new ArrayList<>();
    for (BatchItem item : items) {
      if (!EntityAspectAuthorizationUtils.isAuthorizedToUpdateLineage(session, item.getUrn())) {
        failures.add(
            authFailure(item, "Unauthorized to modify lineage on entity: " + item.getUrn()));
        continue;
      }

      Aspect currentAspect =
          currentAspects.getOrDefault(item.getUrn(), Map.of()).get(UPSTREAM_METRICS_ASPECT_NAME);
      UpstreamMetrics proposed =
          UpstreamMetricsValidator.resolveProposed(item, aspectRetriever, currentAspect);
      Set<Urn> destinations = new HashSet<>();
      destinations.addAll(metricDestinations(toUpstreamMetrics(currentAspect)));
      destinations.addAll(metricDestinations(proposed));
      for (Urn destination : destinations) {
        if (!EntityAspectAuthorizationUtils.isAuthorizedToUpdateLineage(session, destination)) {
          failures.add(
              authFailure(item, "Unauthorized to modify lineage on entity: " + destination));
        }
      }
    }
    return failures;
  }

  @Nullable
  private static UpstreamMetrics toUpstreamMetrics(@Nullable Aspect aspect) {
    if (aspect == null) {
      return null;
    }
    return RecordUtils.toRecordTemplate(UpstreamMetrics.class, aspect.data());
  }

  private static Set<Urn> metricDestinations(@Nullable UpstreamMetrics aspect) {
    Set<Urn> destinations = new HashSet<>();
    if (aspect == null || !aspect.hasMetrics() || aspect.getMetrics() == null) {
      return destinations;
    }
    for (Edge edge : aspect.getMetrics()) {
      if (edge.getDestinationUrn() != null) {
        destinations.add(edge.getDestinationUrn());
      }
    }
    return destinations;
  }
}
