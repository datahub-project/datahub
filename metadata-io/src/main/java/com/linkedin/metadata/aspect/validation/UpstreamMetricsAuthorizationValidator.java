package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.UPSTREAM_METRICS_ASPECT_NAME;

import com.datahub.authorization.AuthorizationSession;
import com.datahub.context.OperationFingerprint;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Requires LINEAGE UPDATE ({@code EDIT_LINEAGE} or {@code EDIT_ENTITY}) on the consumer and each
 * destination Metric for any write to {@code upstreamMetrics}, matching GraphQL {@code
 * updateLineage}.
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
      if (proposed == null || !proposed.hasMetrics() || proposed.getMetrics() == null) {
        continue;
      }
      for (Edge edge : proposed.getMetrics()) {
        Urn destination = edge.getDestinationUrn();
        if (destination == null) {
          continue;
        }
        if (!EntityAspectAuthorizationUtils.isAuthorizedToUpdateLineage(session, destination)) {
          failures.add(
              authFailure(item, "Unauthorized to modify lineage on entity: " + destination));
        }
      }
    }
    return failures;
  }
}
