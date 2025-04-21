package com.linkedin.datahub.graphql.resolvers.assertion;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.Monitor;
import com.linkedin.datahub.graphql.types.monitor.MonitorMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/** Fetches the monitor associated with an assertion, or null if one does not exist. * */
@Slf4j
public class AssertionMonitorResolver implements DataFetcher<CompletableFuture<Monitor>> {
  static final String ASSERTION_MONITOR_RELATIONSHIP_NAME = "Evaluates";

  private final EntityClient entityClient;
  private final GraphClient graphClient;

  public AssertionMonitorResolver(
      @Nonnull final EntityClient entityClient, @Nonnull final GraphClient graphClient) {
    this.entityClient = Objects.requireNonNull(entityClient, "entityClient must not be null");
    this.graphClient = Objects.requireNonNull(graphClient, "graphClient must not be null");
  }

  @Override
  public CompletableFuture<Monitor> get(DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final Urn assertionUrn = UrnUtils.getUrn(((Assertion) environment.getSource()).getUrn());
    return CompletableFuture.supplyAsync(() -> getAssertionMonitor(context, assertionUrn));
  }

  @Nonnull
  private Monitor getAssertionMonitor(
      @Nonnull final QueryContext context, @Nonnull final Urn assertionUrn) {
    try {
      // Fetch the monitor associated with the assertion.
      final EntityRelationships relationships =
          this.graphClient.getRelatedEntities(
              assertionUrn.toString(),
              ImmutableSet.of(ASSERTION_MONITOR_RELATIONSHIP_NAME),
              RelationshipDirection.INCOMING,
              0,
              1,
              context.getActorUrn());

      // If we found multiple monitors for same entity, we have an invalid system state! Log
      // a warning.
      if (relationships.getTotal() > 1) {
        log.warn(
            String.format(
                "Unexpectedly found multiple monitors (%s) for assertion with urn %s! Returning the first. This may lead to inconsistent or unexpected behavior.",
                relationships.getRelationships(), assertionUrn));
      }

      final List<Urn> monitorUrns =
          relationships.getRelationships().stream()
              .map(EntityRelationship::getEntity)
              .filter(urn -> monitorExists(urn, context))
              .collect(Collectors.toList());

      if (!monitorUrns.isEmpty()) {
        // Extract the monitor.
        final Urn monitorUrn = monitorUrns.get(0);

        // Hydrate the contract entities based on the urns from step 1
        final EntityResponse entityResponse =
            this.entityClient.getV2(
                context.getOperationContext(), Constants.MONITOR_ENTITY_NAME, monitorUrn, null);

        if (entityResponse != null) {
          //  Package and return result
          return MonitorMapper.map(context, entityResponse);
        }
      }
      // No contract found
      return null;
    } catch (URISyntaxException | RemoteInvocationException e) {
      throw new RuntimeException("Failed to retrieve Assertion Run Events from GMS", e);
    }
  }

  private boolean monitorExists(
      @Nonnull final Urn monitorUrn, @Nonnull final QueryContext context) {
    try {
      return this.entityClient.exists(context.getOperationContext(), monitorUrn, false);
    } catch (Exception e) {
      throw new RuntimeException("Failed to check if monitor exists. Got bad response from GMS.");
    }
  }
}
