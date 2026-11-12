package com.linkedin.datahub.graphql.resolvers.metrics;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.getQueryContext;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Metric;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Walks the parentMetric chain from the source metric up to the root, returning all ancestors in
 * nearest-first order. Used by the sidebar auto-expand cascade.
 */
@Slf4j
@RequiredArgsConstructor
public class ParentMetricsResolver implements DataFetcher<CompletableFuture<List<Metric>>> {

  private static final int MAX_DEPTH = 20;
  private static final Set<String> ASPECTS_TO_FETCH =
      Collections.singleton(Constants.METRIC_RELATIONSHIPS_ASPECT_NAME);

  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<List<Metric>> get(final DataFetchingEnvironment environment) {
    final QueryContext context = getQueryContext(environment);
    final Urn sourceUrn = UrnUtils.getUrn(((Entity) environment.getSource()).getUrn());

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final List<Urn> parentUrns = walkParentChain(context, sourceUrn);

            final List<Metric> result = new ArrayList<>(parentUrns.size());
            if (!parentUrns.isEmpty()) {
              final Map<Urn, EntityResponse> responses =
                  _entityClient.batchGetV2(
                      context.getOperationContext(),
                      Constants.METRIC_ENTITY_NAME,
                      new HashSet<>(parentUrns),
                      null);

              for (Urn parentUrn : parentUrns) {
                final EntityResponse response = responses.get(parentUrn);
                final Metric stub = new Metric();
                stub.setUrn(parentUrn.toString());
                stub.setType(EntityType.METRIC);
                if (response != null) {
                  final EnvelopedAspect keyAspect =
                      response.getAspects().get(Constants.METRIC_RELATIONSHIPS_ASPECT_NAME);
                  if (keyAspect != null) {
                    final com.linkedin.metric.MetricRelationships rel =
                        new com.linkedin.metric.MetricRelationships(keyAspect.getValue().data());
                    if (rel.hasParentMetric() && rel.getParentMetric() != null) {
                      final Metric grandparentStub = new Metric();
                      grandparentStub.setUrn(rel.getParentMetric().toString());
                      grandparentStub.setType(EntityType.METRIC);
                      stub.setParentMetric(grandparentStub);
                    }
                  }
                }
                result.add(stub);
              }
            }
            return result;
          } catch (Exception e) {
            throw new RuntimeException("Failed to load parent metrics for " + sourceUrn, e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  /**
   * Iteratively fetches the parentMetric chain starting from the source URN. Returns URNs in
   * nearest-first order (immediate parent first, root last). Guards against cycles with a visited
   * set and a depth cap.
   */
  private List<Urn> walkParentChain(final QueryContext context, final Urn sourceUrn)
      throws Exception {
    final List<Urn> chain = new ArrayList<>();
    final Set<Urn> visited = new HashSet<>();
    visited.add(sourceUrn);

    Urn current = resolveParent(context, sourceUrn);
    while (current != null && !visited.contains(current) && chain.size() < MAX_DEPTH) {
      chain.add(current);
      visited.add(current);
      current = resolveParent(context, current);
    }
    return chain;
  }

  private Urn resolveParent(final QueryContext context, final Urn urn) throws Exception {
    final Map<Urn, EntityResponse> responses =
        _entityClient.batchGetV2(
            context.getOperationContext(),
            Constants.METRIC_ENTITY_NAME,
            Collections.singleton(urn),
            ASPECTS_TO_FETCH);
    final EntityResponse response = responses.get(urn);
    if (response == null) {
      return null;
    }
    final EnvelopedAspect enveloped =
        response.getAspects().get(Constants.METRIC_RELATIONSHIPS_ASPECT_NAME);
    if (enveloped == null) {
      return null;
    }
    final com.linkedin.metric.MetricRelationships rel =
        new com.linkedin.metric.MetricRelationships(enveloped.getValue().data());
    return rel.hasParentMetric() ? rel.getParentMetric() : null;
  }
}
