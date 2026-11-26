package com.linkedin.datahub.graphql.loaders;

import static com.linkedin.datahub.graphql.resolvers.container.ContainerEntitiesResolver.CONTAINABLE_ENTITY_NAMES;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.SearchResult;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.dataloader.BatchLoaderContextProvider;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderOptions;

/**
 * Per-request DataLoader resolving {@code Container.entities.total} for the count-only selections
 * in the search fragments, collapsing the previous N {@code searchAcrossEntities} calls into a
 * fixed number of aggregation-only searches.
 *
 * <p>Each key is a container urn. Keys are chunked, and each chunk issues one aggregation-only
 * search faceted on {@code container} and filtered to the chunk's containers. The {@code container}
 * facet buckets are keyed by container urn, so per-container totals read back unambiguously.
 *
 * <p>Unlike the domain equivalent there is no entity-type dimension to group by: {@code
 * Container.entities} has a single call shape in the UI and the fast path only accepts unfiltered,
 * count-only requests, so the key is the bare urn.
 *
 * <p><b>Bucket-cap caveat.</b> This relies on a terms aggregation whose bucket count is capped at
 * {@code min(maxAggValues, maxTermBucketSize)}. Because the query is already filtered to the
 * chunk's containers, the {@code container} bucket only needs to hold those containers. We chunk
 * conservatively ({@link #MAX_CONTAINERS_PER_AGG}) and raise {@code maxAggValues}.
 */
@Slf4j
public final class ContainerEntityCountsBatchLoader {

  public static final String LOADER_NAME = "ContainerEntityCounts";

  private static final String CONTAINER_FIELD = "container";
  private static final String CONTAINER_KEYWORD_FIELD = CONTAINER_FIELD + ".keyword";
  private static final String MATCH_ALL_QUERY = "*";

  // Keep chunks safely under the search-layer terms-bucket cap (default maxTermBucketSize == 60).
  // An asset has at most one container, so the bucket holds no off-chunk keys, but we leave
  // headroom rather than sitting on the limit.
  private static final int MAX_CONTAINERS_PER_AGG = 25;
  private static final int MAX_AGG_VALUES = 1000;

  private ContainerEntityCountsBatchLoader() {}

  public static DataLoader<String, Long> create(
      final EntityClient entityClient, final QueryContext queryContext) {
    final BatchLoaderContextProvider provider = () -> queryContext;
    final DataLoaderOptions options =
        DataLoaderOptions.newOptions().setBatchLoaderContextProvider(provider);

    // Parent the batchLoad span under the operation, not the executor thread (see
    // GmsGraphQLEngine#createDataLoader).
    final Context batchContext = Context.current();

    return DataLoader.newDataLoader(
        (keys, env) ->
            GraphQLConcurrencyUtils.supplyAsync(
                () -> {
                  try (Scope ignored = batchContext.makeCurrent()) {
                    return batchLoad(keys, (QueryContext) env.getContext(), entityClient);
                  }
                },
                LOADER_NAME,
                "batchLoad"),
        options);
  }

  @WithSpan
  public static List<Long> batchLoad(
      final List<String> containerUrns,
      final QueryContext queryContext,
      final EntityClient entityClient) {
    // urn -> total, defaulting to zero (a container with no visible children stays zero).
    final Map<String, Long> resultByUrn = new HashMap<>(containerUrns.size());
    for (String urn : containerUrns) {
      resultByUrn.put(urn, 0L);
    }

    final List<String> distinctUrns =
        containerUrns.stream().distinct().collect(Collectors.toList());

    for (List<String> chunk : partition(distinctUrns, MAX_CONTAINERS_PER_AGG)) {
      try {
        resultByUrn.putAll(loadChunk(queryContext, entityClient, chunk));
      } catch (Exception e) {
        // Surface the failure rather than swallowing it: a returned 0 is indistinguishable from a
        // genuinely empty container and would mask a search outage as "no entities" in the UI
        // counts. Matches the throw-on-failure contract of the direct resolver path
        // (ContainerEntitiesResolver#resolveDirect) and the other GMS batch loaders.
        throw new RuntimeException(
            String.format("Failed to resolve entity counts associated with Containers %s", chunk),
            e);
      }
    }

    // DataLoader contract: results[i] must correspond to keys[i].
    final List<Long> ordered = new ArrayList<>(containerUrns.size());
    for (String urn : containerUrns) {
      ordered.add(resultByUrn.get(urn));
    }
    return ordered;
  }

  private static Map<String, Long> loadChunk(
      final QueryContext queryContext,
      final EntityClient entityClient,
      final List<String> containerUrns)
      throws Exception {

    final OperationContext opContext =
        queryContext
            .getOperationContext()
            .withSearchFlags(
                flags -> flags.setMaxAggValues(MAX_AGG_VALUES).setIncludeDefaultFacets(false));

    final SearchResult searchResult =
        entityClient.searchAcrossEntities(
            opContext,
            CONTAINABLE_ENTITY_NAMES,
            MATCH_ALL_QUERY,
            buildFilter(containerUrns),
            0,
            0,
            Collections.emptyList(),
            Collections.singletonList(CONTAINER_FIELD));

    final Map<String, Long> countsByContainer = new HashMap<>();
    if (searchResult == null) {
      return countsByContainer;
    }
    // `metadata`, its `aggregations` array (which defaults to []), and each entry's own
    // `aggregations` map are all required PDL fields, so none can be null here — their getters
    // throw when absent. A malformed response therefore surfaces as an exception that batchLoad
    // propagates, rather than as a null to guard against.
    for (AggregationMetadata agg : searchResult.getMetadata().getAggregations()) {
      if (!CONTAINER_FIELD.equals(agg.getName())) {
        continue;
      }
      // container-facet bucket keys are container urns; ignore anything outside the chunk.
      agg.getAggregations()
          .forEach(
              (containerUrn, count) -> {
                if (containerUrns.contains(containerUrn)) {
                  countsByContainer.merge(containerUrn, count, Long::sum);
                }
              });
    }
    return countsByContainer;
  }

  private static Filter buildFilter(final List<String> containerUrns) {
    final StringArray urnValues = new StringArray();
    for (String urn : containerUrns) {
      try {
        urnValues.add(UrnUtils.getUrn(urn).toString());
      } catch (Exception e) {
        log.warn("Skipping malformed container urn '{}' in container entity count batch.", urn, e);
      }
    }

    final CriterionArray criteria = new CriterionArray();
    criteria.add(buildCriterion(CONTAINER_KEYWORD_FIELD, Condition.EQUAL, urnValues));
    return new Filter()
        .setOr(new ConjunctiveCriterionArray(new ConjunctiveCriterion().setAnd(criteria)));
  }

  private static <T> List<List<T>> partition(final List<T> list, final int size) {
    final List<List<T>> chunks = new ArrayList<>();
    for (int i = 0; i < list.size(); i += size) {
      chunks.add(list.subList(i, Math.min(i + size, list.size())));
    }
    return chunks;
  }
}
