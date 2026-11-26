package com.linkedin.datahub.graphql.resolvers.search;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.getMaxHops;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.AndFilterInput;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.SearchAcrossLineageCounts;
import com.linkedin.datahub.graphql.generated.SearchAcrossLineageCountsInput;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.common.mappers.LineageFlagsInputMapper;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.query.GroupingSpec;
import com.linkedin.metadata.query.LineageFlags;
import com.linkedin.metadata.query.SchemaFieldValidationMode;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.LineageSearchResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Counts what a graph query on a node reaches, without returning any of it.
 *
 * <p>Returning nothing is the point: the counts are wanted for entities that were never
 * materialized, which cannot be hydrated and would be stripped back out of a result set by the
 * existence checks every other lineage search passes through. Counting them means reading the count
 * off the graph instead of the entity index, which is what {@code forceLightningMode} switches the
 * search service to -- see the caveats on that flag and on {@code includeGhostEntities}.
 */
@Slf4j
@RequiredArgsConstructor
public class SearchAcrossLineageCountsResolver
    implements DataFetcher<CompletableFuture<SearchAcrossLineageCounts>> {

  private static final int COUNT_ONLY = 0;

  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<SearchAcrossLineageCounts> get(DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final SearchAcrossLineageCountsInput input =
        bindArgument(environment.getArgument("input"), SearchAcrossLineageCountsInput.class);

    final Urn urn = UrnUtils.requireUrn(input.getUrn());
    final LineageDirection direction = LineageDirection.valueOf(input.getDirection().toString());
    final List<String> entityNames =
        input.getTypes() == null
            ? Collections.emptyList()
            : input.getTypes().stream().map(EntityTypeMapper::getName).collect(Collectors.toList());

    final List<AndFilterInput> orFilters =
        input.getOrFilters() != null ? input.getOrFilters() : new ArrayList<>();
    final List<FacetFilterInput> facetFilters =
        orFilters.stream()
            .map(AndFilterInput::getAnd)
            .flatMap(List::stream)
            .collect(Collectors.toList());
    final Integer maxHops = getMaxHops(facetFilters);
    final Filter filter = ResolverUtils.buildFilter(null, input.getOrFilters());

    final LineageFlags lineageFlags = LineageFlagsInputMapper.map(context, input.getLineageFlags());
    if (Boolean.TRUE.equals(input.getIncludeGhostEntities())) {
      lineageFlags.setForceLightningMode(true);
    }
    // The service defaults to NONE for the sake of callers that never asked; a count is where a
    // stale column actually misleads, so this query opts in unless told otherwise
    lineageFlags.setValidateSchemaFields(
        input.getValidateSchemaFields() == null
            ? SchemaFieldValidationMode.AUTO
            : SchemaFieldValidationMode.valueOf(input.getValidateSchemaFields().toString()));

    // Fixed rather than exposed: the empty grouping spec keeps a count of schema fields from being
    // folded into a count of the datasets holding them, and version filtering would drop relations
    // the graph still draws
    final SearchFlags searchFlags =
        new SearchFlags()
            .setFulltext(false)
            .setSkipHighlighting(true)
            .setSkipAggregates(true)
            .setGroupingSpec(new GroupingSpec())
            .setFilterNonLatestVersions(false)
            .setIncludeSoftDeleted(Boolean.TRUE.equals(input.getIncludeSoftDeleted()));

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final LineageSearchResult result =
                _entityClient.searchAcrossLineage(
                    context
                        .getOperationContext()
                        .withSearchFlags(flags -> searchFlags)
                        .withLineageFlags(flags -> lineageFlags),
                    urn,
                    direction,
                    entityNames,
                    null,
                    maxHops,
                    filter,
                    Collections.emptyList(),
                    0,
                    COUNT_ONLY);

            final SearchAcrossLineageCounts counts = new SearchAcrossLineageCounts();
            counts.setTotal(result.getNumEntities());
            return counts;
          } catch (Exception e) {
            log.error(
                "Failed to count lineage for urn {}, direction {}, types {}",
                urn,
                direction,
                entityNames,
                e);
            throw new RuntimeException(
                String.format("Failed to count lineage for urn %s, direction %s", urn, direction),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
