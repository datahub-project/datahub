package com.linkedin.datahub.graphql.resolvers.metrics;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.SearchAcrossEntitiesInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.generated.SemanticModel;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.common.mappers.SearchFlagsInputMapper;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver for {@code SemanticModel.entities}: searches metrics and datasets whose member-side
 * {@code semanticModel} field equals this SemanticModel URN. Membership is stored on {@code
 * metricInfo.semanticModel} / {@code semanticModelProperties.semanticModel}.
 */
@Slf4j
@RequiredArgsConstructor
public class ListSemanticModelEntitiesResolver
    implements DataFetcher<CompletableFuture<SearchResults>> {

  static final String SEMANTIC_MODEL_FIELD_NAME = "semanticModel";

  private static final int DEFAULT_START = 0;
  private static final int DEFAULT_COUNT = 10;
  private static final List<String> DEFAULT_ENTITY_NAMES =
      ImmutableList.of(Constants.METRIC_ENTITY_NAME, Constants.DATASET_ENTITY_NAME);

  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<SearchResults> get(DataFetchingEnvironment environment) {
    final QueryContext context = ResolverUtils.getQueryContext(environment);
    final String urn =
        environment.getArgument("urn") != null
            ? environment.getArgument("urn")
            : ((SemanticModel) environment.getSource()).getUrn();
    final SearchAcrossEntitiesInput boundInput =
        bindArgument(environment.getArgument("input"), SearchAcrossEntitiesInput.class);
    final SearchAcrossEntitiesInput input =
        boundInput != null ? boundInput : new SearchAcrossEntitiesInput();

    final List<EntityType> inputEntityTypes =
        (input.getTypes() == null || input.getTypes().isEmpty())
            ? ImmutableList.of()
            : input.getTypes();
    final List<String> finalEntityNames =
        !inputEntityTypes.isEmpty()
            ? inputEntityTypes.stream()
                .map(EntityTypeMapper::getName)
                .distinct()
                .collect(Collectors.toList())
            : DEFAULT_ENTITY_NAMES;

    final String query = input.getQuery();
    final String sanitizedQuery = query != null ? ResolverUtils.escapeForwardSlash(query) : null;
    final int start = input.getStart() != null ? input.getStart() : DEFAULT_START;
    final int count = input.getCount() != null ? input.getCount() : DEFAULT_COUNT;

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final Criterion membershipCriterion =
              buildCriterion(SEMANTIC_MODEL_FIELD_NAME, Condition.EQUAL, urn);
          final Filter membershipFilter =
              new Filter()
                  .setOr(
                      new ConjunctiveCriterionArray(
                          new ConjunctiveCriterion()
                              .setAnd(new CriterionArray(membershipCriterion))));
          final Filter inputFilter = ResolverUtils.buildFilter(null, input.getOrFilters());
          final Filter finalFilter =
              inputFilter == null
                  ? membershipFilter
                  : com.linkedin.datahub.graphql.resolvers.search.SearchUtils.combineFilters(
                      inputFilter, membershipFilter);

          final SearchFlags searchFlags;
          com.linkedin.datahub.graphql.generated.SearchFlags inputFlags = input.getSearchFlags();
          if (inputFlags != null) {
            searchFlags = SearchFlagsInputMapper.INSTANCE.apply(context, inputFlags);
          } else {
            searchFlags = null;
          }

          try {
            return UrnSearchResultsMapper.map(
                context,
                _entityClient.searchAcrossEntities(
                    context
                        .getOperationContext()
                        .withSearchFlags(flags -> searchFlags != null ? searchFlags : flags),
                    finalEntityNames,
                    sanitizedQuery != null ? sanitizedQuery : "*",
                    finalFilter,
                    start,
                    count,
                    null));
          } catch (Exception e) {
            log.error(
                "Failed to execute search for semantic model entities: entity types {}, query {}, start: {}, count: {}",
                finalEntityNames,
                input.getQuery(),
                start,
                count,
                e);
            throw new RuntimeException(
                "Failed to execute search for semantic model entities: "
                    + String.format(
                        "entity types %s, query %s, start: %s, count: %s",
                        finalEntityNames, input.getQuery(), start, count),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
