package com.linkedin.datahub.graphql.resolvers.dataproduct;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataProduct;
import com.linkedin.datahub.graphql.generated.DataProductEntitiesInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.resolvers.EntityTypeMapper;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.SEARCHABLE_ENTITY_TYPES;


/**
 * Retrieves a list of entities associated with a given data product
 */
@Slf4j
public class DataProductEntitiesResolver implements DataFetcher<CompletableFuture<SearchResults>> {

  private static final String DATA_PRODUCT_FIELD_NAME = "dataProducts";
  private static final String INPUT_ARG_NAME = "input";
  private static final String DEFAULT_QUERY = "*";
  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 20;
  private static final DataProductEntitiesInput DEFAULT_ENTITIES_INPUT = new DataProductEntitiesInput();

  static {
    DEFAULT_ENTITIES_INPUT.setQuery(DEFAULT_QUERY);
    DEFAULT_ENTITIES_INPUT.setStart(DEFAULT_START);
    DEFAULT_ENTITIES_INPUT.setCount(DEFAULT_COUNT);
  }

  private final EntityClient _entityClient;

  public DataProductEntitiesResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<SearchResults> get(final DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();
    final String urn = ((DataProduct) environment.getSource()).getUrn();

    final DataProductEntitiesInput input = environment.getArgument(INPUT_ARG_NAME) != null
        ? bindArgument(environment.getArgument(INPUT_ARG_NAME), DataProductEntitiesInput.class)
        : DEFAULT_ENTITIES_INPUT;

    final String query = input.getQuery() != null ? input.getQuery() : "*";
    final int start = input.getStart() != null ? input.getStart() : 0;
    final int count = input.getCount() != null ? input.getCount() : 20;

    return CompletableFuture.supplyAsync(() -> {

      try {

        final Criterion filterCriterion =  new Criterion()
            .setField(DATA_PRODUCT_FIELD_NAME + ".keyword")
            .setCondition(Condition.EQUAL)
            .setValue(urn);

        return UrnSearchResultsMapper.map(_entityClient.searchAcrossEntities(
            SEARCHABLE_ENTITY_TYPES.stream().map(EntityTypeMapper::getName).collect(Collectors.toList()),
            query,
            new Filter().setOr(new ConjunctiveCriterionArray(
                new ConjunctiveCriterion().setAnd(new CriterionArray(ImmutableList.of(filterCriterion)))
            )),
            start,
            count,
            null,
            context.getAuthentication()
        ));

      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Failed to resolve entities associated with data product with urn %s", urn), e);
      }
    });
  }
}