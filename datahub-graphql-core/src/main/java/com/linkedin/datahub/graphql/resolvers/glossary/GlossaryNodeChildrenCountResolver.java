package com.linkedin.datahub.graphql.resolvers.glossary;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.GlossaryNodeChildrenCount;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.utils.CriterionUtils;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class GlossaryNodeChildrenCountResolver
    implements DataFetcher<CompletableFuture<GlossaryNodeChildrenCount>> {
  private final EntityClient _entityClient;

  public GlossaryNodeChildrenCountResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<GlossaryNodeChildrenCount> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final String urn = ((Entity) environment.getSource()).getUrn();

    final CriterionArray andArray = new CriterionArray();
    Criterion parentNodeCriterion =
        CriterionUtils.buildCriterion("parentNode", Condition.EQUAL, urn);
    andArray.add(parentNodeCriterion);
    Filter finalFilter =
        new Filter()
            .setOr(new ConjunctiveCriterionArray(new ConjunctiveCriterion().setAnd(andArray)));

    final Urn entityUrn = Urn.createFromString(urn);
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            GlossaryNodeChildrenCount childrenCount = new GlossaryNodeChildrenCount();
            childrenCount.setTermsCount(0);
            childrenCount.setNodesCount(0);
            SearchResult result =
                _entityClient.searchAcrossEntities(
                    context.getOperationContext(),
                    ImmutableList.of(
                        Constants.GLOSSARY_TERM_ENTITY_NAME, Constants.GLOSSARY_NODE_ENTITY_NAME),
                    "*",
                    finalFilter,
                    0,
                    0, // 0 entity count because we don't want resolved entities
                    Collections.emptyList(),
                    ImmutableList.of("_entityType"));
            Optional<AggregationMetadata> aggMetadata =
                result.getMetadata().getAggregations().stream()
                    .filter(a -> a.getName().equals("_entityType"))
                    .findFirst();
            aggMetadata.ifPresent(
                aggregationMetadata ->
                    aggregationMetadata
                        .getFilterValues()
                        .forEach(
                            filterValue -> {
                              if (filterValue.getValue().equals("glossaryterm")) {
                                childrenCount.setTermsCount(filterValue.getFacetCount().intValue());
                              }
                              if (filterValue.getValue().equals("glossarynode")) {
                                childrenCount.setNodesCount(filterValue.getFacetCount().intValue());
                              }
                            }));

            return childrenCount;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to check whether entity %s exists", entityUrn.toString()));
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
