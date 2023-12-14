package com.linkedin.datahub.graphql.resolvers.glossary;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.GetRootGlossaryEntitiesInput;
import com.linkedin.datahub.graphql.generated.GetRootGlossaryNodesResult;
import com.linkedin.datahub.graphql.generated.GlossaryNode;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class GetRootGlossaryNodesResolver
    implements DataFetcher<CompletableFuture<GetRootGlossaryNodesResult>> {

  private final EntityClient _entityClient;

  public GetRootGlossaryNodesResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<GetRootGlossaryNodesResult> get(
      final DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();

    return CompletableFuture.supplyAsync(
        () -> {
          final GetRootGlossaryEntitiesInput input =
              bindArgument(environment.getArgument("input"), GetRootGlossaryEntitiesInput.class);
          final Integer start = input.getStart();
          final Integer count = input.getCount();

          try {
            final Filter filter = buildGlossaryEntitiesFilter();
            final SearchResult gmsNodesResult =
                _entityClient.filter(
                    Constants.GLOSSARY_NODE_ENTITY_NAME,
                    filter,
                    null,
                    start,
                    count,
                    context.getAuthentication());

            final List<Urn> glossaryNodeUrns =
                gmsNodesResult.getEntities().stream()
                    .map(SearchEntity::getEntity)
                    .collect(Collectors.toList());

            final GetRootGlossaryNodesResult result = new GetRootGlossaryNodesResult();
            result.setNodes(mapUnresolvedGlossaryNodes(glossaryNodeUrns));
            result.setCount(glossaryNodeUrns.size());
            result.setStart(gmsNodesResult.getFrom());
            result.setTotal(gmsNodesResult.getNumEntities());

            return result;
          } catch (RemoteInvocationException e) {
            throw new RuntimeException("Failed to retrieve root glossary nodes from GMS", e);
          }
        });
  }

  private Filter buildGlossaryEntitiesFilter() {
    CriterionArray array =
        new CriterionArray(
            ImmutableList.of(
                new Criterion()
                    .setField("hasParentNode")
                    .setValue("false")
                    .setCondition(Condition.EQUAL)));
    final Filter filter = new Filter();
    filter.setOr(
        new ConjunctiveCriterionArray(ImmutableList.of(new ConjunctiveCriterion().setAnd(array))));
    return filter;
  }

  private List<GlossaryNode> mapUnresolvedGlossaryNodes(final List<Urn> entityUrns) {
    final List<GlossaryNode> results = new ArrayList<>();
    for (final Urn urn : entityUrns) {
      final GlossaryNode unresolvedGlossaryNode = new GlossaryNode();
      unresolvedGlossaryNode.setUrn(urn.toString());
      unresolvedGlossaryNode.setType(EntityType.GLOSSARY_NODE);
      results.add(unresolvedGlossaryNode);
    }
    return results;
  }
}
