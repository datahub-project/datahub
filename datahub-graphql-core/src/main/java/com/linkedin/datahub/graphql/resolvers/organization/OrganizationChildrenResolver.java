package com.linkedin.datahub.graphql.resolvers.organization;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.Organization;
import com.linkedin.datahub.graphql.types.organization.mappers.OrganizationMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class OrganizationChildrenResolver
    implements DataFetcher<CompletableFuture<List<Organization>>> {

  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<List<Organization>> get(DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final String urnStr =
        ((com.linkedin.datahub.graphql.generated.Organization) environment.getSource()).getUrn();
    final Urn parentUrn = UrnUtils.getUrn(urnStr);

    if (!OrganizationAuthUtils.canViewOrganization(context, parentUrn)) {
      log.debug(
          "User {} is not authorized to view children for organization {}",
          context.getActorUrn(),
          urnStr);
      return CompletableFuture.completedFuture(java.util.Collections.emptyList());
    }

    log.debug("Fetching children for organization: {}", urnStr);
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            log.info("Searching for child organizations with parent URN: {}", urnStr);
            final Criterion criterion =
                new Criterion()
                    .setField("parent.keyword")
                    .setCondition(Condition.EQUAL)
                    .setValues(
                        new com.linkedin.data.template.StringArray(
                            java.util.Collections.singletonList(urnStr)));
            final ConjunctiveCriterion conjunctiveCriterion =
                new ConjunctiveCriterion()
                    .setAnd(new CriterionArray(Collections.singletonList(criterion)));
            final Filter filter =
                new Filter()
                    .setOr(
                        new ConjunctiveCriterionArray(
                            Collections.singletonList(conjunctiveCriterion)));

            final SearchResult searchResult =
                _entityClient.filter(
                    context.getOperationContext(), ORGANIZATION_ENTITY_NAME, filter, null, 0, 100);
            log.info(
                "Found {} child organizations for parent URN: {}",
                searchResult.getNumEntities(),
                urnStr);

            final List<Urn> childUrns =
                searchResult.getEntities().stream()
                    .map(SearchEntity::getEntity)
                    .collect(Collectors.toList());

            if (childUrns.isEmpty()) {
              return Collections.emptyList();
            }

            final Map<Urn, EntityResponse> responses =
                _entityClient.batchGetV2(
                    context.getOperationContext(),
                    ORGANIZATION_ENTITY_NAME,
                    new java.util.HashSet<>(childUrns),
                    null);

            final List<Organization> results = new ArrayList<>();
            for (Urn childUrn : childUrns) {
              if (responses.containsKey(childUrn)) {
                results.add(OrganizationMapper.map(context, responses.get(childUrn)));
              }
            }
            return results;
          } catch (Exception e) {
            throw new RuntimeException("Failed to load child organizations", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
