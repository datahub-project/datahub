package com.linkedin.datahub.graphql.resolvers.organization;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.GetEntitiesByOrganizationResult;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver for getting all entities belonging to an organization. Supports filtering by entity
 * type.
 */
@Slf4j
@RequiredArgsConstructor
public class GetEntitiesByOrganizationResolver
    implements DataFetcher<CompletableFuture<GetEntitiesByOrganizationResult>> {
  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<GetEntitiesByOrganizationResult> get(
      final DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final String organizationUrnStr = environment.getArgument("organizationUrn");

    // Get entityTypes - GraphQL passes these as raw objects (Strings), not
    // EntityType enums
    final List<?> entityTypesRaw = environment.getArgument("entityTypes");
    final List<EntityType> entityTypes =
        entityTypesRaw != null
            ? entityTypesRaw.stream()
                .filter(Objects::nonNull)
                .map(
                    obj -> {
                      try {
                        // GraphQL passes enum values as strings, convert to EntityType
                        return EntityType.valueOf(obj.toString());
                      } catch (IllegalArgumentException e) {
                        log.warn("Invalid entity type: {}", obj);
                        return null;
                      }
                    })
                .filter(Objects::nonNull)
                .collect(Collectors.toList())
            : null;

    final Integer start = environment.getArgument("start");
    final Integer count = environment.getArgument("count");

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            // Validate organization URN
            final Urn organizationUrn = Urn.createFromString(organizationUrnStr);

            if (!OrganizationAuthUtils.canViewOrganization(context, organizationUrn)) {
              throw new com.linkedin.datahub.graphql.exception.AuthorizationException(
                  "Unauthorized to view entities for this organization.");
            }

            // Build filter for organizations field
            final Criterion organizationCriterion = new Criterion();
            organizationCriterion.setField("organizations.keyword");
            organizationCriterion.setCondition(Condition.EQUAL);
            organizationCriterion.setValues(new StringArray(organizationUrnStr));

            final CriterionArray criteriaArray = new CriterionArray();
            criteriaArray.add(organizationCriterion);

            final ConjunctiveCriterion conjunctiveCriterion = new ConjunctiveCriterion();
            conjunctiveCriterion.setAnd(criteriaArray);

            final ConjunctiveCriterionArray conjunctiveCriterionArray =
                new ConjunctiveCriterionArray();
            conjunctiveCriterionArray.add(conjunctiveCriterion);

            final Filter filter = new Filter();
            filter.setOr(conjunctiveCriterionArray);

            // Determine entity types to search
            final List<String> entityTypeNames;
            if (entityTypes != null && !entityTypes.isEmpty()) {
              entityTypeNames =
                  entityTypes.stream()
                      .map(this::mapEntityTypeToName)
                      .filter(Objects::nonNull)
                      .collect(Collectors.toList());
            } else {
              entityTypeNames = java.util.Collections.emptyList(); // Search all entity types
            }

            // Execute search
            final SearchResult searchResult =
                _entityClient.searchAcrossEntities(
                    context.getOperationContext(),
                    entityTypeNames,
                    "*", // Match all
                    filter,
                    start,
                    count,
                    java.util.Collections.emptyList());

            // Map results
            final GetEntitiesByOrganizationResult result = new GetEntitiesByOrganizationResult();
            result.setStart(searchResult.getFrom());
            result.setCount(searchResult.getPageSize());
            result.setTotal(searchResult.getNumEntities());
            // Safely handle possible null entities list
            java.util.List<com.linkedin.metadata.search.SearchEntity> entities =
                (searchResult.getEntities() != null)
                    ? searchResult.getEntities()
                    : java.util.Collections.emptyList();
            result.setEntities(
                entities.stream()
                    .map(entity -> UrnToEntityMapper.map(context, entity.getEntity()))
                    .filter(java.util.Objects::nonNull)
                    .collect(java.util.stream.Collectors.toList()));

            return result;
          } catch (Exception e) {
            log.error("Failed to get entities by organization", e);
            throw new RuntimeException(
                "Failed to get entities by organization: " + e.getMessage(), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private String mapEntityTypeToName(final EntityType entityType) {
    // Map GraphQL EntityType enum to entity type name string
    switch (entityType) {
      case DATASET:
        return com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
      case CHART:
        return com.linkedin.metadata.Constants.CHART_ENTITY_NAME;
      case DASHBOARD:
        return com.linkedin.metadata.Constants.DASHBOARD_ENTITY_NAME;
      case DATA_FLOW:
        return com.linkedin.metadata.Constants.DATA_FLOW_ENTITY_NAME;
      case DATA_JOB:
        return com.linkedin.metadata.Constants.DATA_JOB_ENTITY_NAME;
      case TAG:
        return com.linkedin.metadata.Constants.TAG_ENTITY_NAME;
      case CORP_USER:
        return com.linkedin.metadata.Constants.CORP_USER_ENTITY_NAME;
      case CORP_GROUP:
        return com.linkedin.metadata.Constants.CORP_GROUP_ENTITY_NAME;
      case DOMAIN:
        return com.linkedin.metadata.Constants.DOMAIN_ENTITY_NAME;
      case GLOSSARY_TERM:
        return com.linkedin.metadata.Constants.GLOSSARY_TERM_ENTITY_NAME;
      case GLOSSARY_NODE:
        return com.linkedin.metadata.Constants.GLOSSARY_NODE_ENTITY_NAME;
      case CONTAINER:
        return com.linkedin.metadata.Constants.CONTAINER_ENTITY_NAME;
      case NOTEBOOK:
        return com.linkedin.metadata.Constants.NOTEBOOK_ENTITY_NAME;
      case ORGANIZATION:
        return com.linkedin.metadata.Constants.ORGANIZATION_ENTITY_NAME;
      default:
        log.warn("Unknown entity type: {}", entityType);
        return null;
    }
  }
}
