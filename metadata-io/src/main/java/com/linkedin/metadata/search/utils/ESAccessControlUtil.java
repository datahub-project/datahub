package com.linkedin.metadata.search.utils;

import static com.datahub.authorization.AuthUtil.VIEW_RESTRICTED_ENTITY_TYPES;
import static com.linkedin.metadata.authorization.ApiOperation.READ;
import static com.linkedin.metadata.search.utils.ESUtils.*;
import static com.linkedin.metadata.utils.SearchUtil.ES_INDEX_FIELD;

import com.datahub.authorization.AuthUtil;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.hooks.OwnershipOwnerTypes;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.timeseries.elastic.indexbuilder.MappingsBuilder;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.policy.PolicyMatchCriterion;
import com.linkedin.policy.PolicyMatchCriterionArray;
import io.datahubproject.metadata.context.ActorContext;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.services.RestrictedService;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;

@Slf4j
public class ESAccessControlUtil {
  private ESAccessControlUtil() {}

  /**
   * Given an OperationContext and SearchResult, mark the restricted entities. Currently, the entire
   * entity is marked as restricted using the key aspect name.
   *
   * @param searchResult restricted search result
   */
  public static void restrictSearchResult(
      @Nonnull OperationContext opContext, @Nonnull SearchResult searchResult) {
    restrictSearchResult(opContext, searchResult.getEntities());
  }

  public static Collection<SearchEntity> restrictSearchResult(
      @Nonnull OperationContext opContext, Collection<SearchEntity> searchEntities) {
    if (opContext.getOperationContextConfig().getViewAuthorizationConfiguration().isEnabled()
        && !opContext.isSystemAuth()) {
      final EntityRegistry entityRegistry = Objects.requireNonNull(opContext.getEntityRegistry());
      final RestrictedService restrictedService =
          Objects.requireNonNull(opContext.getServicesRegistryContext()).getRestrictedService();

      if (opContext.getSearchContext().isRestrictedSearch()) {
        for (SearchEntity searchEntity : searchEntities) {
          final String entityType = searchEntity.getEntity().getEntityType();
          final com.linkedin.metadata.models.EntitySpec entitySpec =
              entityRegistry.getEntitySpec(entityType);

          if (VIEW_RESTRICTED_ENTITY_TYPES.contains(entityType)
              && !AuthUtil.canViewEntity(opContext, searchEntity.getEntity())) {

            // Not authorized && restricted response requested
            if (opContext.getSearchContext().isRestrictedSearch()) {
              // Restrict entity
              searchEntity.setRestrictedAspects(
                  new StringArray(List.of(entitySpec.getKeyAspectName())));

              searchEntity.setEntity(
                  restrictedService.encryptRestrictedUrn(searchEntity.getEntity()));
            }
          }
        }
      }
    }
    return searchEntities;
  }

  public static boolean restrictUrn(@Nonnull OperationContext opContext, @Nonnull Urn urn) {
    if (opContext.getOperationContextConfig().getViewAuthorizationConfiguration().isEnabled()
        && !opContext.isSystemAuth()) {
      return !AuthUtil.canViewEntity(opContext, urn);
    }
    return false;
  }

  /** Acryl Only */
  private static final String OWNER_TYPES_FIELD = "ownerTypes";

  private static final QueryBuilder MATCH_ALL = QueryBuilders.matchAllQuery();

  /**
   * Given the OperationContext produce a filter for search results
   *
   * @param opContext the OperationContext of the search
   * @return
   */
  public static Optional<QueryBuilder> buildAccessControlFilters(
      @Nonnull final OperationContext opContext, @Nonnull final List<String> entityNames) {
    Optional<QueryBuilder> response = Optional.empty();

    /*
     If search authorization is enabled AND we're also not the system performing the query
    */
    if (shouldApplyAccessControlFilters(opContext, entityNames)
        && !opContext.isSystemAuth()
        && !opContext.getSearchContext().isRestrictedSearch()) {

      BoolQueryBuilder builder = QueryBuilders.boolQuery();

      // Apply access policies
      streamViewQueries(opContext).distinct().forEach(builder::should);

      if (builder.should().isEmpty()) {
        // default no filters
        return Optional.of(builder.mustNot(MATCH_ALL));
      } else if (!builder.should().contains(MATCH_ALL)) {
        // if MATCH_ALL is not present, apply filters requiring at least 1
        builder.minimumShouldMatch(1);
        response = Optional.of(builder);
      }
    }

    // MATCH_ALL filter present or system user or disabled
    return response;
  }

  private static boolean shouldApplyAccessControlFilters(
      @Nonnull final OperationContext opContext, @Nonnull final List<String> entityNames) {
    final boolean isViewAuthorizationEnabled =
        opContext.getOperationContextConfig().getViewAuthorizationConfiguration().isEnabled();

    if (isViewAuthorizationEnabled) {
      return true;
    }

    // When ViewAuthorization is disabled we should forcibly apply access control filters only for
    // IngestionSource entity type
    if (entityNames.isEmpty()) {
      return false;
    }
    boolean isEntityNamesIncludeIngestionSourceEntityName =
        entityNames.contains(Constants.INGESTION_SOURCE_ENTITY_NAME);
    if (!isEntityNamesIncludeIngestionSourceEntityName) {
      return false;
    }
    // When we have not only ingestion source entity type we have to restrict it
    if (entityNames.size() > 1) {
      throw new RuntimeException(
          String.format(
              "Can't apply access control filters by ingestion source entity type with another entity types: %s",
              entityNames));
    }

    return true;
  }

  private static final Function<DataHubPolicyInfo, Boolean> activeMetadataViewEntityPolicyFilter =
      policy ->
          policy.getPrivileges() != null
              && PoliciesConfig.ACTIVE_POLICY_STATE.equals(policy.getState())
              && PoliciesConfig.METADATA_POLICY_TYPE.equals(policy.getType())
              && AuthUtil.lookupEntityAPIPrivilege(READ, VIEW_RESTRICTED_ENTITY_TYPES)
                  .values()
                  .stream()
                  .flatMap(disjunct -> disjunct.stream().flatMap(Collection::stream))
                  .anyMatch(priv -> policy.getPrivileges().contains(priv.getType()));

  private static Stream<QueryBuilder> streamViewQueries(@Nonnull OperationContext opContext) {
    return opContext.getSessionActorContext().getPolicyInfoSet().stream()
        .filter(activeMetadataViewEntityPolicyFilter::apply)
        .map(
            policy -> {
              // Build actor query
              QueryBuilder actorQuery = buildActorResourceQuery(opContext, policy);

              if (!policy.hasResources()
                  || !policy.getResources().hasFilter()
                  || !policy.getResources().getFilter().hasCriteria()
                  || policy.getResources().getFilter().getCriteria().isEmpty()) {
                // no resource restrictions
                return actorQuery;
              } else {

                PolicyMatchCriterionArray criteriaArray =
                    policy.getResources().getFilter().getCriteria();
                // Cannot apply policy if we can't map every field
                if (!criteriaArray.stream()
                    .allMatch(
                        criteria ->
                            toESField(criteria, opContext.getAspectRetriever()).isPresent())) {
                  log.error("Returning deny all due to unknown policy filter field.");
                  return null;
                }

                BoolQueryBuilder resourceQuery = QueryBuilders.boolQuery();
                // apply actor filter if present
                if (!MATCH_ALL.equals(actorQuery)) {
                  resourceQuery.filter(actorQuery);
                }
                // add resource query
                buildResourceQuery(opContext, criteriaArray).forEach(resourceQuery::filter);
                return resourceQuery;
              }
            })
        .filter(Objects::nonNull);
  }

  /**
   * Build an entity index query for ownership policies. If no restrictions, returns MATCH_ALL query
   *
   * @param opContext context
   * @param policy policy
   * @return filter query
   */
  private static QueryBuilder buildActorResourceQuery(
      OperationContext opContext, DataHubPolicyInfo policy) {

    if (policy.hasActors()) {
      DataHubActorFilter actorFilter = policy.getActors();

      if (actorFilter.isAllUsers()) {
        // no actor restriction
        return MATCH_ALL;
      }

      ActorContext actorContext = opContext.getSessionActorContext();
      Urn actorUrn = UrnUtils.getUrn(actorContext.getAuthentication().getActor().toUrnStr());

      // policy applies because of explicit user
      if (actorFilter.getUsers() != null && policy.getActors().getUsers().contains(actorUrn)) {
        return MATCH_ALL;
      }

      // policy applied if part of ANY group
      if (actorFilter.isAllGroups() && !actorContext.getGroupMembership().isEmpty()) {
        return MATCH_ALL;
      }

      // policy applied because of explicit group
      if (actorFilter.getGroups() != null
          && actorContext.getGroupMembership().stream()
              .anyMatch(myGroup -> actorFilter.getGroups().contains(myGroup))) {
        return MATCH_ALL;
      }

      if (actorFilter.getRoles() != null && !actorFilter.getRoles().isEmpty()) {
        return MATCH_ALL;
      }

      if (actorFilter.isResourceOwners()) {
        // policy might apply to the actor via user or group
        List<String> actorAndGroupUrns =
            Stream.concat(
                    Stream.of(actorContext.getAuthentication().getActor().toUrnStr()),
                    actorContext.getGroupMembership().stream().map(Urn::toString))
                .distinct()
                .collect(Collectors.toList());

        if (!hasResourceOwnersType(actorFilter)) {
          // owners without owner type restrictions
          return QueryBuilders.termsQuery(
              ESUtils.toKeywordField(
                  MappingsBuilder.OWNERS_FIELD, false, opContext.getAspectRetriever()),
              actorAndGroupUrns);
        } else {
          // owners with type restrictions
          BoolQueryBuilder orQuery = QueryBuilders.boolQuery();
          orQuery.minimumShouldMatch(1);

          Set<String> typeFields =
              actorFilter.getResourceOwnersTypes().stream()
                  .map(
                      typeUrn ->
                          String.format(
                              "%s.%s%s",
                              OWNER_TYPES_FIELD,
                              OwnershipOwnerTypes.encodeFieldName(typeUrn.toString()),
                              KEYWORD_SUFFIX))
                  .collect(Collectors.toSet());

          typeFields.forEach(
              field -> orQuery.should(QueryBuilders.termsQuery(field, actorAndGroupUrns)));

          return orQuery;
        }
      }
    }

    // deny
    return QueryBuilders.boolQuery().mustNot(MATCH_ALL);
  }

  private static boolean hasResourceOwnersType(DataHubActorFilter actorFilter) {
    return actorFilter.hasResourceOwnersTypes()
        || (actorFilter.getResourceOwnersTypes() != null
            && !actorFilter.getResourceOwnersTypes().isEmpty());
  }

  private static Stream<AbstractQueryBuilder<?>> buildResourceQuery(
      @Nonnull OperationContext opContext, @Nonnull PolicyMatchCriterionArray criteriaArray) {
    return criteriaArray.stream()
        .map(
            criteria -> {
              final String fieldName = toESField(criteria, opContext.getAspectRetriever()).get();
              final Collection<String> values = toESValues(opContext, criteria);

              switch (criteria.getCondition()) {
                case EQUALS:
                  return QueryBuilders.termsQuery(fieldName, values);
                case STARTS_WITH:
                  BoolQueryBuilder startWithBoolQuery =
                      QueryBuilders.boolQuery().minimumShouldMatch(1);
                  switch (criteria.getField()) {
                    case "URN":
                    case "TAG":
                    case "DOMAIN":
                    case "CONTAINER":
                    case "DATA_PLATFORM_INSTANCE":
                      values.forEach(
                          value ->
                              startWithBoolQuery.should(
                                  QueryBuilders.prefixQuery(fieldName, value)));
                      return startWithBoolQuery;
                    default:
                      throw new UnsupportedOperationException(
                          "Unsupported field for search access controls:" + criteria);
                  }
                default:
                  throw new UnsupportedOperationException(
                      "Unsupported search access control condition: " + criteria);
              }
            });
  }

  // Supported fields link to EntityFieldType
  private static Optional<String> toESField(
      PolicyMatchCriterion criterion, @Nullable AspectRetriever aspectRetriever) {
    switch (criterion.getField().toUpperCase(Locale.ROOT)) {
      case "TYPE":
      case "RESOURCE_TYPE":
        return Optional.of(ES_INDEX_FIELD);
      case "URN":
        return Optional.of(
            ESUtils.toKeywordField(MappingsBuilder.URN_FIELD, false, aspectRetriever));
      case "TAG":
        return Optional.of(
            ESUtils.toKeywordField(MappingsBuilder.TAGS_FIELD, false, aspectRetriever));
      case "DOMAIN":
        return Optional.of(
            ESUtils.toKeywordField(MappingsBuilder.DOMAINS_FIELD, false, aspectRetriever));
      case "DATA_PLATFORM_INSTANCE":
        return Optional.of(
            ESUtils.toKeywordField(
                MappingsBuilder.DATA_PLATFORM_INSTANCE_FIELD, false, aspectRetriever));
      case "CONTAINER":
        return Optional.of(
            ESUtils.toKeywordField(MappingsBuilder.CONTAINER_FIELD, false, aspectRetriever));
      default:
        return Optional.empty();
    }
  }

  private static Collection<String> toESValues(
      @Nonnull OperationContext opContext, @Nonnull PolicyMatchCriterion criterion) {
    switch (criterion.getField().toUpperCase(Locale.ROOT)) {
      case "TYPE":
      case "RESOURCE_TYPE":
        return criterion.getValues().stream()
            .map(
                value ->
                    opContext.getSearchContext().getIndexConvention().getEntityIndexName(value))
            .collect(Collectors.toSet());
      case "DOMAIN":
        final Set<Urn> visitedDomains = new HashSet<>();

        getChildDomains(
            opContext,
            criterion.getValues().stream().map(UrnUtils::getUrn).collect(Collectors.toSet()),
            visitedDomains);

        return visitedDomains.stream().map(Urn::toString).sorted().collect(Collectors.toList());
        // TODO: Support child containers?
      default:
        return criterion.getValues();
    }
  }

  private static Collection<Urn> getChildDomains(
      @Nonnull OperationContext opContext,
      Set<Urn> targetDomainUrns,
      @Nonnull Set<Urn> visitedDomainUrns) {

    RelatedEntitiesScrollResult result = null;
    Set<Urn> next = new HashSet<>();

    while (result == null || result.getScrollId() != null) {

      // filter for destination for any of the target domains
      Filter destinationFilter =
          new Filter()
              .setOr(
                  new ConjunctiveCriterionArray(
                      targetDomainUrns.stream()
                          .map(Urn::toString)
                          .map(
                              urnStr ->
                                  new ConjunctiveCriterion()
                                      .setAnd(
                                          new CriterionArray(
                                              List.of(
                                                  CriterionUtils.buildCriterion(
                                                      "urn", Condition.EQUAL, urnStr)))))
                          .collect(Collectors.toList())));

      result =
          opContext
              .getRetrieverContext()
              .getGraphRetriever()
              .scrollRelatedEntities(
                  Set.of(Constants.DOMAIN_ENTITY_NAME),
                  null,
                  Set.of(Constants.DOMAIN_ENTITY_NAME),
                  destinationFilter,
                  Set.of(Constants.IS_PART_OF_RELATIONSHIP_NAME),
                  new RelationshipFilter().setDirection(RelationshipDirection.OUTGOING),
                  Edge.EDGE_SORT_CRITERION,
                  result == null ? null : result.getScrollId(),
                  GraphRetriever.DEFAULT_EDGE_FETCH_LIMIT,
                  null,
                  null);

      next.addAll(
          result.getEntities().stream()
              .map(r -> UrnUtils.getUrn(r.getSourceUrn()))
              .filter(urn -> !visitedDomainUrns.contains(urn))
              .collect(Collectors.toSet()));
    }

    visitedDomainUrns.addAll(targetDomainUrns);

    if (next.isEmpty()) {
      return visitedDomainUrns;
    } else {
      return getChildDomains(opContext, next, visitedDomainUrns);
    }
  }
}
