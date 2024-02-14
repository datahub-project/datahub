package com.linkedin.metadata.search.utils;

import static com.linkedin.metadata.utils.SearchUtil.ES_INDEX_FIELD;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.datahub.authorization.EntitySpec;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.aspect.hooks.OwnerTypeMap;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.timeseries.elastic.indexbuilder.MappingsBuilder;
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.policy.PolicyMatchCriterion;
import com.linkedin.policy.PolicyMatchCriterionArray;
import io.datahubproject.metadata.context.ActorContext;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermsQueryBuilder;

@Slf4j
public class ESAccessControlUtil {
  private ESAccessControlUtil() {}

  private static final String OWNER_TYPES_FIELD = "ownerTypes";
  private static final QueryBuilder MATCH_ALL = QueryBuilders.matchAllQuery();
  /*
    These two privileges should be logically the same for search even
    if one might allow search but not the entity page view at some point.
  */
  private static final Set<String> FILTER_PRIVILEGES =
      Set.of(
          PoliciesConfig.VIEW_ENTITY_PRIVILEGE.getType(),
          PoliciesConfig.VIEW_ENTITY_PAGE_PRIVILEGE.getType());

  /**
   * Given the OperationContext produce a filter for search results
   *
   * @param opContext the OperationContext of the search
   * @return
   */
  public static Optional<QueryBuilder> buildAccessControlFilters(
      @Nonnull OperationContext opContext) {
    Optional<QueryBuilder> response = Optional.empty();

    /*
     If search authorization is enabled AND we're also not the system performing the query
    */
    if (opContext.getOperationContextConfig().getSearchAuthorizationConfiguration().isEnabled()
        && !opContext.getSessionActorContext().isSystemAuthentication()
        && !opContext.getSearchContext().isRestrictedSearch()) {
      BoolQueryBuilder builder = QueryBuilders.boolQuery();
      builder.minimumShouldMatch(1);

      // Apply access policies
      streamViewQueries(opContext).distinct().forEach(builder::should);

      if (builder.should().isEmpty()) {
        // default deny if no filters
        return Optional.of(builder.mustNot(MATCH_ALL));
      } else if (!builder.should().contains(MATCH_ALL)) {
        // if MATCH_ALL is not present, apply filters
        response = Optional.of(builder);
      }
    }

    // MATCH_ALL filter present or system user or disabled
    return response;
  }

  /**
   * Given an OperationContext and SearchResult, mark the restricted entities. Currently, the entire
   * entity is marked as restricted using the key aspect name
   *
   * @param searchResult restricted search result
   */
  public static void restrictSearchResult(
      @Nonnull OperationContext opContext, @Nonnull SearchResult searchResult) {
    restrictSearchResult(opContext, searchResult.getEntities());
  }

  public static <T extends Collection<SearchEntity>> T restrictSearchResult(
      @Nonnull OperationContext opContext, T searchEntities) {
    if (opContext.getOperationContextConfig().getSearchAuthorizationConfiguration().isEnabled()
        && opContext.getSearchContext().isRestrictedSearch()) {
      final EntityRegistry entityRegistry =
          opContext.getEntityRegistryContext().getEntityRegistry();
      final String actorUrnStr =
          opContext.getSessionActorContext().getAuthentication().getActor().toUrnStr();
      final DisjunctivePrivilegeGroup orGroup =
          new DisjunctivePrivilegeGroup(
              ImmutableList.of(new ConjunctivePrivilegeGroup(FILTER_PRIVILEGES)));

      for (SearchEntity searchEntity : searchEntities) {
        final String entityType = searchEntity.getEntity().getEntityType();
        final Optional<EntitySpec> resourceSpec =
            Optional.of(new EntitySpec(entityType, searchEntity.getEntity().toString()));
        if (!AuthUtil.isAuthorized(
            opContext.getAuthorizerContext().getAuthorizer(), actorUrnStr, resourceSpec, orGroup)) {
          final String keyAspectName =
              entityRegistry.getEntitySpecs().get(entityType).getKeyAspectName();
          searchEntity.setRestrictedAspects(new StringArray(List.of(keyAspectName)));
        }
      }
    }
    return searchEntities;
  }

  private static final Function<DataHubPolicyInfo, Boolean> activeMetadataViewEntityPolicyFilter =
      policy ->
          policy.getPrivileges() != null
              && PoliciesConfig.ACTIVE_POLICY_STATE.equals(policy.getState())
              && PoliciesConfig.METADATA_POLICY_TYPE.equals(policy.getType())
              && FILTER_PRIVILEGES.stream().anyMatch(priv -> policy.getPrivileges().contains(priv));

  private static Stream<QueryBuilder> streamViewQueries(OperationContext opContext) {
    return opContext.getSessionActorContext().getPolicyInfoSet().stream()
        .filter(activeMetadataViewEntityPolicyFilter::apply)
        .map(
            policy -> {
              // Build actor query
              QueryBuilder actorQuery = buildActorQuery(opContext, policy);

              if (!policy.hasResources()) {
                // no resource restrictions
                return actorQuery;
              } else {

                // No filters or criteria, deny access
                if (!policy.getResources().hasFilter()
                    || !policy.getResources().getFilter().hasCriteria()) {
                  return null;
                }

                PolicyMatchCriterionArray criteriaArray =
                    policy.getResources().getFilter().getCriteria();
                // Cannot apply policy if we can't map every field, deny access
                if (!criteriaArray.stream().allMatch(criteria -> toESField(criteria).isPresent())) {
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
  private static QueryBuilder buildActorQuery(
      OperationContext opContext, DataHubPolicyInfo policy) {
    DataHubActorFilter actorFilter = policy.getActors();

    if (!policy.hasActors() || !policy.getActors().isResourceOwners()) {
      // no owner restriction
      return MATCH_ALL;
    }

    ActorContext actorContext = opContext.getSessionActorContext();

    // policy might apply to the actor via user or group
    Set<String> actorAndGroupUrns =
        Stream.concat(
                Stream.of(actorContext.getAuthentication().getActor().toUrnStr()),
                actorContext.getGroupMembership().stream()
                    .filter(
                        groupUrn ->
                            actorFilter.getGroups() != null
                                && actorFilter.getGroups().contains(groupUrn))
                    .map(Urn::toString))
            .collect(Collectors.toSet());

    if (!actorFilter.hasResourceOwnersTypes()) {
      // owners without owner type restrictions
      return QueryBuilders.termsQuery(
          ESUtils.toKeywordField(MappingsBuilder.OWNERS_FIELD, false), actorAndGroupUrns);
    } else {
      // owners with type restrictions
      BoolQueryBuilder orQuery = QueryBuilders.boolQuery();
      orQuery.minimumShouldMatch(1);

      Set<String> fields =
          actorAndGroupUrns.stream()
              .map(
                  urnStr ->
                      String.format(
                          "%s.%s", OWNER_TYPES_FIELD, OwnerTypeMap.encodeFieldName(urnStr)))
              .collect(Collectors.toSet());

      fields.forEach(
          field ->
              orQuery.should(
                  QueryBuilders.termsQuery(
                      field,
                      actorFilter.getResourceOwnersTypes().stream()
                          .map(Urn::toString)
                          .collect(Collectors.toSet()))));

      return orQuery;
    }
  }

  private static Stream<TermsQueryBuilder> buildResourceQuery(
      OperationContext opContext, PolicyMatchCriterionArray criteriaArray) {
    return criteriaArray.stream()
        .map(
            criteria ->
                QueryBuilders.termsQuery(
                    toESField(criteria).get(), toESValues(opContext, criteria)));
  }

  private static Optional<String> toESField(PolicyMatchCriterion criterion) {
    switch (criterion.getField()) {
      case "TYPE":
        return Optional.of(ES_INDEX_FIELD);
      case "URN":
        return Optional.of(ESUtils.toKeywordField(MappingsBuilder.URN_FIELD, false));
      case "TAG":
        return Optional.of(ESUtils.toKeywordField(MappingsBuilder.TAGS_FIELD, false));
      case "DOMAIN":
        return Optional.of(ESUtils.toKeywordField(MappingsBuilder.DOMAINS_FIELD, false));
      default:
        return Optional.empty();
    }
  }

  private static Collection<String> toESValues(
      OperationContext opContext, PolicyMatchCriterion criterion) {
    switch (criterion.getField()) {
      case "TYPE":
        return criterion.getValues().stream()
            .map(
                value ->
                    opContext.getSearchContext().getIndexConvention().getEntityIndexName(value))
            .collect(Collectors.toSet());
      default:
        return criterion.getValues();
    }
  }
}
