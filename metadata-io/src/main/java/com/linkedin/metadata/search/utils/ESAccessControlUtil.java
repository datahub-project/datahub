package com.linkedin.metadata.search.utils;

import static com.linkedin.metadata.authorization.ApiGroup.ENTITY;
import static com.linkedin.metadata.authorization.ApiOperation.READ;
import static com.linkedin.metadata.utils.SearchUtil.ES_INDEX_FIELD;
import static com.linkedin.metadata.utils.SearchUtil.KEYWORD_SUFFIX;

import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthUtil;
import com.datahub.plugins.auth.authorization.Authorizer;
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
      final EntityRegistry entityRegistry = opContext.getEntityRegistry();
      final Authentication auth = opContext.getSessionActorContext().getAuthentication();
      final Authorizer authorizer = opContext.getAuthorizerContext().getAuthorizer();

      for (SearchEntity searchEntity : searchEntities) {
        final String entityType = searchEntity.getEntity().getEntityType();
        final com.linkedin.metadata.models.EntitySpec entitySpec =
            entityRegistry.getEntitySpec(entityType);

        if (!AuthUtil.canViewEntity(auth, authorizer, searchEntity.getEntity())) {
          searchEntity.setRestrictedAspects(
              new StringArray(List.of(entitySpec.getKeyAspectName())));
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
              && PoliciesConfig.lookupAPIPrivilege(ENTITY, READ).stream()
                  .flatMap(Collection::stream)
                  .anyMatch(priv -> policy.getPrivileges().contains(priv.getType()));

  private static Stream<QueryBuilder> streamViewQueries(OperationContext opContext) {
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
                if (!criteriaArray.stream().allMatch(criteria -> toESField(criteria).isPresent())) {
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

    DataHubActorFilter actorFilter = policy.getActors();

    if (!policy.hasActors() || !actorFilter.isResourceOwners()) {
      // no owner restriction
      return MATCH_ALL;
    }

    ActorContext actorContext = opContext.getSessionActorContext();
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
          ESUtils.toKeywordField(MappingsBuilder.OWNERS_FIELD, false), actorAndGroupUrns);
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
                          OwnerTypeMap.encodeFieldName(typeUrn.toString()),
                          KEYWORD_SUFFIX))
              .collect(Collectors.toSet());

      typeFields.forEach(
          field -> orQuery.should(QueryBuilders.termsQuery(field, actorAndGroupUrns)));

      return orQuery;
    }
  }

  private static boolean hasResourceOwnersType(DataHubActorFilter actorFilter) {
    return actorFilter.hasResourceOwnersTypes()
        || (actorFilter.getResourceOwnersTypes() != null
            && !actorFilter.getResourceOwnersTypes().isEmpty());
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
