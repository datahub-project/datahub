package com.linkedin.metadata.search.elasticsearch.query.filter;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.identity.UserOrganizations;
import com.linkedin.metadata.aspect.AspectRetriever;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;

/**
 * Rewrites search queries to filter by organization membership. Enforces multi-tenant isolation by
 * ensuring users only see entities from their assigned organization(s).
 *
 * <p>Design: - System actors (datahub, __datahub_system) bypass filtering - Users without
 * organizations see no entities (full isolation) - Users with organizations see entities from ANY
 * of their organizations - Entities can belong to multiple organizations
 */
@Slf4j
@RequiredArgsConstructor
public class OrganizationFilterRewriter implements QueryFilterRewriter {

  private final AspectRetriever aspectRetriever;

  @Override
  public Set<QueryFilterRewriterSearchType> getRewriterSearchTypes() {
    return Set.of(QueryFilterRewriterSearchType.values());
  }

  @Override
  public Set<String> getRewriterFieldNames() {
    return Set.of(ORGANIZATIONS_ASPECT_NAME);
  }

  @Override
  public <T extends QueryBuilder> T rewrite(
      @Nonnull OperationContext opContext,
      @Nonnull QueryFilterRewriterContext rewriterContext,
      @Nullable T filterQuery) {

    // Get authenticated user
    Urn userUrn =
        com.linkedin.common.urn.UrnUtils.getUrn(
            opContext.getSessionAuthentication().getActor().toUrnStr());

    // Skip system actors - they need full access for administration
    if (SYSTEM_ACTOR.equals(userUrn.toString()) || DATAHUB_ACTOR.equals(userUrn.toString())) {
      log.debug("System actor {} - bypassing organization filter", userUrn);
      return filterQuery;
    }

    // Fetch user's organizations
    Set<Urn> userOrganizations = getUserOrganizations(userUrn);

    // If user has no organizations, deny all access (full isolation)
    if (userOrganizations.isEmpty()) {
      log.warn("User {} has no organizations assigned - denying access to all entities", userUrn);
      // Return a filter that matches nothing
      return (T)
          QueryBuilders.boolQuery()
              .must(QueryBuilders.matchQuery("urn", "___ISOLATION_NO_ACCESS___"));
    }

    log.debug(
        "User {} has {} organization(s) - applying filter", userUrn, userOrganizations.size());

    // Apply organization filter to the query
    return (T) applyOrganizationFilter(filterQuery, userOrganizations);
  }

  /**
   * Fetch organizations for a given user from the UserOrganizations aspect.
   *
   * @param userUrn URN of the user
   * @return Set of organization URNs the user belongs to (empty if none or error)
   */
  private Set<Urn> getUserOrganizations(Urn userUrn) {
    try {
      com.linkedin.entity.Aspect aspect =
          aspectRetriever.getLatestAspectObject(userUrn, USER_ORGANIZATIONS_ASPECT_NAME);

      if (aspect != null) {
        UserOrganizations userOrgsAspect = new UserOrganizations(aspect.data());
        if (userOrgsAspect.getOrganizations() != null) {
          Set<Urn> orgs = userOrgsAspect.getOrganizations().stream().collect(Collectors.toSet());
          log.debug("Found {} organizations for user {}", orgs.size(), userUrn);
          return orgs;
        }
      }

      log.debug("No UserOrganizations aspect found for user {}", userUrn);
    } catch (Exception e) {
      log.error("Failed to fetch organizations for user: {}", userUrn, e);
    }
    return Set.of();
  }

  /**
   * Apply organization filter to an existing query.
   *
   * <p>The filter allows entities that belong to ANY of the user's organizations.
   *
   * @param existingFilter Existing filter (may be null)
   * @param allowedOrganizations Set of organization URNs the user can see
   * @return Combined filter with organization restriction
   */
  private QueryBuilder applyOrganizationFilter(
      QueryBuilder existingFilter, Set<Urn> allowedOrganizations) {

    // Build OR clause: entity must belong to at least one user organization
    BoolQueryBuilder orgFilter = QueryBuilders.boolQuery();
    for (Urn orgUrn : allowedOrganizations) {
      // Match against the indexed organizations field
      orgFilter.should(
          QueryBuilders.termQuery(ORGANIZATIONS_ASPECT_NAME + ".keyword", orgUrn.toString()));
    }
    orgFilter.minimumShouldMatch(1);

    // Combine with existing filter using AND
    if (existingFilter == null) {
      return orgFilter;
    }

    return QueryBuilders.boolQuery().must(existingFilter).must(orgFilter);
  }
}
