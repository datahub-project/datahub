package com.linkedin.datahub.graphql.resolvers.mutate.util;

import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;
import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;
import static com.linkedin.metadata.utils.CriterionUtils.buildIsNullCriterion;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.domain.DomainProperties;
import com.linkedin.domain.Domains;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

// TODO: Move to consuming from DomainService.
@Slf4j
public class DomainUtils {
  private static final String PARENT_DOMAIN_INDEX_FIELD_NAME = "parentDomain.keyword";
  private static final String HAS_PARENT_DOMAIN_INDEX_FIELD_NAME = "hasParentDomain";
  private static final String NAME_INDEX_FIELD_NAME = "name";

  private static final ConjunctivePrivilegeGroup ALL_PRIVILEGES_GROUP =
      new ConjunctivePrivilegeGroup(
          ImmutableList.of(PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()));

  private DomainUtils() {}

  /**
   * Checks the Platform Privilege MANAGE_DOMAINS to see if a user is authorized. If true, the user
   * has global control of Domains to create, edit, move, and delete them.
   */
  public static boolean canManageDomains(@Nonnull QueryContext context) {
    return AuthUtil.isAuthorized(
        context.getOperationContext(), PoliciesConfig.MANAGE_DOMAINS_PRIVILEGE);
  }

  /**
   * Returns true if the current user is able to create, delete, or move child Domains under a
   * parent Domain. They can do this with either the global MANAGE_DOMAINS privilege, or if they
   * have the MANAGE_DOMAIN_CHILDREN privilege on the relevant parent domain.
   */
  public static boolean canManageChildDomains(
      @Nonnull QueryContext context,
      @Nullable Urn parentDomainUrn,
      @Nonnull EntityClient entityClient) {
    if (canManageDomains(context)) {
      return true;
    }
    if (parentDomainUrn == null) {
      return false;
    }

    if (hasManagePrivilege(
        context, parentDomainUrn, PoliciesConfig.MANAGE_DOMAIN_CHILDREN_PRIVILEGE)) {
      return true;
    }

    Urn currentParentDomainUrn = parentDomainUrn;
    while (currentParentDomainUrn != null) {
      if (hasManagePrivilege(
          context, currentParentDomainUrn, PoliciesConfig.MANAGE_ALL_DOMAIN_CHILDREN_PRIVILEGE)) {
        return true;
      }
      currentParentDomainUrn = getDomainParentUrn(currentParentDomainUrn, context, entityClient);
    }

    return false;
  }

  /**
   * Returns whether this is a domain entity and whether you can edit this domain with the Manage
   * all children or Manage direct children privileges
   */
  public static boolean canUpdateDomainEntity(
      Urn targetUrn, QueryContext context, EntityClient entityClient) {
    final boolean isDomainEntity = targetUrn.getEntityType().equals(Constants.DOMAIN_ENTITY_NAME);
    if (!isDomainEntity) {
      return false;
    }
    final Urn parentDomainUrn = getDomainParentUrn(targetUrn, context, entityClient);
    return canManageChildDomains(context, parentDomainUrn, entityClient);
  }

  public static boolean hasManagePrivilege(
      @Nonnull QueryContext context,
      @Nullable Urn parentDomainUrn,
      PoliciesConfig.Privilege privilege) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(new ConjunctivePrivilegeGroup(ImmutableList.of(privilege.getType()))));

    return AuthorizationUtils.isAuthorized(
        context, parentDomainUrn.getEntityType(), parentDomainUrn.toString(), orPrivilegeGroups);
  }

  public static boolean hasViewPrivilege(
      @Nonnull QueryContext context, @Nullable Urn domainUrn, PoliciesConfig.Privilege privilege) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(new ConjunctivePrivilegeGroup(ImmutableList.of(privilege.getType()))));

    return AuthorizationUtils.isAuthorized(
        context, domainUrn.getEntityType(), domainUrn.toString(), orPrivilegeGroups);
  }

  /**
   * Returns true if the current user is able to view Domains under a parent Domain. They can do
   * this with either the global MANAGE_DOMAINS privilege, VIEW_ENTITY_PAGE privilege on the domain,
   * or if they have the VIEW_DOMAIN_CHILDREN or VIEW_ALL_DOMAIN_CHILDREN privilege on the relevant
   * parent domain.
   */
  public static boolean canViewChildDomains(
      @Nonnull QueryContext context, @Nullable Urn domainUrn, @Nonnull EntityClient entityClient) {
    if (canManageDomains(context)) {
      return true;
    }
    if (domainUrn == null) {
      return false;
    }

    if (hasViewPrivilege(context, domainUrn, PoliciesConfig.VIEW_ENTITY_PAGE_PRIVILEGE)) {
      return true;
    }

    Urn parentDomainUrn = getDomainParentUrn(domainUrn, context, entityClient);
    if (parentDomainUrn != null
        && hasViewPrivilege(
            context, parentDomainUrn, PoliciesConfig.VIEW_DOMAIN_CHILDREN_PRIVILEGE)) {
      return true;
    }

    Urn currentParentDomainUrn = parentDomainUrn;
    while (currentParentDomainUrn != null) {
      if (hasViewPrivilege(
          context, currentParentDomainUrn, PoliciesConfig.VIEW_ALL_DOMAIN_CHILDREN_PRIVILEGE)) {
        return true;
      }
      currentParentDomainUrn = getDomainParentUrn(currentParentDomainUrn, context, entityClient);
    }

    return false;
  }

  /** Returns the urn of the parent domain for a given Domain. Returns null if it doesn't exist. */
  @Nullable
  private static Urn getDomainParentUrn(
      @Nonnull Urn domainUrn, @Nonnull QueryContext context, @Nonnull EntityClient entityClient) {
    try {
      EntityResponse response =
          entityClient.getV2(
              context.getOperationContext(),
              Constants.DOMAIN_ENTITY_NAME,
              domainUrn,
              ImmutableSet.of(Constants.DOMAIN_PROPERTIES_ASPECT_NAME));
      if (response != null
          && response.getAspects().get(Constants.DOMAIN_PROPERTIES_ASPECT_NAME) != null) {
        DomainProperties domainProperties =
            new DomainProperties(
                response
                    .getAspects()
                    .get(Constants.DOMAIN_PROPERTIES_ASPECT_NAME)
                    .getValue()
                    .data());
        return getParentDomainSafely(domainProperties);
      }
      return null;
    } catch (URISyntaxException | RemoteInvocationException e) {
      throw new RuntimeException("Failed to fetch Domain to check for privileges", e);
    }
  }

  public static boolean isAuthorizedToUpdateDomainsForEntity(
      @Nonnull QueryContext context, Urn entityUrn, EntityClient entityClient) {

    if (GlossaryUtils.canUpdateGlossaryEntity(entityUrn, context, entityClient)) {
      return true;
    }

    // Check if it's a domain entity and handle with recursive permissions
    if (entityUrn.getEntityType().equals(Constants.DOMAIN_ENTITY_NAME)) {
      return canUpdateDomainEntity(entityUrn, context, entityClient);
    }

    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                ALL_PRIVILEGES_GROUP,
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.EDIT_ENTITY_DOMAINS_PRIVILEGE.getType()))));

    return AuthorizationUtils.isAuthorized(
        context, entityUrn.getEntityType(), entityUrn.toString(), orPrivilegeGroups);
  }

  public static void setDomainForResources(
      @Nonnull OperationContext opContext,
      @Nullable Urn domainUrn,
      List<ResourceRefInput> resources,
      Urn actor,
      EntityService<?> entityService)
      throws Exception {
    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceRefInput resource : resources) {
      changes.add(buildSetDomainProposal(opContext, domainUrn, resource, actor, entityService));
    }
    EntityUtils.ingestChangeProposals(opContext, changes, entityService, actor, false);
  }

  private static MetadataChangeProposal buildSetDomainProposal(
      @Nonnull OperationContext opContext,
      @Nullable Urn domainUrn,
      ResourceRefInput resource,
      Urn actor,
      EntityService<?> entityService) {
    Domains domains =
        (Domains)
            EntityUtils.getAspectFromEntity(
                opContext,
                resource.getResourceUrn(),
                Constants.DOMAINS_ASPECT_NAME,
                entityService,
                new Domains());
    final UrnArray newDomains = new UrnArray();
    if (domainUrn != null) {
      newDomains.add(domainUrn);
    }
    domains.setDomains(newDomains);
    return buildMetadataChangeProposalWithUrn(
        UrnUtils.getUrn(resource.getResourceUrn()), Constants.DOMAINS_ASPECT_NAME, domains);
  }

  public static void validateDomain(
      @Nonnull OperationContext opContext, Urn domainUrn, EntityService<?> entityService) {
    if (!entityService.exists(opContext, domainUrn, true)) {
      throw new IllegalArgumentException(
          String.format("Failed to validate Domain with urn %s. Urn does not exist.", domainUrn));
    }
  }

  private static List<Criterion> buildRootDomainCriteria() {
    final List<Criterion> criteria = new ArrayList<>();

    criteria.add(buildCriterion(HAS_PARENT_DOMAIN_INDEX_FIELD_NAME, Condition.EQUAL, "false"));

    criteria.add(buildIsNullCriterion(HAS_PARENT_DOMAIN_INDEX_FIELD_NAME));

    return criteria;
  }

  private static List<Criterion> buildParentDomainCriteria(@Nonnull final Urn parentDomainUrn) {
    final List<Criterion> criteria = new ArrayList<>();

    criteria.add(buildCriterion(HAS_PARENT_DOMAIN_INDEX_FIELD_NAME, Condition.EQUAL, "true"));

    criteria.add(
        buildCriterion(
            PARENT_DOMAIN_INDEX_FIELD_NAME, Condition.EQUAL, parentDomainUrn.toString()));

    return criteria;
  }

  private static Criterion buildNameCriterion(@Nonnull final String name) {
    return buildCriterion(NAME_INDEX_FIELD_NAME, Condition.EQUAL, name);
  }

  /**
   * Builds a filter that ORs together the root parent criterion / ANDs together the parent domain
   * criterion. The reason for the OR on root is elastic can have a null|false value to represent an
   * root domain in the index.
   *
   * @param name an optional name to AND in to each condition of the filter
   * @param parentDomainUrn the parent domain (null means root).
   * @return the Filter
   */
  public static Filter buildNameAndParentDomainFilter(
      @Nullable final String name, @Nullable final Urn parentDomainUrn) {
    if (parentDomainUrn == null) {
      return new Filter()
          .setOr(
              new ConjunctiveCriterionArray(
                  buildRootDomainCriteria().stream()
                      .map(
                          parentCriterion -> {
                            final CriterionArray array = new CriterionArray(parentCriterion);
                            if (name != null) {
                              array.add(buildNameCriterion(name));
                            }
                            return new ConjunctiveCriterion().setAnd(array);
                          })
                      .collect(Collectors.toList())));
    }

    final CriterionArray andArray = new CriterionArray(buildParentDomainCriteria(parentDomainUrn));
    if (name != null) {
      andArray.add(buildNameCriterion(name));
    }
    return new Filter()
        .setOr(new ConjunctiveCriterionArray(new ConjunctiveCriterion().setAnd(andArray)));
  }

  public static Filter buildParentDomainFilter(@Nullable final Urn parentDomainUrn) {
    return buildNameAndParentDomainFilter(null, parentDomainUrn);
  }

  /**
   * Check if a domain has any child domains
   *
   * @param domainUrn the URN of the domain to check
   * @param context query context (includes authorization context to authorize the request)
   * @param entityClient client used to perform the check
   * @return true if the domain has any child domains, false if it does not
   */
  public static boolean hasChildDomains(
      @Nonnull final Urn domainUrn,
      @Nonnull final QueryContext context,
      @Nonnull final EntityClient entityClient)
      throws RemoteInvocationException {
    Filter parentDomainFilter = buildParentDomainFilter(domainUrn);
    // Search for entities matching parent domain
    // Limit count to 1 for existence check
    final SearchResult searchResult =
        entityClient.filter(
            context.getOperationContext(), DOMAIN_ENTITY_NAME, parentDomainFilter, null, 0, 1);
    return (searchResult.getNumEntities() > 0);
  }

  private static Map<Urn, EntityResponse> getDomainsByNameAndParent(
      @Nonnull final String name,
      @Nullable final Urn parentDomainUrn,
      @Nonnull final QueryContext context,
      @Nonnull final EntityClient entityClient) {
    try {
      final Filter filter = buildNameAndParentDomainFilter(name, parentDomainUrn);

      final SearchResult searchResult =
          entityClient.filter(
              context.getOperationContext(), DOMAIN_ENTITY_NAME, filter, null, 0, 1000);

      final Set<Urn> domainUrns =
          searchResult.getEntities().stream()
              .map(SearchEntity::getEntity)
              .collect(Collectors.toSet());

      return entityClient.batchGetV2(
          context.getOperationContext(),
          DOMAIN_ENTITY_NAME,
          domainUrns,
          Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME));
    } catch (Exception e) {
      throw new RuntimeException("Failed fetching Domains by name and parent", e);
    }
  }

  public static boolean hasNameConflict(
      @Nonnull final String name,
      @Nullable final Urn parentDomainUrn,
      @Nonnull final QueryContext context,
      @Nonnull final EntityClient entityClient) {
    final Map<Urn, EntityResponse> entities =
        getDomainsByNameAndParent(name, parentDomainUrn, context, entityClient);

    // Even though we searched by name, do one more pass to check the name is unique
    return entities.values().stream()
        .anyMatch(
            entityResponse -> {
              if (entityResponse.getAspects().containsKey(DOMAIN_PROPERTIES_ASPECT_NAME)) {
                DataMap dataMap =
                    entityResponse
                        .getAspects()
                        .get(DOMAIN_PROPERTIES_ASPECT_NAME)
                        .getValue()
                        .data();
                DomainProperties domainProperties = new DomainProperties(dataMap);
                return (domainProperties.hasName() && domainProperties.getName().equals(name));
              }
              return false;
            });
  }

  @Nullable
  public static Entity getParentDomain(
      @Nonnull final Urn urn,
      @Nonnull final QueryContext context,
      @Nonnull final EntityClient entityClient) {
    try {
      final EntityResponse entityResponse =
          entityClient.getV2(
              context.getOperationContext(),
              DOMAIN_ENTITY_NAME,
              urn,
              Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME));

      if (entityResponse != null
          && entityResponse.getAspects().containsKey(DOMAIN_PROPERTIES_ASPECT_NAME)) {
        final DomainProperties properties =
            new DomainProperties(
                entityResponse.getAspects().get(DOMAIN_PROPERTIES_ASPECT_NAME).getValue().data());
        final Urn parentDomainUrn = getParentDomainSafely(properties);
        return parentDomainUrn != null ? UrnToEntityMapper.map(context, parentDomainUrn) : null;
      }
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to retrieve parent domain for entity %s", urn), e);
    }

    return null;
  }

  /**
   * Get a parent domain only if hasParentDomain was set. There is strange elastic behavior where
   * moving a domain to the root leaves the parentDomain field set but makes hasParentDomain false.
   * This helper makes sure that queries to elastic where hasParentDomain=false and
   * parentDomain=value only gives us the parentDomain if hasParentDomain=true.
   *
   * @param properties the domain properties aspect
   * @return the parentDomain or null
   */
  @Nullable
  public static Urn getParentDomainSafely(@Nonnull final DomainProperties properties) {
    return properties.hasParentDomain() ? properties.getParentDomain() : null;
  }
}
