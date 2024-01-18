package com.linkedin.datahub.graphql.resolvers.mutate.util;

import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.google.common.collect.ImmutableList;
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

  public static boolean isAuthorizedToUpdateDomainsForEntity(
      @Nonnull QueryContext context, Urn entityUrn) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                ALL_PRIVILEGES_GROUP,
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.EDIT_ENTITY_DOMAINS_PRIVILEGE.getType()))));

    return AuthorizationUtils.isAuthorized(
        context.getAuthorizer(),
        context.getActorUrn(),
        entityUrn.getEntityType(),
        entityUrn.toString(),
        orPrivilegeGroups);
  }

  public static void setDomainForResources(
      @Nullable Urn domainUrn,
      List<ResourceRefInput> resources,
      Urn actor,
      EntityService entityService)
      throws Exception {
    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceRefInput resource : resources) {
      changes.add(buildSetDomainProposal(domainUrn, resource, actor, entityService));
    }
    EntityUtils.ingestChangeProposals(changes, entityService, actor, false);
  }

  private static MetadataChangeProposal buildSetDomainProposal(
      @Nullable Urn domainUrn, ResourceRefInput resource, Urn actor, EntityService entityService) {
    Domains domains =
        (Domains)
            EntityUtils.getAspectFromEntity(
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

  public static void validateDomain(Urn domainUrn, EntityService entityService) {
    if (!entityService.exists(domainUrn)) {
      throw new IllegalArgumentException(
          String.format("Failed to validate Domain with urn %s. Urn does not exist.", domainUrn));
    }
  }

  private static List<Criterion> buildRootDomainCriteria() {
    final List<Criterion> criteria = new ArrayList<>();

    criteria.add(
        new Criterion()
            .setField(HAS_PARENT_DOMAIN_INDEX_FIELD_NAME)
            .setValue("false")
            .setCondition(Condition.EQUAL));
    criteria.add(
        new Criterion()
            .setField(HAS_PARENT_DOMAIN_INDEX_FIELD_NAME)
            .setValue("")
            .setCondition(Condition.IS_NULL));

    return criteria;
  }

  private static List<Criterion> buildParentDomainCriteria(@Nonnull final Urn parentDomainUrn) {
    final List<Criterion> criteria = new ArrayList<>();

    criteria.add(
        new Criterion()
            .setField(HAS_PARENT_DOMAIN_INDEX_FIELD_NAME)
            .setValue("true")
            .setCondition(Condition.EQUAL));
    criteria.add(
        new Criterion()
            .setField(PARENT_DOMAIN_INDEX_FIELD_NAME)
            .setValue(parentDomainUrn.toString())
            .setCondition(Condition.EQUAL));

    return criteria;
  }

  private static Criterion buildNameCriterion(@Nonnull final String name) {
    return new Criterion()
        .setField(NAME_INDEX_FIELD_NAME)
        .setValue(name)
        .setCondition(Condition.EQUAL);
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
            DOMAIN_ENTITY_NAME, parentDomainFilter, null, 0, 1, context.getAuthentication());
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
              DOMAIN_ENTITY_NAME, filter, null, 0, 1000, context.getAuthentication());

      final Set<Urn> domainUrns =
          searchResult.getEntities().stream()
              .map(SearchEntity::getEntity)
              .collect(Collectors.toSet());

      return entityClient.batchGetV2(
          DOMAIN_ENTITY_NAME,
          domainUrns,
          Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME),
          context.getAuthentication());
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
              DOMAIN_ENTITY_NAME,
              urn,
              Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME),
              context.getAuthentication());

      if (entityResponse != null
          && entityResponse.getAspects().containsKey(DOMAIN_PROPERTIES_ASPECT_NAME)) {
        final DomainProperties properties =
            new DomainProperties(
                entityResponse.getAspects().get(DOMAIN_PROPERTIES_ASPECT_NAME).getValue().data());
        final Urn parentDomainUrn = getParentDomainSafely(properties);
        return parentDomainUrn != null ? UrnToEntityMapper.map(parentDomainUrn) : null;
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
