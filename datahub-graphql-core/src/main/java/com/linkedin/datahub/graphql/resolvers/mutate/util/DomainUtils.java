package com.linkedin.datahub.graphql.resolvers.mutate.util;

import com.google.common.collect.ImmutableList;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.generated.Domain;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.datahub.graphql.types.domain.DomainMapper;
import com.linkedin.domain.DomainProperties;
import com.linkedin.domain.Domains;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.mxe.MetadataChangeProposal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;
import static com.linkedin.metadata.Constants.*;


// TODO: Move to consuming from DomainService.
@Slf4j
public class DomainUtils {
  public static final String PARENT_DOMAIN_INDEX_FIELD_NAME = "parentDomain.keyword";
  public static final String NAME_INDEX_FIELD_NAME = "name";

  private static final ConjunctivePrivilegeGroup ALL_PRIVILEGES_GROUP = new ConjunctivePrivilegeGroup(ImmutableList.of(
      PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()
  ));

  private DomainUtils() { }

  public static boolean isAuthorizedToUpdateDomainsForEntity(@Nonnull QueryContext context, Urn entityUrn) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups = new DisjunctivePrivilegeGroup(ImmutableList.of(
        ALL_PRIVILEGES_GROUP,
        new ConjunctivePrivilegeGroup(ImmutableList.of(PoliciesConfig.EDIT_ENTITY_DOMAINS_PRIVILEGE.getType()))
    ));

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
      EntityService entityService
  ) throws Exception {
    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceRefInput resource : resources) {
      changes.add(buildSetDomainProposal(domainUrn, resource, actor, entityService));
    }
    EntityUtils.ingestChangeProposals(changes, entityService, actor, false);
  }

  private static MetadataChangeProposal buildSetDomainProposal(
      @Nullable Urn domainUrn,
      ResourceRefInput resource,
      Urn actor,
      EntityService entityService
  ) {
    Domains domains = (Domains) EntityUtils.getAspectFromEntity(
        resource.getResourceUrn(),
        Constants.DOMAINS_ASPECT_NAME,
        entityService,
        new Domains());
    final UrnArray newDomains = new UrnArray();
    if (domainUrn != null) {
      newDomains.add(domainUrn);
    }
    domains.setDomains(newDomains);
    return buildMetadataChangeProposalWithUrn(UrnUtils.getUrn(resource.getResourceUrn()), Constants.DOMAINS_ASPECT_NAME, domains);
  }

  public static void validateDomain(Urn domainUrn, EntityService entityService) {
    if (!entityService.exists(domainUrn)) {
      throw new IllegalArgumentException(String.format("Failed to validate Domain with urn %s. Urn does not exist.", domainUrn));
    }
  }

  private static Criterion buildParentDomainCriterion(@Nullable final Urn parentDomainUrn) {
    return QueryUtils.newCriterion(
        PARENT_DOMAIN_INDEX_FIELD_NAME,
        parentDomainUrn == null ? "" : parentDomainUrn.toString(),
        parentDomainUrn == null ? Condition.IS_NULL : Condition.EQUAL
    );
  }

  private static Criterion buildNameCriterion(@Nonnull final String name) {
    return QueryUtils.newCriterion(NAME_INDEX_FIELD_NAME, name, Condition.EQUAL);
  }

  public static Filter buildParentDomainFilter(@Nullable final Urn parentDomainUrn) {
    return QueryUtils.newFilter(buildParentDomainCriterion(parentDomainUrn));
  }

  public static Filter buildNameAndParentDomainFilter(@Nonnull final String name, @Nullable final Urn parentDomainUrn) {
    final Criterion parentDomainCriterion = buildParentDomainCriterion(parentDomainUrn);
    final Criterion nameCriterion = buildNameCriterion(name);
    return QueryUtils.getFilterFromCriteria(List.of(parentDomainCriterion, nameCriterion));
  }

  private static Map<Urn, EntityResponse> getDomainsByNameAndParent(
      @Nonnull final String name,
      @Nullable final Urn parentDomainUrn,
      @Nonnull final QueryContext context,
      @Nonnull final EntityClient entityClient
  ) {
    try {
      final Filter filter = buildNameAndParentDomainFilter(name, parentDomainUrn);

      final SearchResult searchResult = entityClient.filter(
          DOMAIN_ENTITY_NAME,
          filter,
          null,
          0,
          1000,
          context.getAuthentication());

      final Set<Urn> domainUrns = searchResult.getEntities()
          .stream()
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
      @Nonnull final EntityClient entityClient
  ) {
    final Map<Urn, EntityResponse> entities = getDomainsByNameAndParent(name, parentDomainUrn, context, entityClient);

    // Even though we searched by name, do one more pass to check the name is unique
    return entities.values().stream().anyMatch(entityResponse -> {
      if (entityResponse.getAspects().containsKey(DOMAIN_PROPERTIES_ASPECT_NAME)) {
        DataMap dataMap = entityResponse.getAspects().get(DOMAIN_PROPERTIES_ASPECT_NAME).getValue().data();
        DomainProperties domainProperties = new DomainProperties(dataMap);
        return (domainProperties.hasName() && domainProperties.getName().equals(name));
      }
      return false;
    });
  }

  @Nullable
  public static Domain getParentDomain(
      @Nonnull final String urnStr,
      @Nonnull final QueryContext context,
      @Nonnull final EntityClient entityClient
  ) {
    try {
      Urn urn = Urn.createFromString(urnStr);
      EntityResponse entityResponse = entityClient.getV2(
          urn.getEntityType(),
          urn,
          Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME),
          context.getAuthentication()
      );

      if (entityResponse != null && entityResponse.getAspects().containsKey(DOMAIN_PROPERTIES_ASPECT_NAME)) {
        DomainProperties properties = new DomainProperties(entityResponse.getAspects().get(DOMAIN_PROPERTIES_ASPECT_NAME).getValue().data());
        if (properties.hasParentDomain()) {
          Urn parentDomainUrn = properties.getParentDomain();
          if (parentDomainUrn != null) {
            EntityResponse parentResponse = entityClient.getV2(
                parentDomainUrn.getEntityType(),
                parentDomainUrn,
                Collections.singleton(DOMAIN_KEY_ASPECT_NAME),
                context.getAuthentication()
            );
            if (parentResponse != null) {
              return DomainMapper.map(parentResponse);
            }
          }
        }
      }

      return null;
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to retrieve parent domain for entity %s", urnStr), e);
    }
  }
}