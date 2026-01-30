package com.linkedin.datahub.graphql.resolvers.mutate.util;

import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;
import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;
import static com.linkedin.metadata.utils.CriterionUtils.buildIsNullCriterion;

import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.datahub.authorization.DomainAuthorizationHelper;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
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
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
      @Nonnull QueryContext context, Urn entityUrn, EntityClient entityClient) {

    if (GlossaryUtils.canUpdateGlossaryEntity(entityUrn, context, entityClient)) {
      return true;
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
      EntityService<?> entityService,
      boolean isDomainBasedAuthorizationEnabled)
      throws Exception {
    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceRefInput resource : resources) {
      changes.add(buildSetDomainProposal(opContext, domainUrn, resource, actor, entityService));
    }

    // Only perform domain-based authorization if enabled
    // When disabled, authorization is handled by caller (e.g., BatchSetDomainResolver)
    if (isDomainBasedAuthorizationEnabled) {
      // Perform API-level domain-based authorization before ingestion
      // For domain changes, we need to check authorization for BOTH existing and proposed domains
      Map<Urn, DomainChange> domainChanges =
          extractDomainChangesFromMCPs(changes, opContext, entityService);
      Map<MetadataChangeProposal, Boolean> authResults =
          authorizeMCPsWithDomainChanges(opContext, changes, domainChanges);

      // Check authorization results and throw if any MCP is unauthorized
      authResults.forEach(
          (mcp, authorized) -> {
            if (!authorized) {
              throw new AuthorizationException(
                  String.format(
                      "Unauthorized to update domains for entity %s", mcp.getEntityUrn()));
            }
          });
    }

    EntityUtils.ingestChangeProposals(opContext, changes, entityService, actor, false);
  }

  /**
   * Extract domain changes (existing → proposed) from MetadataChangeProposals.
   *
   * @param mcps List of MetadataChangeProposals to extract domain changes from
   * @param opContext Operation context
   * @param entityService Entity service for reading existing domains
   * @return Map of entity URN to DomainChange (existing and proposed domains)
   */
  private static Map<Urn, DomainChange> extractDomainChangesFromMCPs(
      @Nonnull List<MetadataChangeProposal> mcps,
      @Nonnull OperationContext opContext,
      @Nonnull EntityService<?> entityService) {
    Map<Urn, DomainChange> changes = new HashMap<>();

    for (MetadataChangeProposal mcp : mcps) {
      if (DOMAINS_ASPECT_NAME.equals(mcp.getAspectName())) {
        try {
          Urn entityUrn = mcp.getEntityUrn();

          // Extract proposed domains from MCP
          Domains proposedDomains =
              GenericRecordUtils.deserializeAspect(
                  mcp.getAspect().getValue(), mcp.getAspect().getContentType(), Domains.class);
          Set<Urn> proposedDomainUrns =
              (proposedDomains != null && proposedDomains.getDomains() != null)
                  ? new HashSet<>(proposedDomains.getDomains())
                  : Collections.emptySet();

          // Read existing domains from database
          Set<Urn> existingDomainUrns = Collections.emptySet();
          try {
            Domains existingDomains =
                (Domains)
                    EntityUtils.getAspectFromEntity(
                        opContext, entityUrn.toString(), DOMAINS_ASPECT_NAME, entityService, null);
            if (existingDomains != null && existingDomains.getDomains() != null) {
              existingDomainUrns = new HashSet<>(existingDomains.getDomains());
            }
          } catch (Exception e) {
            log.debug(
                "Failed to read existing domains for entity {}, assuming CREATE operation",
                entityUrn);
            // Entity doesn't exist - this is a CREATE operation, no existing domains
          }

          changes.put(entityUrn, new DomainChange(existingDomainUrns, proposedDomainUrns));
        } catch (Exception e) {
          log.warn("Failed to extract domain change from MCP for entity {}", mcp.getEntityUrn(), e);
        }
      }
    }
    return changes;
  }

  /**
   * Authorize MCPs with domain changes, checking both existing and proposed domains.
   *
   * @param opContext Operation context
   * @param mcps List of MCPs to authorize
   * @param domainChanges Map of entity URN to domain changes
   * @return Map of MCP to authorization result
   */
  private static Map<MetadataChangeProposal, Boolean> authorizeMCPsWithDomainChanges(
      @Nonnull OperationContext opContext,
      @Nonnull List<MetadataChangeProposal> mcps,
      @Nonnull Map<Urn, DomainChange> domainChanges) {

    Map<MetadataChangeProposal, Boolean> results = new HashMap<>();

    for (MetadataChangeProposal mcp : mcps) {
      Urn entityUrn = mcp.getEntityUrn();
      DomainChange domainChange = domainChanges.get(entityUrn);

      if (domainChange == null) {
        // No domain change for this MCP, use standard authorization
        Map<Urn, Set<Urn>> noDomains = Collections.emptyMap();
        Map<MetadataChangeProposal, Boolean> standardAuth =
            DomainAuthorizationHelper.authorizeWithDomains(
                opContext,
                opContext.getEntityRegistry(),
                Collections.singletonList(mcp),
                noDomains,
                opContext.getAspectRetriever());
        results.put(mcp, standardAuth.get(mcp));
        continue;
      }

      Set<Urn> existingDomains = domainChange.existingDomains;
      Set<Urn> proposedDomains = domainChange.proposedDomains;

      // If domains are changing, check authorization for BOTH existing and proposed
      if (!existingDomains.isEmpty() && !existingDomains.equals(proposedDomains)) {
        log.debug(
            "Domain change detected for entity {}: {} -> {}",
            entityUrn,
            existingDomains,
            proposedDomains);

        // Check permission for existing domain (to remove entity from it)
        Map<MetadataChangeProposal, Boolean> existingAuth =
            DomainAuthorizationHelper.authorizeWithDomains(
                opContext,
                opContext.getEntityRegistry(),
                Collections.singletonList(mcp),
                Collections.singletonMap(entityUrn, existingDomains),
                opContext.getAspectRetriever());

        boolean canRemoveFromExisting = existingAuth.getOrDefault(mcp, false);

        if (!canRemoveFromExisting) {
          log.warn(
              "User does not have permission to remove entity {} from existing domains {}",
              entityUrn,
              existingDomains);
          results.put(mcp, false);
          continue;
        }
      }

      // Check permission for proposed domain (to add entity to it)
      Map<MetadataChangeProposal, Boolean> proposedAuth =
          DomainAuthorizationHelper.authorizeWithDomains(
              opContext,
              opContext.getEntityRegistry(),
              Collections.singletonList(mcp),
              Collections.singletonMap(entityUrn, proposedDomains),
              opContext.getAspectRetriever());

      results.put(mcp, proposedAuth.getOrDefault(mcp, false));
    }

    return results;
  }

  /** Helper class to track domain changes (existing → proposed) */
  private static class DomainChange {
    final Set<Urn> existingDomains;
    final Set<Urn> proposedDomains;

    DomainChange(Set<Urn> existingDomains, Set<Urn> proposedDomains) {
      this.existingDomains = existingDomains != null ? existingDomains : Collections.emptySet();
      this.proposedDomains = proposedDomains != null ? proposedDomains : Collections.emptySet();
    }
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
