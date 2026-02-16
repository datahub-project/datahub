package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.DOMAINS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_ENTITY_NAME;
import static com.linkedin.metadata.authorization.ApiGroup.ENTITY;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationSession;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.datahub.authorization.EntitySpec;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.domain.Domains;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.authorization.ApiOperation;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * Validator that performs domain-based authorization checks inside the transaction using simplified
 * 3-step approach.
 *
 * <p>This validator:
 *
 * <ol>
 *   <li>Extracts proposed domains from incoming MCPs
 *   <li>Checks if entity exists
 *   <li>Performs authorization (FieldResolver automatically fetches existing domains/aspects)
 * </ol>
 *
 * <p>Authorization logic:
 *
 * <ul>
 *   <li>Entities changing domains + existing: Check proposed domains AND existing entity
 *   <li>Entities changing domains + new: Check proposed domains only
 *   <li>Entities not changing domains: Standard authorization (FieldResolver fetches existing
 *       domains)
 * </ul>
 */
@Setter
@Getter
@Slf4j
@Accessors(chain = true)
public class DomainBasedAuthorizationValidator extends AspectPayloadValidator {
  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    // Validation happens in validatePreCommitAspects (inside transaction)
    return Stream.empty();
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull Collection<ChangeMCP> changeMCPs, @Nonnull RetrieverContext retrieverContext) {

    AuthorizationSession session = retrieverContext.getAuthorizationSession();

    // Skip domain-based authorization if no session (e.g., system/ingestion operations)
    if (session == null) {
      log.debug(
          "DomainBasedAuthorizationValidator: No authentication session provided, skipping domain-based authorization");
      return Stream.empty();
    }

    AspectRetriever aspectRetriever = retrieverContext.getAspectRetriever();

    // Group changes by entity URN, filtering out DELETE operations
    Map<Urn, List<ChangeMCP>> changesByEntity =
        changeMCPs.stream()
            .filter(
                changeMCP ->
                    !EXECUTION_REQUEST_ENTITY_NAME.equals(changeMCP.getUrn().getEntityType()))
            .filter(
                changeMCP ->
                    ApiOperation.fromChangeType(changeMCP.getChangeType()) != ApiOperation.DELETE)
            .collect(Collectors.groupingBy(ChangeMCP::getUrn));

    return changesByEntity.entrySet().stream()
        .flatMap(
            entry -> {
              Urn entityUrn = entry.getKey();
              List<ChangeMCP> entityChanges = entry.getValue();

              // Determine the operation type
              ApiOperation operation =
                  entityChanges.stream()
                      .map(changeMCP -> ApiOperation.fromChangeType(changeMCP.getChangeType()))
                      .findFirst()
                      .orElse(ApiOperation.UPDATE);

              // Extract proposed domains from MCPs (domains being set/changed)
              Set<Urn> proposedDomains = getDomainsFromMCPs(entityChanges);

              // Check if entity exists
              boolean entityExists = false;
              try {
                Map<Urn, Boolean> existsMap = aspectRetriever.entityExists(Set.of(entityUrn));
                entityExists = existsMap.getOrDefault(entityUrn, false);
              } catch (Exception e) {
                log.warn("Error checking existence for entity {}: {}", entityUrn, e.getMessage());
              }

              // Authorize using simplified 3-step approach
              boolean isAuthorized =
                  isAuthorizedSimplified(
                      session, operation, entityUrn, proposedDomains, entityExists);

              if (!isAuthorized) {
                log.warn(
                    "DomainBasedAuthorizationValidator: BLOCKED - Unauthorized to {} entity {}",
                    operation,
                    entityUrn);
                return Stream.of(
                    AspectValidationException.forAuth(
                        entityChanges.get(0),
                        String.format("Unauthorized to %s entity %s", operation, entityUrn)));
              }

              return Stream.empty();
            });
  }

  /** Extract domains from MCPs */
  private Set<Urn> getDomainsFromMCPs(Collection<ChangeMCP> changeMCPs) {
    return changeMCPs.stream()
        .filter(changeMCP -> DOMAINS_ASPECT_NAME.equals(changeMCP.getAspectName()))
        .flatMap(
            changeMCP -> {
              try {
                Domains domains = changeMCP.getAspect(Domains.class);
                if (domains != null && domains.getDomains() != null) {
                  return domains.getDomains().stream();
                }
              } catch (Exception e) {
                log.warn(
                    "Failed to extract domains from MCP for entity {}: {}",
                    changeMCP.getUrn(),
                    e.getMessage());
              }
              return Stream.empty();
            })
        .collect(Collectors.toSet());
  }

  /**
   * Simplified authorization using 3-step approach consistent with AuthUtil.
   *
   * <p>Authorization Logic:
   *
   * <ul>
   *   <li>Entities with proposed domains + existing: Check proposed domains AND existing entity
   *   <li>Entities with proposed domains + new: Check proposed domains only
   *   <li>Existing entities without proposed domains: Standard authorization (FieldResolver fetches
   *       existing domains)
   *   <li>New entities without proposed domains: Standard authorization
   * </ul>
   *
   * @param session Authorization session
   * @param operation API operation (CREATE, UPDATE, DELETE)
   * @param entityUrn Entity URN to authorize
   * @param proposedDomains Proposed domain URNs from MCP (empty if not changing domains)
   * @param entityExists Whether the entity already exists in the database
   * @return true if authorized, false otherwise
   */
  private boolean isAuthorizedSimplified(
      @Nonnull AuthorizationSession session,
      @Nonnull ApiOperation operation,
      @Nonnull Urn entityUrn,
      @Nonnull Set<Urn> proposedDomains,
      boolean entityExists) {

    boolean hasProposedDomains = !proposedDomains.isEmpty();

    // Case 1: Entity is changing domains AND exists - must pass BOTH checks
    if (hasProposedDomains && entityExists) {
      // Check 1: Authorize with proposed domains
      boolean proposedAuth = authorizeWithDomains(session, operation, entityUrn, proposedDomains);
      if (!proposedAuth) {
        log.warn(
            "User does not have permission to add entity {} to proposed domains {}",
            entityUrn,
            proposedDomains);
        return false;
      }

      // Check 2: Authorize existing entity (FieldResolver fetches existing domains automatically)
      boolean existingAuth =
          AuthUtil.isAPIAuthorizedEntityUrns(session, operation, List.of(entityUrn));
      if (!existingAuth) {
        log.warn("User does not have permission to modify existing entity {}", entityUrn);
        return false;
      }

      return true;
    }

    // Case 2: Entity is changing domains but is NEW - only check proposed domains
    if (hasProposedDomains) {
      return authorizeWithDomains(session, operation, entityUrn, proposedDomains);
    }

    // Case 3 & 4: Entity not changing domains - standard authorization
    // FieldResolver automatically fetches existing domains for existing entities
    return AuthUtil.isAPIAuthorizedEntityUrns(session, operation, List.of(entityUrn));
  }

  /**
   * Authorize with specific domains by creating enriched EntitySpec with proposed domains.
   * FieldResolver will fetch other aspects (ownership, tags, etc.) automatically.
   *
   * @param session Authorization session
   * @param operation API operation
   * @param entityUrn Entity URN
   * @param domains Domain URNs to authorize
   * @return true if authorized, false otherwise
   */
  private boolean authorizeWithDomains(
      @Nonnull AuthorizationSession session,
      @Nonnull ApiOperation operation,
      @Nonnull Urn entityUrn,
      @Nonnull Set<Urn> domains) {

    // Build proposed Domains aspect
    Domains domainsAspect = new Domains();
    domainsAspect.setDomains(new UrnArray(new ArrayList<>(domains)));

    // Create enriched EntitySpec with proposed domains only
    // FieldResolver will fetch other aspects automatically during authorization
    Map<String, RecordTemplate> proposedAspects = new HashMap<>();
    proposedAspects.put(DOMAINS_ASPECT_NAME, domainsAspect);

    EntitySpec enrichedSpec =
        new EntitySpec(entityUrn.getEntityType(), entityUrn.toString(), proposedAspects);

    // Authorize using enriched EntitySpec
    DisjunctivePrivilegeGroup privilegeGroup =
        AuthUtil.buildDisjunctivePrivilegeGroup(ENTITY, operation, entityUrn.getEntityType());

    return AuthUtil.isAuthorized(session, privilegeGroup, enrichedSpec, Collections.emptyList());
  }
}
