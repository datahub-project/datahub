package com.datahub.authorization;

import static com.datahub.authorization.AuthUtil.isAPIAuthorizedEntityUrns;
import static com.linkedin.metadata.authorization.ApiGroup.ENTITY;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.domain.Domains;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.authorization.ApiOperation;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Helper class for domain-based authorization of MetadataChangeProposals.
 *
 * <p>This class implements a 3-step authorization approach:
 *
 * <ol>
 *   <li>For URNs with domain changes, authorize individually against proposed domains
 *   <li>Get set of existing URNs (simple existence check)
 *   <li>Bulk authorize existing URNs (FieldResolver fetches existing domains automatically)
 * </ol>
 */
@Slf4j
public class DomainAuthorizationHelper {

  private DomainAuthorizationHelper() {
    // Utility class - prevent instantiation
  }

  /**
   * Authorize MetadataChangeProposals with optional domain-based authorization.
   *
   * <p>AUTHORIZATION MODES:
   *
   * <ul>
   *   <li><b>Domain-based authorization ENABLED</b> (newDomainsByEntity != null && !empty): For
   *       entities changing domains, performs dual authorization: - Check 1: NEW domains using
   *       EntitySpec with proposedAspects - Check 2: EXISTING domains using standard URN
   *       authorization (only if entity exists) For entities not changing domains, uses standard
   *       URN authorization
   *   <li><b>Domain-based authorization DISABLED</b> (newDomainsByEntity == null || empty): Uses
   *       standard entity URN authorization without domain context
   * </ul>
   *
   * @param opContext Operation context containing authorization session
   * @param entityRegistry Entity registry for URN resolution
   * @param mcps Collection of MCPs to authorize
   * @param newDomainsByEntity Map of entity URN to their NEW domain URNs being proposed in MCPs.
   *     Only contains entries for entities that are changing their domains aspect. Pass null or
   *     empty map to use standard authorization without domain context.
   * @param aspectRetriever AspectRetriever for checking entity existence (required for domain-based
   *     auth)
   * @return Map of MCP to authorization result (true if authorized, false otherwise)
   */
  public static Map<MetadataChangeProposal, Boolean> authorizeWithDomains(
      @Nonnull final OperationContext opContext,
      @Nonnull final EntityRegistry entityRegistry,
      @Nonnull final Collection<MetadataChangeProposal> mcps,
      @Nullable final Map<Urn, Set<Urn>> newDomainsByEntity,
      @Nullable final AspectRetriever aspectRetriever) {

    boolean useDomainAuth = newDomainsByEntity != null && !newDomainsByEntity.isEmpty();

    if (!useDomainAuth) {
      // Domain-based authorization is DISABLED - use existing standard flow
      Map<MetadataChangeProposal, Boolean> results = new HashMap<>();
      Map<ApiOperation, List<Pair<MetadataChangeProposal, Urn>>> mcpsByOperation =
          groupMCPsByOperation(mcps, entityRegistry);
      authorizeMCPsStandard(opContext, mcpsByOperation, results);
      return results;
    }

    // Domain-based authorization is ENABLED - use new dual-check flow
    return authorizeMCPsWithDomainBasedAuth(
        opContext, entityRegistry, mcps, newDomainsByEntity, aspectRetriever);
  }

  /**
   * Authorize MCPs with domain-based authorization using simplified 3-step approach. Only called
   * when domain-based authorization is enabled.
   *
   * <p>Authorization Flow:
   *
   * <ol>
   *   <li>Step 1: For each URN with domain changes, authorize against proposed domains individually
   *   <li>Step 2: Get set of existing URNs
   *   <li>Step 3: Bulk authorize all existing URNs (FieldResolver fetches existing domains
   *       automatically)
   * </ol>
   */
  private static Map<MetadataChangeProposal, Boolean> authorizeMCPsWithDomainBasedAuth(
      @Nonnull final OperationContext opContext,
      @Nonnull final EntityRegistry entityRegistry,
      @Nonnull final Collection<MetadataChangeProposal> mcps,
      @Nonnull final Map<Urn, Set<Urn>> newDomainsByEntity,
      @Nullable final AspectRetriever aspectRetriever) {

    Map<MetadataChangeProposal, Boolean> results = new HashMap<>();
    Map<ApiOperation, List<Pair<MetadataChangeProposal, Urn>>> mcpsByOperation =
        groupMCPsByOperation(mcps, entityRegistry);

    if (aspectRetriever == null) {
      log.warn("AspectRetriever not available - cannot perform domain-based authorization checks");
      // Fall back to standard authorization
      authorizeMCPsStandard(opContext, mcpsByOperation, results);
      return results;
    }

    // Step 2: Get set of existing URNs (from all MCPs)
    Set<Urn> allUrns =
        mcpsByOperation.values().stream()
            .flatMap(List::stream)
            .map(Pair::getSecond)
            .collect(Collectors.toSet());
    Set<Urn> existingUrns = batchGetExistingUrns(aspectRetriever, allUrns);

    // Process each operation
    for (Map.Entry<ApiOperation, List<Pair<MetadataChangeProposal, Urn>>> entry :
        mcpsByOperation.entrySet()) {
      ApiOperation operation = entry.getKey();
      List<Pair<MetadataChangeProposal, Urn>> mcpUrnPairs = entry.getValue();

      // Step 1: Authorize URNs with domain changes (individual checks with proposed domains)
      Map<Urn, Boolean> domainChangeAuthResults = new HashMap<>();
      for (Pair<MetadataChangeProposal, Urn> pair : mcpUrnPairs) {
        Urn urn = pair.getSecond();
        if (newDomainsByEntity.containsKey(urn)) {
          Set<Urn> proposedDomains = newDomainsByEntity.get(urn);
          boolean authorized =
              isAPIAuthorizedWithDomains(opContext, operation, urn, proposedDomains);
          domainChangeAuthResults.put(urn, authorized);

          if (!authorized) {
            log.warn(
                "User does not have permission to add entity {} to proposed domains {}",
                urn,
                proposedDomains);
          }
        }
      }

      // Step 3: Bulk authorize existing URNs (FieldResolver fetches existing domains per URN)
      Set<Urn> existingUrnsInThisOperation =
          mcpUrnPairs.stream()
              .map(Pair::getSecond)
              .filter(existingUrns::contains)
              .collect(Collectors.toSet());

      boolean existingUrnsAuthorized = true;
      if (!existingUrnsInThisOperation.isEmpty()) {
        existingUrnsAuthorized =
            isAPIAuthorizedEntityUrns(opContext, operation, existingUrnsInThisOperation);

        if (!existingUrnsAuthorized) {
          log.warn(
              "Bulk authorization failed for {} existing entities in operation {}",
              existingUrnsInThisOperation.size(),
              operation);
        }
      }

      // Combine results for each MCP
      // First pass: determine if ALL MCPs are authorized ("all or nothing")
      boolean allAuthorizedInOperation = true;
      for (Pair<MetadataChangeProposal, Urn> pair : mcpUrnPairs) {
        Urn urn = pair.getSecond();

        boolean isChangingDomains = newDomainsByEntity.containsKey(urn);
        boolean isExisting = existingUrns.contains(urn);

        boolean mcpAuthorized;
        // For entities changing domains: must pass BOTH checks
        if (isChangingDomains && isExisting) {
          boolean domainChangeAuth = domainChangeAuthResults.getOrDefault(urn, false);
          mcpAuthorized = domainChangeAuth && existingUrnsAuthorized;
        }
        // For entities changing domains but NOT existing: only need domain change check
        else if (isChangingDomains) {
          mcpAuthorized = domainChangeAuthResults.getOrDefault(urn, false);
        }
        // For existing entities NOT changing domains: only need existing entity check
        else if (isExisting) {
          mcpAuthorized = existingUrnsAuthorized;
        }
        // For new entities NOT changing domains: use standard authorization
        else {
          mcpAuthorized = isAPIAuthorizedEntityUrns(opContext, operation, List.of(urn));
        }

        if (!mcpAuthorized) {
          allAuthorizedInOperation = false;
          break; // Short-circuit: if any MCP is denied, entire batch is denied
        }
      }

      // Second pass: apply "all or nothing" result to all MCPs
      for (Pair<MetadataChangeProposal, Urn> pair : mcpUrnPairs) {
        results.put(pair.getFirst(), allAuthorizedInOperation);
      }
    }

    return results;
  }

  /**
   * Get set of URNs that exist in the database.
   *
   * @param aspectRetriever AspectRetriever for checking existence
   * @param urns Set of URNs to check
   * @return Set of URNs that exist
   */
  @Nonnull
  private static Set<Urn> batchGetExistingUrns(
      @Nonnull final AspectRetriever aspectRetriever, @Nonnull final Set<Urn> urns) {
    try {
      // Batch check existence for all URNs at once
      Map<Urn, Boolean> existsMap = aspectRetriever.entityExists(urns);
      return existsMap.entrySet().stream()
          .filter(Map.Entry::getValue)
          .map(Map.Entry::getKey)
          .collect(Collectors.toSet());
    } catch (Exception e) {
      log.warn("Error checking existence for entities: {}", e.getMessage());
      // Return empty set if error - treat all as non-existent
      return Collections.emptySet();
    }
  }

  /** Group MCPs by their operation type (CREATE, UPDATE, DELETE) for batch processing. */
  private static Map<ApiOperation, List<Pair<MetadataChangeProposal, Urn>>> groupMCPsByOperation(
      @Nonnull final Collection<MetadataChangeProposal> mcps,
      @Nonnull final EntityRegistry entityRegistry) {

    Map<ApiOperation, List<Pair<MetadataChangeProposal, Urn>>> mcpsByOperation = new HashMap<>();

    for (MetadataChangeProposal mcp : mcps) {
      Urn urn = getUrnFromMCP(mcp, entityRegistry);
      ApiOperation operation = getOperationFromChangeType(mcp.getChangeType());
      mcpsByOperation.computeIfAbsent(operation, k -> new ArrayList<>()).add(Pair.of(mcp, urn));
    }

    return mcpsByOperation;
  }

  /**
   * Authorize MCPs using standard entity URN authorization (no domain context). All operations use
   * basic entity-level permissions only.
   *
   * <p>IMPORTANT: Uses "all or nothing" authorization - if ANY entity in the batch is denied, the
   * ENTIRE batch is denied.
   */
  private static void authorizeMCPsStandard(
      @Nonnull final OperationContext opContext,
      @Nonnull final Map<ApiOperation, List<Pair<MetadataChangeProposal, Urn>>> mcpsByOperation,
      @Nonnull final Map<MetadataChangeProposal, Boolean> results) {

    for (Map.Entry<ApiOperation, List<Pair<MetadataChangeProposal, Urn>>> entry :
        mcpsByOperation.entrySet()) {
      ApiOperation operation = entry.getKey();
      List<Pair<MetadataChangeProposal, Urn>> mcpUrnPairs = entry.getValue();

      // Check each URN individually to detect partial authorization
      boolean allAuthorized = true;
      for (Pair<MetadataChangeProposal, Urn> pair : mcpUrnPairs) {
        Urn urn = pair.getSecond();
        boolean authorized =
            isAPIAuthorizedEntityUrns(opContext, operation, Collections.singletonList(urn));
        if (!authorized) {
          allAuthorized = false;
          break; // Short-circuit: if any is denied, entire batch is denied
        }
      }

      // Apply "all or nothing" result to all MCPs in this batch
      for (Pair<MetadataChangeProposal, Urn> pair : mcpUrnPairs) {
        results.put(pair.getFirst(), allAuthorized);
      }
    }
  }

  /** Extract URN from MCP, generating it from entity key if not present. */
  @Nonnull
  private static Urn getUrnFromMCP(
      @Nonnull final MetadataChangeProposal mcp, @Nonnull final EntityRegistry entityRegistry) {
    Urn urn = mcp.getEntityUrn();
    if (urn == null) {
      com.linkedin.metadata.models.EntitySpec entitySpec =
          entityRegistry.getEntitySpec(mcp.getEntityType());
      urn = EntityKeyUtils.getUrnFromProposal(mcp, entitySpec.getKeyAspectSpec());
    }
    return urn;
  }

  /** Convert ChangeType to ApiOperation. */
  @Nonnull
  private static ApiOperation getOperationFromChangeType(@Nonnull final ChangeType changeType) {
    switch (changeType) {
      case CREATE:
      case CREATE_ENTITY:
        return ApiOperation.CREATE;
      case DELETE:
        return ApiOperation.DELETE;
      case UPSERT:
      case UPDATE:
      case RESTATE:
      case PATCH:
      default:
        return ApiOperation.UPDATE;
    }
  }

  /**
   * Authorizes an entity URN with domains by creating an enriched EntitySpec.
   *
   * <p>IMPORTANT: For domain changes (UPDATE operations), the caller must check authorization for
   * BOTH existing and new domains separately to prevent unauthorized domain transfers. This method
   * only checks authorization for the provided domains.
   *
   * @param opContext Operation context containing authorization session
   * @param operation API operation (CREATE, UPDATE, DELETE)
   * @param urn Entity URN to authorize
   * @param domains Domain URNs to check authorization for
   * @return true if authorized, false otherwise
   */
  private static boolean isAPIAuthorizedWithDomains(
      @Nonnull final OperationContext opContext,
      @Nonnull final ApiOperation operation,
      @Nonnull final Urn urn,
      @Nonnull final Set<Urn> domains) {

    if (domains.isEmpty()) {
      // No domains, use standard authorization
      return isAPIAuthorizedEntityUrns(opContext, operation, List.of(urn));
    }

    // Build proposed Domains aspect
    Domains domainsAspect = new Domains();
    domainsAspect.setDomains(new UrnArray(new ArrayList<>(domains)));

    // Create enriched EntitySpec with proposed domains
    Map<String, RecordTemplate> proposedAspects = new HashMap<>();
    proposedAspects.put("domains", domainsAspect);

    EntitySpec enrichedSpec = new EntitySpec(urn.getEntityType(), urn.toString(), proposedAspects);

    // Authorize using enriched EntitySpec
    DisjunctivePrivilegeGroup privilegeGroup =
        AuthUtil.buildDisjunctivePrivilegeGroup(ENTITY, operation, urn.getEntityType());

    return AuthUtil.isAuthorized(opContext, privilegeGroup, enrichedSpec, Collections.emptyList());
  }
}
