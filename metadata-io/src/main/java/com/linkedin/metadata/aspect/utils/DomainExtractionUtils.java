package com.linkedin.metadata.aspect.utils;

import static com.linkedin.metadata.Constants.DOMAINS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_ENTITY_NAME;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Entity;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.NewModelUtils;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility class for extracting domain information from entities and MetadataChangeProposals. Used
 * by API resources (OpenAPI, RestLI, GraphQL) to support domain-based authorization.
 */
@Slf4j
public class DomainExtractionUtils {

  private DomainExtractionUtils() {
    // Utility class - prevent instantiation
  }

  /**
   * Result of extracting domains for authorization. Explicitly separates entities with new domains
   * (from MCPs) vs existing domains (from database).
   */
  @Data
  @Builder
  @AllArgsConstructor
  public static class EntityDomainsResult {
    /**
     * Entities with NEW domains being proposed (from MCPs). These entities are changing their
     * domains aspect. Authorization must check both existing AND new domains for these entities.
     */
    @Nonnull private final Map<Urn, Set<Urn>> entitiesWithNewDomains;

    /**
     * Entities with EXISTING domains (from database). These entities are NOT changing their domains
     * aspect. Authorization only needs to check existing domains for these entities.
     */
    @Nonnull private final Map<Urn, Set<Urn>> entitiesWithExistingDomains;

    /**
     * Get all domains for a given entity URN for authorization. Returns new domains if entity is
     * changing domains, otherwise returns existing domains.
     *
     * @param entityUrn the entity URN
     * @return Set of domain URNs for authorization, empty if none found
     */
    @Nonnull
    public Set<Urn> getDomainsForEntity(@Nonnull Urn entityUrn) {
      // Check new domains first (takes precedence if entity is changing domains)
      Set<Urn> newDomains = entitiesWithNewDomains.get(entityUrn);
      if (newDomains != null) {
        return newDomains;
      }

      // Fall back to existing domains
      Set<Urn> existingDomains = entitiesWithExistingDomains.get(entityUrn);
      if (existingDomains != null) {
        return existingDomains;
      }

      return Collections.emptySet();
    }

    /**
     * Check if an entity is changing its domains.
     *
     * @param entityUrn the entity URN
     * @return true if entity has new domains being proposed
     */
    public boolean isChangingDomains(@Nonnull Urn entityUrn) {
      return entitiesWithNewDomains.containsKey(entityUrn);
    }

    /**
     * Get all entity URNs that are changing domains.
     *
     * @return Set of entity URNs with domain changes
     */
    @Nonnull
    public Set<Urn> getEntitiesChangingDomains() {
      return entitiesWithNewDomains.keySet();
    }

    /**
     * Convert to the legacy format (single map) for backward compatibility. Returns a map where
     * values have mixed semantics (new or existing domains).
     *
     * @deprecated Use the structured fields instead for clarity
     * @return Combined map of entity URN to domains
     */
    @Deprecated
    @Nonnull
    public Map<Urn, Set<Urn>> toLegacyFormat() {
      Map<Urn, Set<Urn>> combined = new HashMap<>(entitiesWithExistingDomains);
      combined.putAll(entitiesWithNewDomains); // New domains override existing
      return combined;
    }
  }

  /**
   * Extract domains from a MetadataChangeProposal's Domains aspect. Handles both regular
   * UPSERT/CREATE operations and PATCH operations.
   *
   * @param mcp The MetadataChangeProposal containing the aspect
   * @return Set of domain URNs found in the aspect, or empty set if none found
   */
  @Nonnull
  public static Set<Urn> extractDomainsFromMCP(@Nonnull MetadataChangeProposal mcp) {
    try {
      if (mcp.getAspect() == null || mcp.getAspect().getValue() == null) {
        return Collections.emptySet();
      }

      String aspectValue = mcp.getAspect().getValue().asString(StandardCharsets.UTF_8);

      // Handle PATCH operations differently - they contain JSON Patch documents
      if (mcp.getChangeType() == com.linkedin.events.metadata.ChangeType.PATCH) {
        return extractDomainsFromPatchDocument(aspectValue);
      }

      // Regular UPSERT/CREATE operations - parse as Domains object
      Domains domains = RecordUtils.toRecordTemplate(Domains.class, aspectValue);

      if (domains.getDomains() != null && !domains.getDomains().isEmpty()) {
        return new HashSet<>(domains.getDomains());
      }
    } catch (Exception e) {
      log.warn(
          "Error parsing domains from MCP for entity {}: {}", mcp.getEntityUrn(), e.getMessage());
    }
    return Collections.emptySet();
  }

  /**
   * Extract domains from a RestLI Entity object's snapshot. The Entity format is used by RestLI
   * APIs and contains a snapshot with aspects.
   *
   * @param entity The Entity object containing the snapshot with aspects
   * @return Set of domain URNs found in the entity, or empty set if none found
   */
  @Nonnull
  public static Set<Urn> extractDomainsFromEntity(@Nonnull Entity entity) {
    try {
      // Get the snapshot from the entity
      Snapshot snapshot = entity.getValue();
      if (snapshot == null) {
        return Collections.emptySet();
      }

      // Convert snapshot union to concrete snapshot record
      RecordTemplate snapshotRecord = RecordUtils.getSelectedRecordTemplateFromUnion(snapshot);

      // Extract all aspects from the snapshot using NewModelUtils
      List<Pair<String, RecordTemplate>> aspects =
          NewModelUtils.getAspectsFromSnapshot(snapshotRecord);

      // Find Domains aspect
      for (Pair<String, RecordTemplate> aspectPair : aspects) {
        if (DOMAINS_ASPECT_NAME.equals(aspectPair.getFirst())) {
          RecordTemplate aspectRecord = aspectPair.getSecond();
          if (aspectRecord instanceof Domains) {
            Domains domains = (Domains) aspectRecord;
            if (domains.getDomains() != null && !domains.getDomains().isEmpty()) {
              return new HashSet<>(domains.getDomains());
            }
          }
        }
      }
    } catch (Exception e) {
      log.warn("Error extracting domains from Entity: {}", e.getMessage());
    }
    return Collections.emptySet();
  }

  /**
   * Extract domain URNs from a JSON Patch document. Parses patch operations to find domain URNs
   * being added or replaced.
   *
   * @param patchDocument The JSON Patch document as a string
   * @return Set of domain URNs found in the patch, or empty set if none found
   */
  @Nonnull
  private static Set<Urn> extractDomainsFromPatchDocument(@Nonnull String patchDocument) {
    Set<Urn> domainUrns = new HashSet<>();

    try {
      // Deserialize to GenericJsonPatch for structured access
      ObjectMapper mapper = new ObjectMapper();
      GenericJsonPatch patch = mapper.readValue(patchDocument, GenericJsonPatch.class);

      // Iterate through patch operations
      for (GenericJsonPatch.PatchOp op : patch.getPatch()) {
        String path = op.getPath();
        Object value = op.getValue();

        // Check if this operation is modifying domains
        if (path != null && (path.startsWith("/domains") || path.equals("/domains"))) {
          if (value != null) {
            domainUrns.addAll(extractDomainUrnsFromValue(value));
          }
        }
      }
    } catch (Exception e) {
      log.warn("Error extracting domain URNs from patch document: {}", e.getMessage());
    }

    return domainUrns;
  }

  /**
   * Extract domain URNs from a patch operation value. Handles both single domain (String) and
   * multiple domains (List).
   *
   * @param value The value from a patch operation
   * @return Set of domain URNs found in the value
   */
  @Nonnull
  private static Set<Urn> extractDomainUrnsFromValue(@Nonnull Object value) {
    Set<Urn> domainUrns = new HashSet<>();

    // Handle String (single domain)
    if (value instanceof String) {
      String urnStr = (String) value;
      if (urnStr.startsWith("urn:li:domain:")) {
        try {
          domainUrns.add(Urn.createFromString(urnStr));
        } catch (Exception e) {
          log.warn("Invalid domain URN in patch: {}", urnStr);
        }
      }
    }
    // Handle List (multiple domains)
    else if (value instanceof List) {
      for (Object item : (List<?>) value) {
        if (item instanceof String) {
          String urnStr = (String) item;
          if (urnStr.startsWith("urn:li:domain:")) {
            try {
              domainUrns.add(Urn.createFromString(urnStr));
            } catch (Exception e) {
              log.warn("Invalid domain URN in patch: {}", urnStr);
            }
          }
        }
      }
    }

    return domainUrns;
  }

  /**
   * Extract NEW domain URNs from MetadataChangeProposals that are changing the Domains aspect.
   *
   * <p>This method only returns domains for entities where the Domains aspect is being modified in
   * the provided MCPs. It extracts the NEW/proposed domains from the MCP itself, not from the
   * database.
   *
   * <p>Special entities like dataHubExecutionRequest are excluded from domain-based authorization.
   *
   * @param mcps MetadataChangeProposals to process
   * @return Map of entity URN to set of NEW domain URNs being proposed. Only contains entries for
   *     entities that are changing their domains aspect.
   */
  @Nonnull
  public static Map<Urn, Set<Urn>> extractNewDomainsFromMCPs(
      @Nonnull Collection<MetadataChangeProposal> mcps) {

    Map<Urn, Set<Urn>> newDomainsByEntity = new HashMap<>();

    // Find MCPs that are changing the Domains aspect
    for (MetadataChangeProposal mcp : mcps) {
      Urn entityUrn = mcp.getEntityUrn();

      // Skip if no URN or system entity
      if (entityUrn == null || EXECUTION_REQUEST_ENTITY_NAME.equals(entityUrn.getEntityType())) {
        continue;
      }

      // Only process Domains aspect changes
      if (DOMAINS_ASPECT_NAME.equals(mcp.getAspectName())) {
        Set<Urn> newDomains = extractDomainsFromMCP(mcp);
        if (!newDomains.isEmpty()) {
          newDomainsByEntity.put(entityUrn, newDomains);
        }
      }
    }

    return newDomainsByEntity;
  }

  /**
   * Extract NEW domain URNs from Entity objects (RestLI format) that contain Domains aspects.
   *
   * <p>This method extracts domains from the incoming Entity snapshots. It returns domains for ALL
   * entities that have the Domains aspect in their snapshot, regardless of whether they're changing
   * or not (since we can't tell from the Entity object alone).
   *
   * <p>Special entities like dataHubExecutionRequest are excluded from domain-based authorization.
   *
   * @param entities List of Entity objects with their URNs
   * @return Map of entity URN to set of domain URNs found in the entities. Only contains entries
   *     for entities that have domains in their snapshot.
   */
  @Nonnull
  public static Map<Urn, Set<Urn>> extractNewDomainsFromEntities(
      @Nonnull List<Pair<Urn, Entity>> entities) {

    Map<Urn, Set<Urn>> newDomainsByEntity = new HashMap<>();

    for (Pair<Urn, Entity> pair : entities) {
      Urn entityUrn = pair.getFirst();
      Entity entity = pair.getSecond();

      // Skip system entities
      if (EXECUTION_REQUEST_ENTITY_NAME.equals(entityUrn.getEntityType())) {
        continue;
      }

      // Extract domains from incoming Entity
      Set<Urn> proposedDomains = extractDomainsFromEntity(entity);
      if (!proposedDomains.isEmpty()) {
        newDomainsByEntity.put(entityUrn, proposedDomains);
      }
    }

    return newDomainsByEntity;
  }

  /**
   * Validate that all domain URNs exist in the system.
   *
   * @param opContext Operation context
   * @param entityService Entity service for existence checks
   * @param domainUrns Set of domain URNs to validate
   * @return true if all domains exist, false otherwise
   */
  public static boolean validateDomainsExist(
      @Nonnull OperationContext opContext,
      @Nonnull EntityService<?> entityService,
      @Nonnull Set<Urn> domainUrns) {

    for (Urn domainUrn : domainUrns) {
      if (!entityService.exists(opContext, domainUrn, true)) {
        log.warn("Domain URN does not exist: {}", domainUrn);
        return false;
      }
    }
    return true;
  }

  /**
   * Collect all unique domain URNs from a map of entity domains.
   *
   * @param entityDomains Map of entity URN to set of domain URNs
   * @return Set of all unique domain URNs across all entities
   */
  @Nonnull
  public static Set<Urn> collectAllDomains(@Nonnull Map<Urn, Set<Urn>> entityDomains) {
    return entityDomains.values().stream().flatMap(Set::stream).collect(Collectors.toSet());
  }
}
