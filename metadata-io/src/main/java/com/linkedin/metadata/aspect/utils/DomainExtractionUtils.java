package com.linkedin.metadata.aspect.utils;

import static com.linkedin.metadata.Constants.DOMAINS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_ENTITY_NAME;

import com.datahub.util.RecordUtils;
import com.linkedin.common.urn.Urn;
import com.linkedin.domain.Domains;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility class for extracting domain information from entities and MetadataChangeProposals.
 * Used by API resources (OpenAPI, RestLI, GraphQL) to support domain-based authorization.
 */
@Slf4j
public class DomainExtractionUtils {

  private DomainExtractionUtils() {
    // Utility class - prevent instantiation
  }

  /**
   * Extract domains from a MetadataChangeProposal's Domains aspect.
   * Handles both regular UPSERT/CREATE operations and PATCH operations.
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
      log.warn("Error parsing domains from MCP for entity {}: {}",
          mcp.getEntityUrn(), e.getMessage());
    }
    return Collections.emptySet();
  }

  /**
   * Extract domain URNs from a JSON Patch document.
   * Parses patch operations to find domain URNs being added or replaced.
   *
   * @param patchDocument The JSON Patch document as a string
   * @return Set of domain URNs found in the patch, or empty set if none found
   */
  @Nonnull
  private static Set<Urn> extractDomainsFromPatchDocument(@Nonnull String patchDocument) {
    Set<Urn> domainUrns = new HashSet<>();

    try {
      // Parse the patch document to extract domain URNs
      com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
      com.fasterxml.jackson.databind.JsonNode patchNode = mapper.readTree(patchDocument);

      if (patchNode.has("patch") && patchNode.get("patch").isArray()) {
        for (com.fasterxml.jackson.databind.JsonNode operation : patchNode.get("patch")) {
          String path = operation.has("path") ? operation.get("path").asText() : null;
          com.fasterxml.jackson.databind.JsonNode value = operation.has("value") ? operation.get("value") : null;

          // Check if this operation is modifying domains
          if (path != null && (path.startsWith("/domains") || path.equals("/domains"))) {
            if (value != null) {
              // Handle different value types (String for single domain, Array for multiple domains)
              if (value.isTextual()) {
                String urnStr = value.asText();
                if (urnStr.startsWith("urn:li:domain:")) {
                  try {
                    domainUrns.add(Urn.createFromString(urnStr));
                  } catch (Exception e) {
                    log.warn("Invalid domain URN in patch: {}", urnStr);
                  }
                }
              } else if (value.isArray()) {
                for (com.fasterxml.jackson.databind.JsonNode item : value) {
                  if (item.isTextual()) {
                    String urnStr = item.asText();
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
            }
          }
        }
      }
    } catch (Exception e) {
      log.warn("Error extracting domain URNs from patch document: {}", e.getMessage());
    }

    return domainUrns;
  }

  /**
   * Get the domain URNs for an entity from its existing Domains aspect.
   *
   * @param opContext Operation context
   * @param entityService Entity service for domain lookups
   * @param entityUrn The entity URN to get domains for
   * @return Set of domain URNs for the entity, or empty set if none found
   */
  @Nonnull
  public static Set<Urn> getEntityDomains(
      @Nonnull OperationContext opContext,
      @Nonnull EntityService<?> entityService,
      @Nonnull Urn entityUrn) {
    try {
      if (!entityService.exists(opContext, entityUrn, true)) {
        return Collections.emptySet();
      }

      EnvelopedAspect envelopedAspect = entityService.getLatestEnvelopedAspect(
          opContext, entityUrn.getEntityType(), entityUrn, DOMAINS_ASPECT_NAME);

      if (envelopedAspect != null) {
        Domains domains = RecordUtils.toRecordTemplate(
            Domains.class, envelopedAspect.getValue().data());
        if (domains.getDomains() != null && !domains.getDomains().isEmpty()) {
          return new HashSet<>(domains.getDomains());
        }
      }
    } catch (Exception e) {
      log.warn("Error retrieving domains for entity {}: {}", entityUrn, e.getMessage());
    }
    return Collections.emptySet();
  }

  /**
   * Extract domain URNs from a collection of MetadataChangeProposals for authorization.
   * This combines:
   * 1. Existing domains from entities already in the system
   * 2. New domains being set in the current MCPs (from Domains aspect)
   *
   * Special entities like dataHubExecutionRequest are excluded from domain-based authorization
   * as they are system entities that should not be subject to domain restrictions.
   *
   * This is the central method for domain extraction used by REST resources
   * when domain-based authorization is enabled.
   *
   * @param opContext Operation context
   * @param entityService Entity service for domain lookups
   * @param mcps MetadataChangeProposals to process
   * @return Map of entity URN to set of domain URNs (only entities with domains are included)
   */
  @Nonnull
  public static Map<Urn, Set<Urn>> extractEntityDomainsForAuthorization(
      @Nonnull OperationContext opContext,
      @Nonnull EntityService<?> entityService,
      @Nonnull Collection<MetadataChangeProposal> mcps) {

    Map<Urn, Set<Urn>> entityDomains = new HashMap<>();

    // Collect unique entity URNs from MCPs, excluding special system entities
    // that should not be subject to domain-based authorization
    Set<Urn> entityUrns = mcps.stream()
        .map(MetadataChangeProposal::getEntityUrn)
        .filter(Objects::nonNull)
        .filter(urn -> !EXECUTION_REQUEST_ENTITY_NAME.equals(urn.getEntityType()))
        .collect(Collectors.toSet());

    // Get existing domains from entities already in the system
    for (Urn entityUrn : entityUrns) {
      Set<Urn> domains = getEntityDomains(opContext, entityService, entityUrn);
      if (!domains.isEmpty()) {
        entityDomains.put(entityUrn, new HashSet<>(domains));
      }
    }

    // Extract domains from MCPs with Domains aspect (new domains being set)
    for (MetadataChangeProposal mcp : mcps) {
      if (mcp.getEntityUrn() != null &&
          DOMAINS_ASPECT_NAME.equals(mcp.getAspectName()) &&
          mcp.getAspect() != null) {

        Set<Urn> mcpDomains = extractDomainsFromMCP(mcp);
        if (!mcpDomains.isEmpty()) {
          entityDomains.computeIfAbsent(mcp.getEntityUrn(), k -> new HashSet<>())
              .addAll(mcpDomains);
        }
      }
    }

    return entityDomains;
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
    return entityDomains.values().stream()
        .flatMap(Set::stream)
        .collect(Collectors.toSet());
  }
}