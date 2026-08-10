package com.linkedin.metadata.authorization;

import static com.linkedin.metadata.Constants.DOMAINS_ASPECT_NAME;
import static com.linkedin.metadata.authorization.ApiGroup.ENTITY;
import static com.linkedin.metadata.authorization.ApiOperation.CREATE;
import static com.linkedin.metadata.authorization.ApiOperation.UPDATE;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationSession;
import com.datahub.authorization.EntityFieldType;
import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.FieldResolver;
import com.datahub.authorization.ResolvedEntitySpec;
import com.datahub.context.OperationFingerprint;
import com.datahub.util.RecordUtils;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import com.linkedin.metadata.entity.ebean.batch.ProposedItem;
import com.linkedin.metadata.graph.cache.client.BoundHierarchyAccess;
import com.linkedin.metadata.graph.cache.client.HierarchyBindings;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Helpers for domain-separated writers: privilege selection by entity existence, proposed-domain
 * resolution (including PATCH apply), and authorizing writes against domain-scoped Create/Edit
 * policies (including before/after Edit reconciliation for {@code domains} PATCH).
 */
@Slf4j
public final class DomainWriteAuthorizationUtils {

  private DomainWriteAuthorizationUtils() {}

  /**
   * Collect proposed {@code domains} aspects from a batch, keyed by entity URN. Later items for the
   * same URN overwrite earlier ones. Prefer {@link #extractProposedDomainsFromItem} for per-item
   * authorization. PATCH items are skipped here — use {@link #resolveProposedDomainsForItem}.
   */
  @Nonnull
  public static Map<Urn, Domains> extractProposedDomainsByUrn(
      @Nonnull Collection<? extends BatchItem> batchItems) {
    Map<Urn, Domains> proposed = new HashMap<>();
    for (BatchItem item : batchItems) {
      Domains domains = extractProposedDomainsFromItem(item);
      if (domains != null) {
        proposed.put(item.getUrn(), domains);
      }
    }
    return proposed;
  }

  /**
   * Extract proposed domains from a single batch item (UPSERT/CREATE style payloads).
   *
   * <p>Must not call {@link BatchItem#getRecordTemplate()} for {@link ChangeType#PATCH}: under
   * alternate MCP validation, PATCH arrives as a {@code ProposedItem} whose aspect content type is
   * {@code application/json-patch+json}, and deserializing it throws.
   */
  @Nullable
  public static Domains extractProposedDomainsFromItem(@Nonnull BatchItem item) {
    if (!DOMAINS_ASPECT_NAME.equals(item.getAspectName())) {
      return null;
    }
    if (ChangeType.PATCH.equals(item.getChangeType())) {
      return null;
    }
    try {
      Domains domains = item.getAspect(Domains.class);
      if (domains == null && item.getRecordTemplate() != null) {
        domains = RecordUtils.toRecordTemplate(Domains.class, item.getRecordTemplate().data());
      }
      return domains;
    } catch (RuntimeException e) {
      log.warn(
          "Failed to extract proposed domains aspect for urn={}: {}", item.getUrn(), e.toString());
      return null;
    }
  }

  /**
   * Resolve the domains value that authorization should evaluate for this item. For PATCH items
   * without a record template, applies the patch against the current (or empty) domains aspect.
   */
  @Nullable
  public static Domains resolveProposedDomainsForItem(
      @Nonnull BatchItem item,
      @Nonnull AspectRetriever aspectRetriever,
      @Nullable Aspect currentDomainsAspect) {
    if (!DOMAINS_ASPECT_NAME.equals(item.getAspectName())) {
      return null;
    }
    if (ChangeType.PATCH.equals(item.getChangeType()) && item instanceof MCPItem) {
      try {
        Domains current =
            currentDomainsAspect == null
                ? null
                : RecordUtils.toRecordTemplate(Domains.class, currentDomainsAspect.data());
        MCPItem mcpItem = (MCPItem) item;
        PatchItemImpl patchItem =
            item instanceof PatchItemImpl
                ? (PatchItemImpl) item
                : PatchItemImpl.builder()
                    .build(
                        mcpItem.getMetadataChangeProposal(),
                        mcpItem.getAuditStamp(),
                        aspectRetriever.getEntityRegistry());
        return patchItem.applyPatch(current, aspectRetriever).getAspect(Domains.class);
      } catch (Exception e) {
        log.warn(
            "Failed to resolve proposed domains from PATCH for urn={}: {}",
            item.getUrn(),
            e.toString());
        return null;
      }
    }
    return extractProposedDomainsFromItem(item);
  }

  @Nonnull
  public static Map<Urn, Boolean> resolveEntityExists(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull AspectRetriever aspectRetriever,
      @Nonnull Collection<Urn> urns) {
    if (urns.isEmpty()) {
      return Map.of();
    }
    return aspectRetriever.entityExists(operationContext, Set.copyOf(urns));
  }

  /**
   * Whether each URN already has a persisted {@code domains} aspect. URNs are fetched per entity
   * type because {@code EntityServiceAspectRetriever.getLatestAspectObjects} scopes the query to
   * the first URN's entity type.
   */
  @Nonnull
  public static Map<Urn, Boolean> resolveDomainsAspectExists(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull AspectRetriever aspectRetriever,
      @Nonnull Collection<Urn> urns) {
    if (urns.isEmpty()) {
      return Map.of();
    }
    Map<String, Set<Urn>> byEntityType =
        urns.stream()
            .collect(
                Collectors.groupingBy(Urn::getEntityType, Collectors.toCollection(HashSet::new)));
    Map<Urn, Boolean> result = new HashMap<>();
    for (Set<Urn> typeUrns : byEntityType.values()) {
      Map<Urn, Map<String, Aspect>> latest =
          aspectRetriever.getLatestAspectObjects(
              operationContext, typeUrns, Set.of(DOMAINS_ASPECT_NAME));
      for (Urn urn : typeUrns) {
        result.put(urn, latest.getOrDefault(urn, Map.of()).containsKey(DOMAINS_ASPECT_NAME));
      }
    }
    return result;
  }

  /**
   * Whether this write should match domain-scoped policies using <em>proposed</em> domains (entity
   * create with domains in the batch, or first write establishing a missing domains aspect).
   */
  public static boolean shouldUseProposedDomainsForMatch(
      boolean entityExists, boolean domainsAspectExists, boolean batchProposesDomainsForUrn) {
    if (!batchProposesDomainsForUrn) {
      return false;
    }
    if (!entityExists) {
      return true;
    }
    return !domainsAspectExists;
  }

  /** Privilege path for an entity write given change type and existence. */
  @Nonnull
  public static com.linkedin.metadata.authorization.ApiOperation resolveApiOperation(
      @Nonnull ChangeType changeType, boolean entityExists) {
    switch (changeType) {
      case CREATE_ENTITY:
        return entityExists ? UPDATE : CREATE;
      case CREATE:
        return UPDATE;
      case PATCH:
        // PATCH never uses CREATE_ENTITY — always Edit Entity.
        return UPDATE;
      case UPSERT:
      case UPDATE:
      case RESTATE:
        return entityExists ? UPDATE : CREATE;
      case DELETE:
        return com.linkedin.metadata.authorization.ApiOperation.DELETE;
      default:
        return UPDATE;
    }
  }

  /** Whether {@code domains} carries at least one domain URN. */
  public static boolean hasDomainMembership(@Nullable Domains domains) {
    return domains != null && domains.hasDomains() && !domains.getDomains().isEmpty();
  }

  /**
   * Authorize domain-scoped {@code EDIT_ENTITY} for a domains write given before/after membership.
   *
   * <ul>
   *   <li>No before domains: match against after only (first-domains pattern).
   *   <li>Before domains present: policy must allow both before and after.
   * </ul>
   */
  public static boolean isAuthorizedDomainsEdit(
      @Nonnull AuthorizationSession session,
      @Nonnull Urn urn,
      @Nullable Domains beforeDomains,
      @Nullable Domains afterDomains) {
    if (hasDomainMembership(beforeDomains)) {
      if (afterDomains == null) {
        return false;
      }
      if (!isAuthorizedEntityWrite(session, urn, UPDATE, true, beforeDomains)) {
        return false;
      }
      return isAuthorizedEntityWrite(session, urn, UPDATE, true, afterDomains);
    }
    return isAuthorizedEntityWrite(session, urn, UPDATE, true, afterDomains);
  }

  /**
   * Resolves proposed Domains for one batch item using any earlier in-batch Domains for the same
   * URN as prior (falling back to {@code persistedFallback}), then stores the result in {@code
   * proposedSoFar} when non-null.
   *
   * <p>Callers must pass items in batch order so UPSERT→PATCH (and similar) chains correctly.
   */
  @Nullable
  public static Domains resolveAndAccumulateProposedDomains(
      @Nonnull BatchItem item,
      @Nonnull AspectRetriever aspectRetriever,
      @Nonnull Map<Urn, Domains> proposedSoFar) {
    return resolveAndAccumulateProposedDomains(item, aspectRetriever, proposedSoFar, null);
  }

  @Nullable
  public static Domains resolveAndAccumulateProposedDomains(
      @Nonnull BatchItem item,
      @Nonnull AspectRetriever aspectRetriever,
      @Nonnull Map<Urn, Domains> proposedSoFar,
      @Nullable Domains persistedFallback) {
    if (!DOMAINS_ASPECT_NAME.equals(item.getAspectName())) {
      return null;
    }
    Domains prior = proposedSoFar.get(item.getUrn());
    if (prior == null) {
      prior = persistedFallback;
    }
    Aspect priorAspect = prior == null ? null : new Aspect(prior.data());
    Domains domains = resolveProposedDomainsForItem(item, aspectRetriever, priorAspect);
    if (domains != null) {
      proposedSoFar.put(item.getUrn(), domains);
    }
    return domains;
  }

  /**
   * Load persisted {@code domains} for the given URNs (batch per entity type). Missing aspects map
   * to null.
   */
  @Nonnull
  public static Map<Urn, Domains> loadPersistedDomains(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull AspectRetriever aspectRetriever,
      @Nonnull Collection<Urn> urns) {
    if (urns.isEmpty()) {
      return Map.of();
    }
    Map<String, Set<Urn>> byEntityType =
        urns.stream()
            .collect(
                Collectors.groupingBy(Urn::getEntityType, Collectors.toCollection(HashSet::new)));
    Map<Urn, Domains> result = new HashMap<>();
    for (Set<Urn> typeUrns : byEntityType.values()) {
      Map<Urn, Map<String, Aspect>> latest =
          aspectRetriever.getLatestAspectObjects(
              operationContext, typeUrns, Set.of(DOMAINS_ASPECT_NAME));
      for (Urn urn : typeUrns) {
        Aspect aspect = latest.getOrDefault(urn, Map.of()).get(DOMAINS_ASPECT_NAME);
        result.put(
            urn,
            aspect == null ? null : RecordUtils.toRecordTemplate(Domains.class, aspect.data()));
      }
    }
    return result;
  }

  /**
   * Walks {@code batchItems} in list order and accumulates resolved Domains per URN (in-batch prior
   * chaining). Used by API auth seed and unit tests.
   */
  @Nonnull
  public static Map<Urn, Domains> accumulateProposedDomainsFromBatchItems(
      @Nonnull AspectRetriever aspectRetriever, @Nonnull List<? extends BatchItem> batchItems) {
    Map<Urn, Domains> proposed = new HashMap<>();
    for (BatchItem item : batchItems) {
      resolveAndAccumulateProposedDomains(item, aspectRetriever, proposed);
    }
    return proposed;
  }

  /**
   * Seed proposed domains from an MCP batch into the session resource-spec cache so domain-scoped
   * CREATE_ENTITY / EDIT policies can match at the API auth layer before ingest.
   *
   * <p>Accumulates in-batch proposed domains chronologically so a later PATCH for the same URN is
   * applied against an earlier UPSERT in the same request (matching ingest order). {@code
   * batchItems} must be a {@link List} because chaining depends on that order; a bare {@link
   * Collection} is not enough.
   */
  public static void seedProposedDomainsForApiAuth(
      @Nonnull OperationContext opContext,
      @Nonnull OperationFingerprint operationContext,
      @Nonnull AspectRetriever aspectRetriever,
      @Nonnull List<? extends BatchItem> batchItems) {
    if (batchItems.isEmpty()) {
      return;
    }
    seedProposedDomainsForApiAuth(
        opContext,
        operationContext,
        aspectRetriever,
        accumulateProposedDomainsFromBatchItems(aspectRetriever, batchItems));
  }

  /** Seed proposed domains extracted from raw MCPs (RestLi / Platform OpenAPI ingest path). */
  public static void seedProposedDomainsFromMcps(
      @Nonnull OperationContext opContext,
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull Collection<MetadataChangeProposal> mcps) {
    Map<Urn, Domains> proposed =
        accumulateProposedDomainsFromMcps(
            opContext, entityRegistry, opContext.getAspectRetriever(), mcps);
    if (proposed.isEmpty()) {
      return;
    }
    seedProposedDomainsForApiAuth(opContext, opContext, opContext.getAspectRetriever(), proposed);
  }

  private static void seedProposedDomainsForApiAuth(
      @Nonnull OperationContext opContext,
      @Nonnull OperationFingerprint operationContext,
      @Nonnull AspectRetriever aspectRetriever,
      @Nonnull Map<Urn, Domains> proposedByUrn) {
    if (proposedByUrn.isEmpty()) {
      return;
    }
    Map<Urn, Boolean> entityExists =
        resolveEntityExists(operationContext, aspectRetriever, proposedByUrn.keySet());
    Map<Urn, Boolean> domainsExists =
        resolveDomainsAspectExists(operationContext, aspectRetriever, proposedByUrn.keySet());

    for (Map.Entry<Urn, Domains> entry : proposedByUrn.entrySet()) {
      Urn urn = entry.getKey();
      boolean exists = Boolean.TRUE.equals(entityExists.get(urn));
      boolean domainsAspectExists = Boolean.TRUE.equals(domainsExists.get(urn));
      if (!shouldUseProposedDomainsForMatch(exists, domainsAspectExists, true)) {
        continue;
      }
      EntitySpec resourceSpec = new EntitySpec(urn.getEntityType(), urn.toString());
      seedProposedDomainsForResourceSpec(opContext, resourceSpec, entry.getValue());
    }
  }

  /**
   * Resolve proposed domains from raw MCPs, including PATCH (via {@link PatchItemImpl}), with
   * in-batch prior chaining and persisted domains as fallback.
   */
  @Nonnull
  public static Map<Urn, Domains> accumulateProposedDomainsFromMcps(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull AspectRetriever aspectRetriever,
      @Nonnull Collection<MetadataChangeProposal> mcps) {
    List<MetadataChangeProposal> domainsMcps =
        mcps.stream()
            .filter(
                mcp ->
                    mcp.getAspectName() != null
                        && DOMAINS_ASPECT_NAME.equals(mcp.getAspectName())
                        && mcp.getAspect() != null)
            .collect(Collectors.toList());
    if (domainsMcps.isEmpty()) {
      return Map.of();
    }

    Set<Urn> urns = new HashSet<>();
    List<BatchItem> items = new ArrayList<>();
    AuditStamp auditStamp =
        new AuditStamp().setActor(UrnUtils.getUrn("urn:li:corpuser:datahub")).setTime(0L);
    for (MetadataChangeProposal mcp : domainsMcps) {
      try {
        Urn urn = mcp.getEntityUrn();
        if (urn == null) {
          urn =
              EntityKeyUtils.getUrnFromProposal(
                  mcp, entityRegistry.getEntitySpec(mcp.getEntityType()).getKeyAspectSpec());
        }
        urns.add(urn);
        items.add(ProposedItem.builder().build(mcp, auditStamp, entityRegistry));
      } catch (Exception e) {
        log.warn(
            "Failed to build batch item for domains MCP (entityType={}): {}",
            mcp.getEntityType(),
            e.toString());
      }
    }

    Map<Urn, Domains> persisted = loadPersistedDomains(operationContext, aspectRetriever, urns);
    Map<Urn, Domains> proposed = new HashMap<>();
    for (BatchItem item : items) {
      resolveAndAccumulateProposedDomains(
          item, aspectRetriever, proposed, persisted.get(item.getUrn()));
    }
    return proposed;
  }

  /**
   * Authorize a write against the entity resource, optionally seeding the session resource-spec
   * cache with proposed domains (plus ancestors) so domain-scoped policies can match.
   */
  public static boolean isAuthorizedEntityWrite(
      @Nonnull AuthorizationSession session,
      @Nonnull Urn urn,
      @Nonnull com.linkedin.metadata.authorization.ApiOperation apiOperation,
      boolean useProposedDomains,
      @Nullable Domains proposedDomains) {

    EntitySpec resourceSpec = new EntitySpec(urn.getEntityType(), urn.toString());

    if (useProposedDomains && session instanceof OperationContext) {
      seedProposedDomainsForResourceSpec((OperationContext) session, resourceSpec, proposedDomains);
    }

    return AuthUtil.isAuthorized(
        session,
        AuthUtil.buildDisjunctivePrivilegeGroup(
            AuthUtil.lookupAPIPrivilege(ENTITY, apiOperation, urn.getEntityType())),
        resourceSpec);
  }

  /**
   * Expand proposed domain URNs (including ancestors) and seed the request-scoped resource-spec
   * cache so DOMAIN field matching uses those values.
   */
  public static void seedProposedDomainsForResourceSpec(
      @Nonnull OperationContext opContext,
      @Nonnull EntitySpec resourceSpec,
      @Nullable Domains proposedDomains) {
    Set<Urn> domainUrns = EntityAspectAuthorizationUtils.resolveUniqueDomainUrns(proposedDomains);
    Set<String> domainValues = expandDomainFieldValues(opContext, domainUrns);
    seedProposedDomainResourceSpec(opContext, resourceSpec, domainValues);
  }

  @Nonnull
  public static Set<String> expandDomainFieldValues(
      @Nonnull OperationContext opContext, @Nonnull Collection<Urn> domainUrns) {
    if (domainUrns.isEmpty()) {
      return Set.of();
    }
    Set<Urn> withAncestors =
        BoundHierarchyAccess.expandAncestors(
            opContext, HierarchyBindings.domainSpec(opContext), new HashSet<>(domainUrns));
    return withAncestors.stream().map(Urn::toString).collect(Collectors.toSet());
  }

  /**
   * Pre-populate the request-scoped resolved-spec cache so DOMAIN field matching uses proposed
   * values instead of a persisted (empty) domains aspect. Merges into any existing
   * ResolvedEntitySpec so OWNER/TAG/CONTAINER resolvers are preserved, while DOMAIN is always
   * overwritten with the latest proposed values (later items in the same request win).
   */
  public static void seedProposedDomainResourceSpec(
      @Nonnull OperationContext opContext,
      @Nonnull EntitySpec resourceSpec,
      @Nonnull Set<String> domainFieldValues) {
    Map<EntityFieldType, FieldResolver> baseResolvers = new EnumMap<>(EntityFieldType.class);
    baseResolvers.put(
        EntityFieldType.TYPE, FieldResolver.getResolverFromValues(Set.of(resourceSpec.getType())));
    baseResolvers.put(
        EntityFieldType.RESOURCE_TYPE,
        FieldResolver.getResolverFromValues(Set.of(resourceSpec.getType())));
    baseResolvers.put(
        EntityFieldType.URN, FieldResolver.getResolverFromValues(Set.of(resourceSpec.getEntity())));
    baseResolvers.put(
        EntityFieldType.RESOURCE_URN,
        FieldResolver.getResolverFromValues(Set.of(resourceSpec.getEntity())));
    FieldResolver domainResolver = FieldResolver.getResolverFromValues(domainFieldValues);

    opContext
        .getAuthorizationContext()
        .getSessionResourceSpecCache()
        .compute(
            resourceSpec,
            (key, existing) -> {
              Map<EntityFieldType, FieldResolver> merged = new EnumMap<>(EntityFieldType.class);
              if (existing != null) {
                merged.putAll(existing.getFieldResolvers());
              }
              baseResolvers.forEach(merged::putIfAbsent);
              merged.put(EntityFieldType.DOMAIN, domainResolver);
              return new ResolvedEntitySpec(resourceSpec, merged);
            });
  }
}
