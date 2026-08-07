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
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import com.linkedin.metadata.graph.cache.client.BoundHierarchyAccess;
import com.linkedin.metadata.graph.cache.client.HierarchyBindings;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
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
 * Helpers for domain-separated writers: privilege selection by entity existence, and authorizing
 * writes while matching domain-scoped policies against <em>proposed</em> domains when the domains
 * aspect is being established.
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
      case UPSERT:
      case UPDATE:
      case RESTATE:
      case PATCH:
        return entityExists ? UPDATE : CREATE;
      case DELETE:
        return com.linkedin.metadata.authorization.ApiOperation.DELETE;
      default:
        return UPDATE;
    }
  }

  /**
   * Resolves proposed Domains for one batch item using any earlier in-batch Domains for the same
   * URN as prior, then stores the result in {@code proposedSoFar} when non-null.
   *
   * <p>Callers must pass items in batch order so UPSERT→PATCH (and similar) chains correctly.
   */
  @Nullable
  public static Domains resolveAndAccumulateProposedDomains(
      @Nonnull BatchItem item,
      @Nonnull AspectRetriever aspectRetriever,
      @Nonnull Map<Urn, Domains> proposedSoFar) {
    if (!DOMAINS_ASPECT_NAME.equals(item.getAspectName())) {
      return null;
    }
    Domains prior = proposedSoFar.get(item.getUrn());
    Aspect priorAspect = prior == null ? null : new Aspect(prior.data());
    Domains domains = resolveProposedDomainsForItem(item, aspectRetriever, priorAspect);
    if (domains != null) {
      proposedSoFar.put(item.getUrn(), domains);
    }
    return domains;
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
    Map<Urn, Domains> proposed = extractProposedDomainsFromMcps(entityRegistry, mcps);
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

  @Nonnull
  public static Map<Urn, Domains> extractProposedDomainsFromMcps(
      @Nonnull EntityRegistry entityRegistry, @Nonnull Collection<MetadataChangeProposal> mcps) {
    Map<Urn, Domains> proposed = new HashMap<>();
    for (MetadataChangeProposal mcp : mcps) {
      if (mcp.getAspectName() == null
          || !DOMAINS_ASPECT_NAME.equals(mcp.getAspectName())
          || mcp.getAspect() == null) {
        continue;
      }
      Urn urn = mcp.getEntityUrn();
      if (urn == null) {
        try {
          urn =
              EntityKeyUtils.getUrnFromProposal(
                  mcp, entityRegistry.getEntitySpec(mcp.getEntityType()).getKeyAspectSpec());
        } catch (Exception e) {
          log.warn(
              "Failed to derive entity URN from domains MCP (entityType={}): {}",
              mcp.getEntityType(),
              e.toString());
          continue;
        }
      }
      try {
        if (ChangeType.PATCH.equals(mcp.getChangeType())
            || GenericRecordUtils.JSON_PATCH.equals(mcp.getAspect().getContentType())) {
          // PATCH payloads are not Domains records; resolved at auth time via PatchItemImpl.
          continue;
        }
        AspectSpec aspectSpec =
            entityRegistry.getEntitySpec(mcp.getEntityType()).getAspectSpec(DOMAINS_ASPECT_NAME);
        if (aspectSpec == null) {
          continue;
        }
        RecordTemplate template =
            GenericRecordUtils.deserializeAspect(
                mcp.getAspect().getValue(), mcp.getAspect().getContentType(), aspectSpec);
        proposed.put(urn, RecordUtils.toRecordTemplate(Domains.class, template.data()));
      } catch (Exception e) {
        log.warn("Failed to deserialize proposed domains aspect for urn={}: {}", urn, e.toString());
      }
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
