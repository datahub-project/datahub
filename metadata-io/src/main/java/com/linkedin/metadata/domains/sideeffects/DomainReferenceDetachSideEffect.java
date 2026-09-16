package com.linkedin.metadata.domains.sideeffects;

import static com.linkedin.metadata.Constants.DOMAINS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DOMAIN_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DOMAIN_KEY_ASPECT_NAME;

import com.datahub.context.OperationFingerprint;
import com.datahub.util.RecordUtils;
import com.linkedin.common.urn.Urn;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MCPSideEffect;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.utils.elasticsearch.FilterUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * When a Domain is hard-deleted, strip that Domain URN from every entity's {@code domains} aspect.
 *
 * <p>Discovery uses the search index ({@code domains} field), not the graph. {@code
 * graphService.removeNode} on the Domain key DELETE MCL reaps {@code AssociatedWith} edges, so
 * graph-based {@code deleteEntityReferences} races and can leave dangling URNs. Search documents
 * still carry the domain filter until this patch lands.
 *
 * <p>Patches are removal-only and touch {@code domains} only; {@code DomainsSyncMutationHook}
 * re-derives {@code domainAssociations}.
 */
@Slf4j
@Getter
@Setter
@Accessors(chain = true)
public class DomainReferenceDetachSideEffect extends MCPSideEffect {

  public static final int DEFAULT_MAX_FANOUT_PER_COMMIT = 500;
  public static final int MAX_PAGE_ATTEMPTS = 3;
  public static final String DOMAINS_SEARCH_FIELD = "domains";

  private int maxFanoutPerCommit = DEFAULT_MAX_FANOUT_PER_COMMIT;

  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<ChangeMCP> applyMCPSideEffect(
      @Nonnull OperationFingerprint operationContext,
      Collection<ChangeMCP> changeMCPS,
      @Nonnull RetrieverContext retrieverContext) {
    return Stream.of();
  }

  @Override
  protected Stream<MCPItem> postMCPSideEffect(
      @Nonnull OperationFingerprint operationContext,
      Collection<MCLItem> mclItems,
      @Nonnull RetrieverContext retrieverContext) {
    return mclItems.stream()
        .filter(item -> DOMAIN_KEY_ASPECT_NAME.equals(item.getAspectName()))
        .filter(item -> ChangeType.DELETE.equals(item.getChangeType()))
        .filter(item -> DOMAIN_ENTITY_NAME.equals(item.getUrn().getEntityType()))
        .flatMap(item -> detachReferences(operationContext, item, retrieverContext));
  }

  private Stream<MCPItem> detachReferences(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull MCLItem mclItem,
      @Nonnull RetrieverContext retrieverContext) {
    Urn missingDomain = mclItem.getUrn();
    SearchRetriever searchRetriever = retrieverContext.getSearchRetriever();
    if (searchRetriever == null || searchRetriever == SearchRetriever.EMPTY) {
      log.debug(
          "Skipping domain reference detach for {}; search retriever is unavailable",
          missingDomain);
      return Stream.empty();
    }

    List<String> entities = entitiesWithDomainsAspect(retrieverContext);
    if (entities.isEmpty()) {
      return Stream.empty();
    }

    Filter filter =
        FilterUtils.createValuesFilter(DOMAINS_SEARCH_FIELD, List.of(missingDomain.toString()));
    List<MCPItem> patches = new ArrayList<>();
    String scrollId = null;
    do {
      if (patches.size() >= maxFanoutPerCommit) {
        log.warn(
            "Detached {} reference(s) to deleted domain {} in this commit; more may remain",
            patches.size(),
            missingDomain);
        break;
      }
      int pageSize = Math.min(maxFanoutPerCommit, maxFanoutPerCommit - patches.size());
      final String currentScrollId = scrollId;
      ScrollResult scrollResult =
          withBoundedRetry(
              "scroll for domain references to " + missingDomain,
              () ->
                  searchRetriever.scroll(
                      entities,
                      filter,
                      currentScrollId,
                      pageSize,
                      List.of(),
                      SearchRetriever
                          .RETRIEVER_SEARCH_FLAGS_NO_CACHE_ALL_VERSIONS_INCLUDE_SOFT_DELETED));

      if (scrollResult.getEntities() == null || scrollResult.getEntities().isEmpty()) {
        break;
      }

      List<Urn> candidates =
          scrollResult.getEntities().stream()
              .map(SearchEntity::getEntity)
              .filter(Objects::nonNull)
              .collect(Collectors.toList());
      Map<Urn, Map<String, Aspect>> persisted =
          latestDomains(operationContext, candidates, retrieverContext);

      for (Urn entityUrn : candidates) {
        if (!stillReferences(persisted.get(entityUrn), missingDomain)) {
          continue;
        }
        MCPItem patch = removalPatch(entityUrn, missingDomain, mclItem, retrieverContext);
        if (patch != null) {
          patches.add(patch);
        }
      }

      String nextScrollId = scrollResult.getScrollId();
      if (nextScrollId == null || nextScrollId.equals(scrollId)) {
        break;
      }
      scrollId = nextScrollId;
    } while (true);
    return patches.stream();
  }

  @Nonnull
  private static <T> T withBoundedRetry(@Nonnull String what, @Nonnull Supplier<T> action) {
    RuntimeException last = null;
    for (int attempt = 1; attempt <= MAX_PAGE_ATTEMPTS; attempt++) {
      try {
        return action.get();
      } catch (RuntimeException e) {
        last = e;
        log.warn("{} failed (attempt {}/{})", what, attempt, MAX_PAGE_ATTEMPTS, e);
      }
    }
    throw new RuntimeException("Exhausted retries: " + what, last);
  }

  @Nonnull
  private static List<String> entitiesWithDomainsAspect(
      @Nonnull RetrieverContext retrieverContext) {
    return retrieverContext
        .getAspectRetriever()
        .getEntityRegistry()
        .getEntitySpecs()
        .values()
        .stream()
        .filter(spec -> spec.getAspectSpec(DOMAINS_ASPECT_NAME) != null)
        .map(EntitySpec::getName)
        .sorted()
        .collect(Collectors.toList());
  }

  @Nonnull
  private static Map<Urn, Map<String, Aspect>> latestDomains(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull List<Urn> entities,
      @Nonnull RetrieverContext retrieverContext) {
    if (entities.isEmpty()) {
      return Map.of();
    }
    return withBoundedRetry(
        "read persisted domains aspects for " + entities.size() + " entit(y/ies)",
        () -> {
          Map<Urn, Map<String, Aspect>> existing =
              AspectRetriever.getLatestAspectObjectsAcrossEntityTypes(
                  retrieverContext.getAspectRetriever(),
                  operationContext,
                  new HashSet<>(entities),
                  Set.of(DOMAINS_ASPECT_NAME));
          return existing != null ? existing : Map.of();
        });
  }

  private static boolean stillReferences(
      @Nullable Map<String, Aspect> aspects, @Nonnull Urn missingDomain) {
    if (aspects == null) {
      return false;
    }
    Aspect aspect = aspects.get(DOMAINS_ASPECT_NAME);
    if (aspect == null) {
      return false;
    }
    Domains domains = RecordUtils.toRecordTemplate(Domains.class, aspect.data());
    if (domains.getDomains() == null) {
      return false;
    }
    return domains.getDomains().contains(missingDomain);
  }

  @Nullable
  private static MCPItem removalPatch(
      @Nonnull Urn entityUrn,
      @Nonnull Urn missingDomain,
      @Nonnull MCLItem source,
      @Nonnull RetrieverContext retrieverContext) {
    final EntitySpec entitySpec =
        Optional.ofNullable(retrieverContext.getAspectRetriever().getEntityRegistry())
            .map(registry -> registry.getEntitySpec(entityUrn.getEntityType()))
            .orElse(null);
    if (entitySpec == null || entitySpec.getAspectSpec(DOMAINS_ASPECT_NAME) == null) {
      return null;
    }

    String encoded = missingDomain.toString().replace("~", "~0").replace("/", "~1");
    GenericJsonPatch.PatchOp patchOp = new GenericJsonPatch.PatchOp();
    patchOp.setOp(PatchOperationType.REMOVE.getValue());
    patchOp.setPath("/domains/" + encoded);

    return PatchItemImpl.builder()
        .urn(entityUrn)
        .entitySpec(entitySpec)
        .aspectName(DOMAINS_ASPECT_NAME)
        .aspectSpec(entitySpec.getAspectSpec(DOMAINS_ASPECT_NAME))
        .patch(GenericJsonPatch.builder().patch(List.of(patchOp)).build().getJsonPatch())
        .auditStamp(source.getAuditStamp())
        .systemMetadata(source.getSystemMetadata())
        .build(retrieverContext.getAspectRetriever().getEntityRegistry());
  }
}
