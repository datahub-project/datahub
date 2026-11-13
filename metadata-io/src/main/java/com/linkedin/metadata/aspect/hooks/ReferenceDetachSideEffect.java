package com.linkedin.metadata.aspect.hooks;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
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
 * When an entity is hard-deleted, strip its URN from the aspect other entities reference it with.
 *
 * <p>Discovery uses the search index, not the graph. {@code graphService.removeNode} on the key
 * DELETE MCL reaps the incoming edges, so graph-based {@code deleteEntityReferences} races and can
 * leave dangling URNs. Search documents still carry the reference until this patch lands.
 */
@Slf4j
@Getter
@Setter
@Accessors(chain = true)
public abstract class ReferenceDetachSideEffect extends MCPSideEffect {

  public static final int DEFAULT_MAX_FANOUT_PER_COMMIT = 500;
  public static final int MAX_PAGE_ATTEMPTS = 3;

  private int maxFanoutPerCommit = DEFAULT_MAX_FANOUT_PER_COMMIT;

  @Nonnull private AspectPluginConfig config;

  @Nonnull
  protected abstract String deletedEntityName();

  @Nonnull
  protected abstract String deletedKeyAspectName();

  /** The aspect on referencing entities that holds the reference. */
  @Nonnull
  protected abstract String referencingAspectName();

  /** The search field, indexed from {@link #referencingAspectName()}, that holds the reference. */
  @Nonnull
  protected abstract String searchFieldName();

  protected abstract boolean references(@Nonnull Aspect aspect, @Nonnull Urn deletedUrn);

  @Nonnull
  protected abstract GenericJsonPatch removalPatch(@Nonnull Urn deletedUrn);

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
        .filter(item -> deletedKeyAspectName().equals(item.getAspectName()))
        .filter(item -> ChangeType.DELETE.equals(item.getChangeType()))
        .filter(item -> deletedEntityName().equals(item.getUrn().getEntityType()))
        .flatMap(item -> detachReferences(operationContext, item, retrieverContext));
  }

  private Stream<MCPItem> detachReferences(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull MCLItem mclItem,
      @Nonnull RetrieverContext retrieverContext) {
    Urn deletedUrn = mclItem.getUrn();
    SearchRetriever searchRetriever = retrieverContext.getSearchRetriever();
    if (searchRetriever == null || searchRetriever == SearchRetriever.EMPTY) {
      log.debug(
          "Skipping {} reference detach for {}; search retriever is unavailable",
          deletedEntityName(),
          deletedUrn);
      return Stream.empty();
    }

    List<String> entities = entitiesWithReferencingAspect(retrieverContext);
    if (entities.isEmpty()) {
      return Stream.empty();
    }

    Filter filter =
        FilterUtils.createValuesFilter(searchFieldName(), List.of(deletedUrn.toString()));
    List<MCPItem> patches = new ArrayList<>();
    String scrollId = null;
    do {
      if (patches.size() >= maxFanoutPerCommit) {
        log.warn(
            "Detached {} reference(s) to deleted {} {} in this commit; more may remain",
            patches.size(),
            deletedEntityName(),
            deletedUrn);
        break;
      }
      int pageSize = Math.min(maxFanoutPerCommit, maxFanoutPerCommit - patches.size());
      final String currentScrollId = scrollId;
      Optional<ScrollResult> scrollResultOpt =
          withBoundedRetry(
              "scroll for " + deletedEntityName() + " references to " + deletedUrn,
              () ->
                  searchRetriever.scroll(
                      entities,
                      filter,
                      currentScrollId,
                      pageSize,
                      List.of(),
                      SearchRetriever
                          .RETRIEVER_SEARCH_FLAGS_NO_CACHE_ALL_VERSIONS_INCLUDE_SOFT_DELETED));
      if (scrollResultOpt.isEmpty()) {
        // The key delete already committed; do not fail it. Return patches so far.
        log.error(
            "Exhausted retries scrolling for {} references to {};"
                + " returning {} patch(es) collected so far; more may remain",
            deletedEntityName(),
            deletedUrn,
            patches.size());
        break;
      }
      ScrollResult scrollResult = scrollResultOpt.get();

      if (scrollResult.getEntities() == null || scrollResult.getEntities().isEmpty()) {
        break;
      }

      List<Urn> candidates =
          scrollResult.getEntities().stream()
              .map(SearchEntity::getEntity)
              .filter(Objects::nonNull)
              .collect(Collectors.toList());
      Map<Urn, Map<String, Aspect>> persisted =
          latestReferencingAspects(operationContext, candidates, retrieverContext);

      for (Urn entityUrn : candidates) {
        if (!stillReferences(persisted.get(entityUrn), deletedUrn)) {
          continue;
        }
        MCPItem patch = buildPatchItem(entityUrn, deletedUrn, mclItem, retrieverContext);
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

  /**
   * Retries a transient failure a bounded number of times. Returns empty on exhaustion so callers
   * can fail soft — this side effect runs after the key delete is already committed.
   */
  @Nonnull
  private static <T> Optional<T> withBoundedRetry(
      @Nonnull String what, @Nonnull Supplier<T> action) {
    RuntimeException last = null;
    for (int attempt = 1; attempt <= MAX_PAGE_ATTEMPTS; attempt++) {
      try {
        return Optional.ofNullable(action.get());
      } catch (RuntimeException e) {
        last = e;
        log.warn("{} failed (attempt {}/{})", what, attempt, MAX_PAGE_ATTEMPTS, e);
      }
    }
    log.error("Exhausted retries: {}", what, last);
    return Optional.empty();
  }

  @Nonnull
  private List<String> entitiesWithReferencingAspect(@Nonnull RetrieverContext retrieverContext) {
    return retrieverContext
        .getAspectRetriever()
        .getEntityRegistry()
        .getEntitySpecs()
        .values()
        .stream()
        .filter(spec -> spec.getAspectSpec(referencingAspectName()) != null)
        .map(EntitySpec::getName)
        .sorted()
        .collect(Collectors.toList());
  }

  @Nonnull
  private Map<Urn, Map<String, Aspect>> latestReferencingAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull List<Urn> entities,
      @Nonnull RetrieverContext retrieverContext) {
    if (entities.isEmpty()) {
      return Map.of();
    }
    return withBoundedRetry(
            "read persisted "
                + referencingAspectName()
                + " aspects for "
                + entities.size()
                + " entit(y/ies)",
            () -> {
              Map<Urn, Map<String, Aspect>> existing =
                  AspectRetriever.getLatestAspectObjectsAcrossEntityTypes(
                      retrieverContext.getAspectRetriever(),
                      operationContext,
                      new HashSet<>(entities),
                      Set.of(referencingAspectName()));
              return existing != null ? existing : Map.<Urn, Map<String, Aspect>>of();
            })
        // Fail soft: the key delete already committed; skip this page rather than abort it.
        .orElseGet(Map::of);
  }

  private boolean stillReferences(@Nullable Map<String, Aspect> aspects, @Nonnull Urn deletedUrn) {
    if (aspects == null) {
      return false;
    }
    Aspect aspect = aspects.get(referencingAspectName());
    return aspect != null && references(aspect, deletedUrn);
  }

  @Nullable
  private MCPItem buildPatchItem(
      @Nonnull Urn entityUrn,
      @Nonnull Urn deletedUrn,
      @Nonnull MCLItem source,
      @Nonnull RetrieverContext retrieverContext) {
    final EntitySpec entitySpec =
        Optional.ofNullable(retrieverContext.getAspectRetriever().getEntityRegistry())
            .flatMap(registry -> registry.findEntitySpec(entityUrn.getEntityType()))
            .orElse(null);
    if (entitySpec == null || entitySpec.getAspectSpec(referencingAspectName()) == null) {
      return null;
    }

    return PatchItemImpl.builder()
        .urn(entityUrn)
        .entitySpec(entitySpec)
        .aspectName(referencingAspectName())
        .aspectSpec(entitySpec.getAspectSpec(referencingAspectName()))
        .patch(removalPatch(deletedUrn))
        .auditStamp(source.getAuditStamp())
        .systemMetadata(source.getSystemMetadata())
        .build(retrieverContext.getAspectRetriever().getEntityRegistry());
  }

  /** Escapes {@code urn} for use as one JSON Pointer path segment. */
  @Nonnull
  protected static String pathSegment(@Nonnull Urn urn) {
    return urn.toString().replace("~", "~0").replace("/", "~1");
  }
}
