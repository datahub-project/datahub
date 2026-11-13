package com.linkedin.metadata.aspect.hooks;

import static com.linkedin.metadata.Constants.DOCUMENT_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SEMANTIC_APPLY_PROPERTY;
import static com.linkedin.metadata.Constants.SEMANTIC_CONTENT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SEMANTIC_TEXT_ASPECT_NAME;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.EmbeddingModelData;
import com.linkedin.common.SemanticContent;
import com.linkedin.common.SemanticRefreshMarker;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.LongMap;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import com.linkedin.metadata.search.elasticsearch.index.entity.SemanticDocumentProvenance;
import com.linkedin.metadata.search.semantic.profile.ContentBinding;
import com.linkedin.metadata.search.semantic.profile.SemanticProfileRefresh;
import com.linkedin.metadata.search.semantic.profile.SemanticProfileRegistry;
import com.linkedin.util.Pair;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Keeps the entity refresh marker monotonic and decides {@code current} on every {@code
 * semanticContent} write. A source edit reaches this hook as the marker patch emitted by {@link
 * SemanticRefreshSideEffect}, so the hook only reads the stored source aspects, never in-batch
 * ones. Apply writes keep the {@code current} they computed.
 */
@Setter
@Getter
@Accessors(chain = true)
public class SemanticRefreshMarkerHook extends MutationHook {

  @Nonnull private AspectPluginConfig config;
  @Nonnull private SemanticProfileRegistry registry;

  @Override
  protected Stream<Pair<ChangeMCP, Boolean>> writeMutation(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {
    return changeMCPs.stream()
        .map(item -> Pair.of(item, mutate(item, retrieverContext, operationContext)));
  }

  private boolean mutate(
      @Nonnull ChangeMCP item,
      @Nonnull RetrieverContext retrieverContext,
      @Nonnull OperationFingerprint operationContext) {
    if (!SEMANTIC_CONTENT_ASPECT_NAME.equals(item.getAspectName())
        || item.getRecordTemplate() == null) {
      return false;
    }
    SemanticContent live = item.getAspect(SemanticContent.class);
    if (live == null || !live.hasEmbeddings()) {
      return false;
    }
    SemanticContent previous = item.getPreviousAspect(SemanticContent.class);
    DataMap beforeMutation = copyData(live.data());
    Set<String> advanced = reconcileMarker(live, previous);
    if (!isApply(item)) {
      markCurrent(item.getUrn(), live, previous, advanced, retrieverContext, operationContext);
    }
    return !beforeMutation.equals(live.data());
  }

  /**
   * Keeps each entity marker source at the later of the stored and written time, so a stale
   * full-record write cannot move the clock back. Returns the watch keys whose time moved forward.
   */
  @Nonnull
  private static Set<String> reconcileMarker(
      @Nonnull SemanticContent live, @Nullable SemanticContent previous) {
    LongMap stored =
        previous != null && previous.hasRefreshMarker()
            ? previous.getRefreshMarker().getSources()
            : new LongMap();
    LongMap merged = new LongMap();
    merged.putAll(stored);
    Set<String> advanced = new HashSet<>();
    if (live.hasRefreshMarker()) {
      live.getRefreshMarker()
          .getSources()
          .forEach(
              (watchKey, time) -> {
                Long before = stored.get(watchKey);
                if (before == null || time > before) {
                  merged.put(watchKey, time);
                  advanced.add(watchKey);
                }
              });
    }
    if (merged.isEmpty()) {
      live.removeRefreshMarker();
    } else {
      live.setRefreshMarker(new SemanticRefreshMarker().setSources(merged));
    }
    return advanced;
  }

  /**
   * Decides {@code current} for each profile whose payload changed or whose bound sources moved. A
   * profile with its own marker is compared to the entity marker. A legacy profile without one is
   * stale after a non-text source moves, hashed against the stored document for document text, and
   * otherwise trusted as written. An external binding is left as written.
   */
  private void markCurrent(
      @Nonnull Urn urn,
      @Nonnull SemanticContent live,
      @Nullable SemanticContent previous,
      @Nonnull Set<String> advanced,
      @Nonnull RetrieverContext retrieverContext,
      @Nonnull OperationFingerprint operationContext) {
    String resolved = null;
    boolean resolvedLoaded = false;
    for (String key : live.getEmbeddings().keySet()) {
      EmbeddingModelData profile = live.getEmbeddings().get(key);
      ContentBinding binding = registry.bindingFor(urn.getEntityType(), key);
      Set<String> watchKeys = SemanticProfileRefresh.watchKeys(binding);
      Set<String> boundAdvanced = new HashSet<>(advanced);
      boundAdvanced.retainAll(watchKeys);
      if (!profileChanged(previous, key, profile) && boundAdvanced.isEmpty()) {
        continue;
      }
      if (profile.hasRefreshMarker()) {
        profile.setCurrent(
            SemanticProfileRefresh.coversBoundSources(profile.getRefreshMarker(), live, watchKeys));
      } else if (binding.isExternal()) {
        continue;
      } else if (movesNonText(boundAdvanced)) {
        // A non-text edit has no hash to compare, so it keeps the profile stale even when the same
        // binding also watches document text.
        profile.setCurrent(false);
      } else {
        if (SemanticProfileRefresh.isDocumentText(binding) && !resolvedLoaded) {
          resolved = resolvedText(urn, retrieverContext, operationContext);
          resolvedLoaded = true;
        }
        SemanticProfileRefresh.markWrittenCurrent(profile, binding, resolved);
      }
    }
  }

  private static boolean movesNonText(@Nonnull Set<String> watchKeys) {
    return watchKeys.stream().anyMatch(key -> !SemanticProfileRefresh.isDocumentTextKey(key));
  }

  @Nonnull
  private static DataMap copyData(@Nonnull DataMap data) {
    try {
      return data.copy();
    } catch (CloneNotSupportedException e) {
      throw new IllegalStateException("Failed to snapshot semanticContent", e);
    }
  }

  private static boolean isApply(@Nonnull ChangeMCP item) {
    return item.getSystemMetadata() != null
        && item.getSystemMetadata().hasProperties()
        && item.getSystemMetadata().getProperties().containsKey(SEMANTIC_APPLY_PROPERTY);
  }

  private static boolean profileChanged(
      @Nullable SemanticContent previous,
      @Nonnull String key,
      @Nonnull EmbeddingModelData profile) {
    if (previous == null
        || !previous.hasEmbeddings()
        || !previous.getEmbeddings().containsKey(key)) {
      return true;
    }
    EmbeddingModelData stored = previous.getEmbeddings().get(key);
    return !Objects.equals(stored.getContentHash(), profile.getContentHash())
        || !Objects.equals(stored.getChunks(), profile.getChunks())
        || !Objects.equals(stored.getSourceTextSha256(), profile.getSourceTextSha256())
        || !Objects.equals(profileMarker(stored), profileMarker(profile));
  }

  @Nullable
  private static SemanticRefreshMarker profileMarker(@Nonnull EmbeddingModelData profile) {
    return profile.hasRefreshMarker() ? profile.getRefreshMarker() : null;
  }

  @Nullable
  private static String resolvedText(
      @Nonnull Urn urn,
      @Nonnull RetrieverContext retrieverContext,
      @Nonnull OperationFingerprint operationContext) {
    String override =
        storedField(urn, SEMANTIC_TEXT_ASPECT_NAME, "text", retrieverContext, operationContext);
    String body =
        storedField(
            urn, DOCUMENT_INFO_ASPECT_NAME, "contents.text", retrieverContext, operationContext);
    return SemanticDocumentProvenance.resolvedText(override, body);
  }

  @Nullable
  private static String storedField(
      @Nonnull Urn urn,
      @Nonnull String aspectName,
      @Nonnull String path,
      @Nonnull RetrieverContext retrieverContext,
      @Nonnull OperationFingerprint operationContext) {
    Aspect aspect =
        retrieverContext
            .getAspectRetriever()
            .getLatestAspectObject(operationContext, urn, aspectName);
    return aspect == null ? null : stringAt(aspect.data(), path);
  }

  @Nullable
  private static String stringAt(@Nullable DataMap map, @Nonnull String path) {
    Object value = map;
    for (String part : path.split("\\.")) {
      if (!(value instanceof DataMap dataMap)) {
        return null;
      }
      value = dataMap.get(part);
    }
    return value == null ? null : String.valueOf(value);
  }
}
