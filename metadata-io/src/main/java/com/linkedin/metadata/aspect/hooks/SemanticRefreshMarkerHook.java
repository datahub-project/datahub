package com.linkedin.metadata.aspect.hooks;

import static com.linkedin.metadata.Constants.SEMANTIC_APPLY_PROPERTY;
import static com.linkedin.metadata.Constants.SEMANTIC_CONTENT_ASPECT_NAME;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.EmbeddingModelData;
import com.linkedin.common.SemanticContent;
import com.linkedin.common.SemanticRefreshMarker;
import com.linkedin.common.SemanticSourceToken;
import com.linkedin.common.SemanticSourceTokenMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
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
 * SemanticRefreshSideEffect}; the hook reads no source aspects. Apply writes keep the {@code
 * current} they computed.
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
    return changeMCPs.stream().map(item -> Pair.of(item, mutate(item)));
  }

  private boolean mutate(@Nonnull ChangeMCP item) {
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
      markCurrent(item.getUrn(), live, previous, advanced);
    }
    return !beforeMutation.equals(live.data());
  }

  /**
   * Keeps each entity marker source at the later of the stored and written token, so a stale
   * full-record write cannot move the clock back. Returns the watch keys whose token moved forward.
   */
  @Nonnull
  private static Set<String> reconcileMarker(
      @Nonnull SemanticContent live, @Nullable SemanticContent previous) {
    SemanticSourceTokenMap stored =
        previous != null && previous.hasRefreshMarker()
            ? previous.getRefreshMarker().getSources()
            : new SemanticSourceTokenMap();
    SemanticSourceTokenMap merged = new SemanticSourceTokenMap();
    merged.putAll(stored);
    Set<String> advanced = new HashSet<>();
    if (live.hasRefreshMarker()) {
      live.getRefreshMarker()
          .getSources()
          .forEach(
              (watchKey, token) -> {
                SemanticSourceToken before = stored.get(watchKey);
                if (before == null || SemanticProfileRefresh.compare(token, before) > 0) {
                  merged.put(watchKey, token);
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
   * Decides {@code current} per profile. The binding decides who owns it. A watched profile is
   * computed by this hook, so the incoming value is never trusted: a profile whose payload and
   * marker are unchanged and whose bound sources did not move keeps the stored value, and a changed
   * profile is recomputed. A profile with its own marker is compared to the entity marker; one
   * without is stale once any bound source moves, and current when only its payload changed. An
   * external profile is left as the connector wrote it, except that a marker-backed one is
   * recomputed.
   */
  private void markCurrent(
      @Nonnull Urn urn,
      @Nonnull SemanticContent live,
      @Nullable SemanticContent previous,
      @Nonnull Set<String> advanced) {
    for (String key : live.getEmbeddings().keySet()) {
      EmbeddingModelData profile = live.getEmbeddings().get(key);
      ContentBinding binding = registry.bindingFor(urn.getEntityType(), key);
      Set<String> watchKeys = SemanticProfileRefresh.watchKeys(binding);
      Set<String> boundAdvanced = new HashSet<>(advanced);
      boundAdvanced.retainAll(watchKeys);
      boolean changed = profileChanged(previous, key, profile);
      if (binding.isExternal()) {
        if (!changed && boundAdvanced.isEmpty()) {
          continue;
        }
        if (profile.hasRefreshMarker()) {
          profile.setCurrent(
              SemanticProfileRefresh.coversBoundSources(
                  profile.getRefreshMarker(), live, watchKeys));
        }
      } else if (changed || !boundAdvanced.isEmpty()) {
        if (profile.hasRefreshMarker()) {
          profile.setCurrent(
              SemanticProfileRefresh.coversBoundSources(
                  profile.getRefreshMarker(), live, watchKeys));
        } else {
          profile.setCurrent(boundAdvanced.isEmpty());
        }
      } else {
        // Unchanged implies a stored profile under this key (profileChanged is true otherwise).
        restoreStoredCurrent(profile, previous.getEmbeddings().get(key));
      }
    }
  }

  /**
   * A client that rewrites only {@code current} does not move freshness. Put back the stored value,
   * or leave it unset when the stored profile had none (legacy data).
   */
  private static void restoreStoredCurrent(
      @Nonnull EmbeddingModelData profile, @Nonnull EmbeddingModelData stored) {
    if (stored.hasCurrent()) {
      profile.setCurrent(stored.isCurrent());
    } else {
      profile.removeCurrent();
    }
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
}
