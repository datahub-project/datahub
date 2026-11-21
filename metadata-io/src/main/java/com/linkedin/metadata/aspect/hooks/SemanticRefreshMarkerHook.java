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
import com.linkedin.data.template.GetMode;
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

  /** Returns true only when the hook changed the entity marker or a profile's {@code current}. */
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
    SemanticRefreshMarker writtenMarker = live.hasRefreshMarker() ? live.getRefreshMarker() : null;
    Set<String> advanced = reconcileMarker(live, previous);
    SemanticRefreshMarker reconciledMarker =
        live.hasRefreshMarker() ? live.getRefreshMarker() : null;
    boolean markerChanged = !Objects.equals(writtenMarker, reconciledMarker);
    boolean currentChanged = false;
    if (!isApply(item)) {
      currentChanged = markCurrent(item.getUrn(), live, previous, advanced);
    }
    return markerChanged || currentChanged;
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
  private boolean markCurrent(
      @Nonnull Urn urn,
      @Nonnull SemanticContent live,
      @Nullable SemanticContent previous,
      @Nonnull Set<String> advanced) {
    boolean changed = false;
    for (String key : live.getEmbeddings().keySet()) {
      EmbeddingModelData profile = live.getEmbeddings().get(key);
      ContentBinding binding = registry.bindingFor(urn.getEntityType(), key);
      Set<String> watchKeys = SemanticProfileRefresh.watchKeys(binding);
      Set<String> boundAdvanced = new HashSet<>(advanced);
      boundAdvanced.retainAll(watchKeys);
      boolean payloadChanged = profileChanged(previous, key, profile);
      if (binding.isExternal()) {
        if (!payloadChanged && boundAdvanced.isEmpty()) {
          continue;
        }
        if (profile.hasRefreshMarker()) {
          changed |=
              setCurrent(
                  profile,
                  SemanticProfileRefresh.coversBoundSources(
                      profile.getRefreshMarker(), live, watchKeys));
        }
      } else if (payloadChanged || !boundAdvanced.isEmpty()) {
        if (profile.hasRefreshMarker()) {
          changed |=
              setCurrent(
                  profile,
                  SemanticProfileRefresh.coversBoundSources(
                      profile.getRefreshMarker(), live, watchKeys));
        } else {
          changed |= setCurrent(profile, boundAdvanced.isEmpty());
        }
      } else {
        // Unchanged implies a stored profile under this key (profileChanged is true otherwise).
        changed |= restoreStoredCurrent(profile, previous.getEmbeddings().get(key));
      }
    }
    return changed;
  }

  /**
   * A client that rewrites only {@code current} does not move freshness. Put back the stored value,
   * or leave it unset when the stored profile had none (legacy data).
   */
  private static boolean restoreStoredCurrent(
      @Nonnull EmbeddingModelData profile, @Nonnull EmbeddingModelData stored) {
    if (stored.hasCurrent()) {
      return setCurrent(profile, stored.isCurrent());
    }
    if (!profile.hasCurrent()) {
      return false;
    }
    profile.removeCurrent();
    return true;
  }

  /** Sets {@code current} and reports whether the stored value moved. */
  private static boolean setCurrent(@Nonnull EmbeddingModelData profile, boolean current) {
    if (profile.hasCurrent() && profile.isCurrent() == current) {
      return false;
    }
    profile.setCurrent(current);
    return true;
  }

  private static boolean isApply(@Nonnull ChangeMCP item) {
    return item.getSystemMetadata() != null
        && item.getSystemMetadata().hasProperties()
        && item.getSystemMetadata().getProperties().containsKey(SEMANTIC_APPLY_PROPERTY);
  }

  /**
   * Whether this write carries a newly produced payload. Every writer stamps {@code generatedAt}
   * when it builds vectors, so the stamps and the scalar identity fields are enough. The vectors
   * are not compared: a float-to-double representation change would read as a change, and the stamp
   * already covers a real rewrite. A rewrite that moves no stamp keeps the stored {@code current},
   * which is the safe direction.
   */
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
    // Skip and removal writes, and pre-v3 profiles, omit optional fields. The strict getters throw
    // for those, so read them as null.
    return !Objects.equals(
            stored.getContentHash(GetMode.NULL), profile.getContentHash(GetMode.NULL))
        || !Objects.equals(
            stored.getSourceTextSha256(GetMode.NULL), profile.getSourceTextSha256(GetMode.NULL))
        || !Objects.equals(
            stored.getGeneratedAt(GetMode.NULL), profile.getGeneratedAt(GetMode.NULL))
        || !Objects.equals(
            stored.getTotalChunks(GetMode.NULL), profile.getTotalChunks(GetMode.NULL))
        || !Objects.equals(
            stored.getModelVersion(GetMode.NULL), profile.getModelVersion(GetMode.NULL))
        || !Objects.equals(profileMarker(stored), profileMarker(profile));
  }

  @Nullable
  private static SemanticRefreshMarker profileMarker(@Nonnull EmbeddingModelData profile) {
    return profile.hasRefreshMarker() ? profile.getRefreshMarker() : null;
  }
}
