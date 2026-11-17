package com.linkedin.metadata.search.embedding;

import static com.linkedin.metadata.Constants.DOCUMENT_ENTITY_NAME;

import com.linkedin.metadata.config.search.ModelEmbeddingConfig;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.search.template.SearchTemplateProfiles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * The embedding profiles this deployment serves and the default profile for each entity type. The
 * entity and system lists are configuration: the service never asks a store which entity types or
 * profiles it holds.
 */
public final class EmbeddingProfileRegistry {
  public static final String ELASTICSEARCH = "elasticsearch";
  public static final String OPENSEARCH = "opensearch";

  /** A named id the registry does not know. */
  public static final String UNKNOWN_PROFILE = "unknownProfile";

  /** A resolved profile is not available on this deployment's system. */
  public static final String SYSTEM_UNAVAILABLE = "systemUnavailable";

  /** The entity needs its default profile and has none. */
  public static final String NO_DEFAULT = "noDefault";

  /** The selection removes every profile, including the default. */
  public static final String EMPTY_SELECTION = "emptySelection";

  private static final EmbeddingProfileRegistry EMPTY =
      new EmbeddingProfileRegistry(List.of(), Map.of());

  private final Map<String, EmbeddingProfile> profiles;
  private final Map<String, String> defaults;

  /**
   * @param defaults entity type to the id of its default profile, which must cover that type
   */
  public EmbeddingProfileRegistry(
      @Nonnull Collection<EmbeddingProfile> profiles, @Nonnull Map<String, String> defaults) {
    Map<String, EmbeddingProfile> byId = new LinkedHashMap<>();
    for (EmbeddingProfile profile : profiles) {
      if (byId.put(profile.id(), profile) != null) {
        throw new IllegalArgumentException(
            "Embedding profile " + profile.id() + " is registered twice");
      }
    }
    defaults.forEach(
        (entityType, id) -> {
          EmbeddingProfile profile = byId.get(id);
          if (profile == null || !profile.covers(entityType)) {
            throw new IllegalArgumentException(
                "Default embedding profile " + id + " does not cover " + entityType);
          }
        });
    this.profiles = Map.copyOf(byId);
    this.defaults = Map.copyOf(defaults);
  }

  @Nonnull
  public static EmbeddingProfileRegistry empty() {
    return EMPTY;
  }

  /**
   * One {@code textChunks} profile for Document, keyed by the configured model embedding key and
   * available on both native engines. Empty when semantic search is off or the key has no model
   * configuration. The covered types do not follow {@code enabledEntities}: that list is a separate
   * entity-level gate.
   *
   * @param modelKey embeddings map key derived from the configured model, the key semantic search
   *     queries today
   */
  @Nonnull
  public static EmbeddingProfileRegistry fromSemanticSearch(
      @Nullable SemanticSearchConfiguration semanticSearch, @Nullable String modelKey) {
    if (semanticSearch == null
        || !semanticSearch.isEnabled()
        || modelKey == null
        || semanticSearch.getModels() == null) {
      return EMPTY;
    }
    ModelEmbeddingConfig model = semanticSearch.getModels().get(modelKey);
    if (model == null) {
      return EMPTY;
    }
    String modelId =
        semanticSearch.getEmbeddingProvider() == null
            ? null
            : semanticSearch.getEmbeddingProvider().getModelId();
    EmbeddingProfile document =
        new EmbeddingProfile(
            modelKey,
            EmbeddingProfile.TEXT_CHUNKS,
            modelId,
            model.getSpaceType() == null ? "" : model.getSpaceType(),
            Set.of(DOCUMENT_ENTITY_NAME),
            Set.of(ELASTICSEARCH, OPENSEARCH));
    return new EmbeddingProfileRegistry(List.of(document), Map.of(DOCUMENT_ENTITY_NAME, modelKey));
  }

  /** Registry name of the engine that serves Search V3, or null when it is not known. */
  @Nullable
  public static String system(@Nullable SearchClientShim.SearchEngineType engine) {
    if (engine == null) {
      return null;
    }
    if (engine.isOpenSearch()) {
      return OPENSEARCH;
    }
    if (engine.isElasticsearch()) {
      return ELASTICSEARCH;
    }
    return null;
  }

  @Nullable
  public EmbeddingProfile profile(@Nonnull String id) {
    return profiles.get(id);
  }

  @Nullable
  public String defaultFor(@Nonnull String entityType) {
    return defaults.get(entityType);
  }

  /**
   * Resolves a merged {@code profiles} selection for one entity type.
   *
   * <p>A null selection is the entity default. {@code replace} is the list; otherwise {@code add}
   * and {@code remove} patch the entity default. A named profile that does not cover the entity is
   * replaced by the entity default, once. A profile that is not available on {@code system} fails,
   * and so does an id the registry does not know: neither falls back to the default.
   */
  @Nonnull
  public Resolution resolve(
      @Nullable SearchTemplateProfiles selection,
      @Nonnull String entityType,
      @Nullable String system) {
    String fallback = defaultFor(entityType);
    List<String> requested = new ArrayList<>();
    if (selection != null && selection.hasReplace() && selection.getReplace() != null) {
      requested.addAll(selection.getReplace());
    } else {
      Set<String> removed =
          selection != null && selection.hasRemove() && selection.getRemove() != null
              ? Set.copyOf(selection.getRemove())
              : Set.of();
      if (fallback != null && !removed.contains(fallback)) {
        requested.add(fallback);
      }
      if (selection != null && selection.hasAdd() && selection.getAdd() != null) {
        requested.addAll(selection.getAdd());
      }
      if (requested.isEmpty()) {
        return Resolution.failed(fallback == null ? NO_DEFAULT : EMPTY_SELECTION);
      }
    }
    Set<String> resolved = new LinkedHashSet<>();
    for (String id : requested) {
      EmbeddingProfile profile = profiles.get(id);
      if (profile == null) {
        return Resolution.failed(UNKNOWN_PROFILE);
      }
      if (!profile.covers(entityType)) {
        if (fallback == null) {
          return Resolution.failed(NO_DEFAULT);
        }
        profile = profiles.get(fallback);
      }
      if (!profile.availableOn(system)) {
        return Resolution.failed(SYSTEM_UNAVAILABLE);
      }
      resolved.add(profile.id());
    }
    return new Resolution(List.copyOf(resolved), null);
  }

  /**
   * Profile ids for one entity, or the bounded reason it fails embedding profile coverage.
   *
   * @param failure {@link #UNKNOWN_PROFILE}, {@link #SYSTEM_UNAVAILABLE}, {@link #NO_DEFAULT}, or
   *     {@link #EMPTY_SELECTION}; null when covered
   */
  public record Resolution(@Nonnull List<String> profileIds, @Nullable String failure) {
    @Nonnull
    static Resolution failed(@Nonnull String failure) {
      return new Resolution(List.of(), failure);
    }

    public boolean covered() {
      return failure == null;
    }
  }
}
