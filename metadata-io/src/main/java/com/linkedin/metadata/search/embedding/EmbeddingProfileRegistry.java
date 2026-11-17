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
   * The ids a selection names for one entity type, before availability is checked. A null selection
   * is the entity default. {@code replace} sets the list, otherwise the entity default starts it,
   * and {@code remove} and {@code add} patch it. A named profile that does not cover the entity is
   * replaced by the entity default, once. An unknown id stays in the list.
   *
   * @return the ids, or null when the entity needs a default it does not have
   */
  @Nullable
  public List<String> select(
      @Nullable SearchTemplateProfiles selection, @Nonnull String entityType) {
    String fallback = defaultFor(entityType);
    List<String> requested = new ArrayList<>();
    if (selection != null && selection.hasReplace() && selection.getReplace() != null) {
      requested.addAll(selection.getReplace());
    } else if (fallback != null) {
      requested.add(fallback);
    }
    if (selection != null && selection.hasRemove() && selection.getRemove() != null) {
      requested.removeAll(selection.getRemove());
    }
    if (selection != null && selection.hasAdd() && selection.getAdd() != null) {
      requested.addAll(selection.getAdd());
    }
    Set<String> selected = new LinkedHashSet<>();
    for (String id : requested) {
      EmbeddingProfile profile = profiles.get(id);
      if (profile != null && !profile.covers(entityType)) {
        if (fallback == null) {
          return null;
        }
        selected.add(fallback);
      } else {
        selected.add(id);
      }
    }
    return List.copyOf(selected);
  }

  /**
   * Resolves a merged {@code profiles} selection for one entity type: {@link #select}, then every
   * selected profile must be registered and available on {@code system}. A failure keeps the
   * selected ids and does not fall back to the default.
   */
  @Nonnull
  public Resolution resolve(
      @Nullable SearchTemplateProfiles selection,
      @Nonnull String entityType,
      @Nullable String system) {
    List<String> selected = select(selection, entityType);
    if (selected == null) {
      return new Resolution(List.of(), NO_DEFAULT);
    }
    if (selected.isEmpty()) {
      return new Resolution(
          selected, defaultFor(entityType) == null ? NO_DEFAULT : EMPTY_SELECTION);
    }
    for (String id : selected) {
      EmbeddingProfile profile = profiles.get(id);
      if (profile == null) {
        return new Resolution(selected, UNKNOWN_PROFILE);
      }
      if (!profile.availableOn(system)) {
        return new Resolution(selected, SYSTEM_UNAVAILABLE);
      }
    }
    return new Resolution(selected, null);
  }

  /**
   * The profile ids one entity selects, and the bounded reason it fails embedding profile coverage.
   *
   * @param profileIds the selected ids, also when coverage fails
   * @param failure {@link #UNKNOWN_PROFILE}, {@link #SYSTEM_UNAVAILABLE}, {@link #NO_DEFAULT}, or
   *     {@link #EMPTY_SELECTION}; null when covered
   */
  public record Resolution(@Nonnull List<String> profileIds, @Nullable String failure) {
    public boolean covered() {
      return failure == null;
    }
  }
}
