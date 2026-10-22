package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * The {@link V3SearchFields} facts of one entity list, built once. The registry's specs do not
 * change while GMS runs, and a search across every entity type would otherwise rebuild them from
 * thousands of fields on every request and every hit. {@link EntitySpec} has identity equality, so
 * the cache key is the list of spec instances: a different spec with the same name is a different
 * key.
 */
public final class V3SearchFieldModel {

  private static final Cache<List<EntitySpec>, V3SearchFieldModel> MODELS =
      Caffeine.newBuilder().maximumSize(256).build();

  private final Map<String, List<String>> storedParsedNameFields;
  private final Map<String, String> storedParsedNameLabels;
  private final Map<String, List<String>> fullTextFields;
  private final Map<String, Set<String>> analyzerSubfields;
  private final Map<String, List<SearchableFieldSpec>> rootSourceFieldSpecs;
  private final Set<SearchableFieldSpec> entityNameFallbacks;
  private final SearchFieldResolver resolver;

  private V3SearchFieldModel(@Nonnull final List<EntitySpec> entitySpecs) {
    storedParsedNameFields = immutable(V3SearchFields.storedParsedNameFields(entitySpecs));
    final Map<String, String> labels = new LinkedHashMap<>();
    storedParsedNameFields.forEach(
        (label, paths) -> {
          for (String path : paths) {
            labels.putIfAbsent(path, label);
          }
        });
    storedParsedNameLabels = Collections.unmodifiableMap(labels);
    fullTextFields = immutable(V3SearchFields.fullTextFields(entitySpecs));
    analyzerSubfields = immutableSets(V3SearchFields.analyzerSubfields(entitySpecs));
    rootSourceFieldSpecs = immutable(V3SearchFields.rootSourceFieldSpecs(entitySpecs));
    entityNameFallbacks = Set.copyOf(V3SearchFields.entityNameFallbacks(entitySpecs));
    resolver = new SearchFieldResolver(entitySpecs);
  }

  /** The model of exactly these entity specs. */
  @Nonnull
  public static V3SearchFieldModel of(@Nonnull final Collection<EntitySpec> entitySpecs) {
    return MODELS.get(List.copyOf(entitySpecs), V3SearchFieldModel::new);
  }

  /** The model of these entities and every entity that shares their V3 indices. */
  @Nonnull
  public static V3SearchFieldModel forIndexGroup(
      @Nonnull final EntityRegistry entityRegistry,
      @Nonnull final Collection<EntitySpec> entitySpecs) {
    return of(V3SearchFields.indexGroupSpecs(entityRegistry, entitySpecs));
  }

  /** {@link V3SearchFields#storedParsedNameFields}. */
  @Nonnull
  public Map<String, List<String>> storedParsedNameFields() {
    return storedParsedNameFields;
  }

  /** Each stored parsed-name path, with the analyzed URN label it reports. */
  @Nonnull
  public Map<String, String> storedParsedNameLabels() {
    return storedParsedNameLabels;
  }

  /** {@link V3SearchFields#fullTextFields}. */
  @Nonnull
  public Map<String, List<String>> fullTextFields() {
    return fullTextFields;
  }

  /** {@link V3SearchFields#analyzerSubfields}. */
  @Nonnull
  public Map<String, Set<String>> analyzerSubfields() {
    return analyzerSubfields;
  }

  @Nonnull
  public SearchFieldResolver resolver() {
    return resolver;
  }

  /** {@link V3SearchFields#copyDestinations}, from the cached fields. */
  @Nonnull
  public List<String> copyDestinations(@Nullable final Collection<String> configured) {
    return V3SearchFields.copyDestinations(
        fullTextFields.keySet(), rootSourceFieldSpecs, entityNameFallbacks, configured);
  }

  @Nonnull
  private static <V> Map<String, List<V>> immutable(@Nonnull final Map<String, List<V>> source) {
    final Map<String, List<V>> copy = new LinkedHashMap<>();
    source.forEach((key, values) -> copy.put(key, List.copyOf(values)));
    return Collections.unmodifiableMap(copy);
  }

  @Nonnull
  private static Map<String, Set<String>> immutableSets(
      @Nonnull final Map<String, Set<String>> source) {
    final Map<String, Set<String>> copy = new LinkedHashMap<>();
    source.forEach((key, values) -> copy.put(key, Collections.unmodifiableSet(values)));
    return Collections.unmodifiableMap(copy);
  }
}
