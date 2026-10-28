package com.linkedin.metadata.search.elasticsearch.query.template;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;

/** Components registered for this process, keyed by name and contract version. */
public final class SearchTemplateComponentRegistry {
  private final Map<Key, SearchTemplateComponent> components;

  public SearchTemplateComponentRegistry(@Nonnull List<SearchTemplateComponent> components) {
    Map<Key, SearchTemplateComponent> byKey = new LinkedHashMap<>();
    for (SearchTemplateComponent component : components) {
      Key key = new Key(component.name(), component.version());
      SearchTemplateComponent previous = byKey.putIfAbsent(key, component);
      if (previous != null) {
        throw new IllegalStateException(
            "Duplicate search template component " + key.name + ":" + key.version);
      }
    }
    this.components = Map.copyOf(byKey);
  }

  @Nonnull
  public static SearchTemplateComponentRegistry empty() {
    return new SearchTemplateComponentRegistry(List.of());
  }

  @Nonnull
  public Optional<SearchTemplateComponent> find(@Nonnull String name, int version) {
    return Optional.ofNullable(components.get(new Key(name, version)));
  }

  @Nonnull
  public SearchTemplateComponent require(@Nonnull String name, int version) {
    return find(name, version)
        .orElseThrow(
            () ->
                new SearchTemplateException(
                    "Search template component is not registered: " + name + ":" + version));
  }

  private record Key(@Nonnull String name, int version) {}
}
