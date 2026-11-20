package com.linkedin.metadata.search.semantic.profile;

import com.linkedin.metadata.config.search.ModelEmbeddingConfig;
import com.linkedin.metadata.config.search.SemanticBindingConfig;
import com.linkedin.metadata.config.search.SemanticProfilesConfiguration;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Registry built from {@code semanticSearch.models} and {@code semanticSearch.profiles}. */
public class ConfiguredSemanticProfileRegistry implements SemanticProfileRegistry {

  private final boolean generationEnabled;
  private final String defaultBindingName;
  private final Map<String, ContentBinding> bindings;
  private final Map<String, ModelEmbeddingConfig> models;

  public ConfiguredSemanticProfileRegistry(@Nullable SemanticSearchConfiguration configuration) {
    SemanticProfilesConfiguration profiles =
        configuration != null && configuration.getProfiles() != null
            ? configuration.getProfiles()
            : new SemanticProfilesConfiguration();
    this.generationEnabled = profiles.isGenerationEnabled();
    this.defaultBindingName =
        profiles.getDefaultBinding() != null ? profiles.getDefaultBinding() : "documentText";
    this.bindings = toBindings(profiles.getBindings());
    this.models =
        configuration != null && configuration.getModels() != null
            ? configuration.getModels()
            : Map.of();
    requireBinding(defaultBindingName, "semanticSearch.profiles.defaultBinding");
    models.forEach(
        (key, model) -> {
          if (model != null && model.getBinding() != null) {
            requireBinding(model.getBinding(), "semanticSearch.models." + key + ".binding");
          }
        });
  }

  /**
   * A misspelled binding would otherwise fall back to another binding and silently change which
   * sources mark the profile stale, so it fails startup instead.
   */
  private void requireBinding(@Nonnull String name, @Nonnull String setting) {
    if (!bindings.containsKey(name)) {
      throw new IllegalArgumentException(
          "Unknown semantic content binding '"
              + name
              + "' in "
              + setting
              + "; configured bindings: "
              + bindings.keySet());
    }
  }

  @Override
  public boolean generationEnabled() {
    return generationEnabled;
  }

  @Override
  public boolean isServiceProfile(@Nonnull String entityType, @Nonnull String profileKey) {
    ModelEmbeddingConfig model = models.get(profileKey);
    return model != null && model.isServiceOwned();
  }

  @Override
  public boolean isConfigured(@Nonnull String entityType, @Nonnull String profileKey) {
    return models.containsKey(profileKey);
  }

  @Nonnull
  @Override
  public ContentBinding bindingFor(@Nonnull String entityType, @Nonnull String profileKey) {
    ModelEmbeddingConfig model = models.get(profileKey);
    String name =
        model != null && model.getBinding() != null ? model.getBinding() : defaultBindingName;
    return bindings.get(name);
  }

  @Nonnull
  private static Map<String, ContentBinding> toBindings(
      @Nullable Map<String, SemanticBindingConfig> configured) {
    Map<String, SemanticBindingConfig> source =
        configured != null ? configured : SemanticProfilesConfiguration.defaultBindings();
    Map<String, ContentBinding> result = new java.util.LinkedHashMap<>();
    for (Map.Entry<String, SemanticBindingConfig> entry : source.entrySet()) {
      result.put(entry.getKey(), binding(entry.getKey(), entry.getValue()));
    }
    result.putIfAbsent("external", ContentBinding.EXTERNAL);
    if (!result.containsKey("documentText")) {
      result.put(
          "documentText",
          new ContentBinding(
              fields("documentText", SemanticProfilesConfiguration.defaultDocumentTextTriggers())));
    }
    return Map.copyOf(result);
  }

  /**
   * Spring ignores keys it cannot bind, so a binding in the older flat shape (aspect names directly
   * under the binding) arrives with no triggers. Requiring {@code external: true} for that case
   * turns it into a startup failure instead of a binding that silently watches nothing.
   */
  @Nonnull
  private static ContentBinding binding(
      @Nonnull String name, @Nullable SemanticBindingConfig config) {
    SemanticBindingConfig source = config != null ? config : new SemanticBindingConfig();
    Map<String, Set<String>> triggers = fields(name, source.getTriggers());
    Map<String, Set<String>> includes = fields(name, source.getIncludes());
    if (source.isExternal()) {
      if (!triggers.isEmpty() || !includes.isEmpty()) {
        throw new IllegalArgumentException(
            "Semantic content binding '"
                + name
                + "' is external and cannot declare triggers or includes");
      }
      return ContentBinding.EXTERNAL;
    }
    if (triggers.isEmpty()) {
      throw new IllegalArgumentException(
          "Semantic content binding '"
              + name
              + "' has no triggers; declare its aspect paths under 'triggers' (and optional"
              + " 'includes'), or set 'external: true'");
    }
    for (Map.Entry<String, Set<String>> include : includes.entrySet()) {
      Set<String> triggered = triggers.get(include.getKey());
      if (triggered == null) {
        continue;
      }
      if (triggered.isEmpty() || include.getValue().isEmpty()) {
        throw new IllegalArgumentException(
            "Semantic content binding '"
                + name
                + "' lists aspect '"
                + include.getKey()
                + "' whole in triggers or includes and also in the other");
      }
      Set<String> overlap = new HashSet<>(include.getValue());
      overlap.retainAll(triggered);
      if (!overlap.isEmpty()) {
        throw new IllegalArgumentException(
            "Semantic content binding '"
                + name
                + "' lists "
                + overlap
                + " of aspect '"
                + include.getKey()
                + "' in both triggers and includes");
      }
    }
    return new ContentBinding(triggers, includes);
  }

  @Nonnull
  private static Map<String, Set<String>> fields(
      @Nonnull String name, @Nullable Map<String, List<String>> configured) {
    Map<String, Set<String>> aspects = new java.util.LinkedHashMap<>();
    Map<String, List<String>> source = configured != null ? configured : Map.of();
    for (Map.Entry<String, List<String>> aspect : source.entrySet()) {
      Set<String> paths = new HashSet<>();
      if (aspect.getValue() != null) {
        for (String path : aspect.getValue()) {
          SemanticProfileRefresh.checkPath(path);
          paths.add(path);
        }
      }
      aspects.put(aspect.getKey(), paths);
    }
    return aspects;
  }
}
