package com.linkedin.metadata.search.semantic.profile;

import com.linkedin.metadata.config.search.ModelEmbeddingConfig;
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

  @Nonnull
  @Override
  public ContentBinding bindingFor(@Nonnull String entityType, @Nonnull String profileKey) {
    ModelEmbeddingConfig model = models.get(profileKey);
    String name =
        model != null && model.getBinding() != null ? model.getBinding() : defaultBindingName;
    return bindings.getOrDefault(name, ContentBinding.EXTERNAL);
  }

  @Nonnull
  private static Map<String, ContentBinding> toBindings(
      @Nullable Map<String, Map<String, List<String>>> configured) {
    Map<String, Map<String, List<String>>> source =
        configured != null ? configured : SemanticProfilesConfiguration.defaultBindings();
    Map<String, ContentBinding> result = new java.util.LinkedHashMap<>();
    for (Map.Entry<String, Map<String, List<String>>> entry : source.entrySet()) {
      result.put(entry.getKey(), binding(entry.getValue()));
    }
    result.putIfAbsent("external", ContentBinding.EXTERNAL);
    if (!result.containsKey("documentText")) {
      result.put(
          "documentText",
          binding(SemanticProfilesConfiguration.defaultBindings().get("documentText")));
    }
    return Map.copyOf(result);
  }

  @Nonnull
  private static ContentBinding binding(@Nullable Map<String, List<String>> fields) {
    Map<String, Set<String>> aspects = new java.util.LinkedHashMap<>();
    Map<String, List<String>> source = fields != null ? fields : Map.of();
    for (Map.Entry<String, List<String>> aspect : source.entrySet()) {
      if (aspect.getValue() != null) {
        for (String path : aspect.getValue()) {
          SemanticProfileRefresh.checkPath(path);
        }
      }
      aspects.put(
          aspect.getKey(), aspect.getValue() == null ? Set.of() : new HashSet<>(aspect.getValue()));
    }
    return new ContentBinding(aspects);
  }
}
