package com.linkedin.metadata.models.registry;

import com.linkedin.data.schema.compatibility.CompatibilityChecker;
import com.linkedin.data.schema.compatibility.CompatibilityOptions;
import com.linkedin.data.schema.compatibility.CompatibilityResult;
import com.linkedin.metadata.aspect.patch.template.AspectTemplateEngine;
import com.linkedin.metadata.aspect.plugins.PluginFactory;
import com.linkedin.metadata.aspect.plugins.config.PluginConfiguration;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.ConfigEntitySpec;
import com.linkedin.metadata.models.DefaultEntitySpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.EventSpec;
import com.linkedin.metadata.models.annotation.EntityAnnotation;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/** Combines results from two entity registries, where the second takes precedence */
@Slf4j
public class MergedEntityRegistry implements EntityRegistry {

  private final Map<String, EntitySpec> entityNameToSpec;
  private final Map<String, EventSpec> eventNameToSpec;
  private final AspectTemplateEngine _aspectTemplateEngine;
  private final Map<String, AspectSpec> _aspectNameToSpec;

  @Getter @Nonnull private PluginFactory pluginFactory;

  @Getter @Nullable
  private BiFunction<PluginConfiguration, List<ClassLoader>, PluginFactory> pluginFactoryProvider;

  public MergedEntityRegistry(EntityRegistry baseEntityRegistry) {
    // baseEntityRegistry.get*Specs() can return immutable Collections.emptyMap() which fails
    // when this class attempts .put* operations on it.
    entityNameToSpec =
        baseEntityRegistry.getEntitySpecs() != null
            ? new HashMap<>(baseEntityRegistry.getEntitySpecs())
            : new HashMap<>();
    eventNameToSpec =
        baseEntityRegistry.getEventSpecs() != null
            ? new HashMap<>(baseEntityRegistry.getEventSpecs())
            : new HashMap<>();
    baseEntityRegistry.getAspectTemplateEngine();
    _aspectTemplateEngine = baseEntityRegistry.getAspectTemplateEngine();
    _aspectNameToSpec = baseEntityRegistry.getAspectSpecs();
    if (baseEntityRegistry instanceof ConfigEntityRegistry) {
      this.pluginFactory = ((ConfigEntityRegistry) baseEntityRegistry).getPluginFactory();
    } else if (baseEntityRegistry instanceof PatchEntityRegistry) {
      this.pluginFactory = ((PatchEntityRegistry) baseEntityRegistry).getPluginFactory();
    } else {
      this.pluginFactory = PluginFactory.empty();
    }
    this.pluginFactoryProvider = baseEntityRegistry.getPluginFactoryProvider();
  }

  private void validateEntitySpec(EntitySpec entitySpec, final ValidationResult validationResult) {
    if (entitySpec.getKeyAspectSpec() == null) {
      validationResult.setValid(false);
      validationResult
          .getValidationFailures()
          .add(String.format("Key aspect is missing in entity %s", entitySpec.getName()));
    }
  }

  public MergedEntityRegistry apply(EntityRegistry patchEntityRegistry)
      throws EntityRegistryException {

    ValidationResult validationResult = validatePatch(patchEntityRegistry);
    if (!validationResult.isValid()) {
      throw new EntityRegistryException(
          String.format(
              "Failed to validate new registry with %s",
              validationResult.validationFailures.stream().collect(Collectors.joining("\n"))));
    }

    // Merge Aspect Specs
    // (Fixed issue where custom defined aspects are not included in the API specification.)
    //
    if (!patchEntityRegistry.getAspectSpecs().isEmpty()) {
      _aspectNameToSpec.putAll(patchEntityRegistry.getAspectSpecs());
    }

    // Merge Entity Specs
    for (Map.Entry<String, EntitySpec> e2Entry : patchEntityRegistry.getEntitySpecs().entrySet()) {
      if (entityNameToSpec.containsKey(e2Entry.getKey())) {
        EntitySpec mergeEntitySpec =
            mergeEntitySpecs(entityNameToSpec.get(e2Entry.getKey()), e2Entry.getValue());
        entityNameToSpec.put(e2Entry.getKey(), mergeEntitySpec);
      } else {
        // We are inserting a new entity into the registry
        entityNameToSpec.put(e2Entry.getKey(), e2Entry.getValue());
      }
    }

    // Merge Event Specs
    if (!patchEntityRegistry.getEventSpecs().isEmpty()) {
      eventNameToSpec.putAll(patchEntityRegistry.getEventSpecs());
    }
    // TODO: Validate that the entity registries don't have conflicts among each other

    // Merge Plugins
    this.pluginFactory =
        PluginFactory.merge(
            this.pluginFactory, patchEntityRegistry.getPluginFactory(), this.pluginFactoryProvider);

    return this;
  }

  private ValidationResult validatePatch(EntityRegistry patchEntityRegistry) {
    ValidationResult validationResult = new ValidationResult();
    for (Map.Entry<String, EntitySpec> e2Entry : patchEntityRegistry.getEntitySpecs().entrySet()) {
      checkMergeable(
          entityNameToSpec.getOrDefault(e2Entry.getKey(), null),
          e2Entry.getValue(),
          validationResult);
    }
    return validationResult;
  }

  private void checkMergeable(
      EntitySpec existingEntitySpec,
      EntitySpec newEntitySpec,
      final ValidationResult validationResult) {
    if (existingEntitySpec != null) {
      // Check for searchGroup conflicts
      validateSearchGroupCompatibility(existingEntitySpec, newEntitySpec, validationResult);

      existingEntitySpec
          .getAspectSpecMap()
          .forEach(
              (key, value) -> {
                if (newEntitySpec.hasAspect(key)) {
                  CompatibilityResult result =
                      CompatibilityChecker.checkCompatibility(
                          value.getPegasusSchema(),
                          newEntitySpec.getAspectSpec(key).getPegasusSchema(),
                          new CompatibilityOptions());
                  if (result.isError()) {
                    log.error(
                        "{} schema is not compatible with previous schema due to {}",
                        key,
                        result.getMessages());
                    // we want to continue processing all aspects to collect all failures
                    validationResult.setValid(false);
                    validationResult
                        .getValidationFailures()
                        .add(
                            String.format(
                                "%s schema is not compatible with previous schema due to %s",
                                key, result.getMessages()));
                  } else {
                    log.info(
                        "{} schema is compatible with previous schema due to {}",
                        key,
                        result.getMessages());
                  }
                }
              });
    } else {
      validateEntitySpec(newEntitySpec, validationResult);
    }
  }

  /**
   * Validates that searchGroup values are compatible between existing and new entity specs. Rules:
   * 1. If only one registry defines searchGroup (not default), it's compatible 2. If both define
   * the same searchGroup, it's compatible 3. If both define different non-default searchGroups,
   * it's incompatible
   */
  private void validateSearchGroupCompatibility(
      EntitySpec existingEntitySpec, EntitySpec newEntitySpec, ValidationResult validationResult) {

    String existingSearchGroup = existingEntitySpec.getSearchGroup();
    String newSearchGroup = newEntitySpec.getSearchGroup();

    // If both have the same searchGroup, it's compatible
    if (existingSearchGroup.equals(newSearchGroup)) {
      return;
    }

    // If one has default and the other doesn't, it's compatible
    if (EntityAnnotation.DEFAULT_SEARCH_GROUP.equals(existingSearchGroup)
        || EntityAnnotation.DEFAULT_SEARCH_GROUP.equals(newSearchGroup)) {
      return;
    }

    // If both have non-default different searchGroups, it's incompatible
    validationResult.setValid(false);
    validationResult
        .getValidationFailures()
        .add(
            String.format(
                "Entity '%s' has conflicting searchGroup values: existing registry has '%s', "
                    + "new registry has '%s'. Only one searchGroup can be defined per entity across all registries.",
                existingEntitySpec.getName(), existingSearchGroup, newSearchGroup));
  }

  private EntitySpec mergeEntitySpecs(EntitySpec existingEntitySpec, EntitySpec newEntitySpec) {
    Map<String, AspectSpec> aspectSpecMap = new HashMap<>(existingEntitySpec.getAspectSpecMap());
    aspectSpecMap.putAll(newEntitySpec.getAspectSpecMap());

    // Handle searchGroup merging logic:
    // 1. If only one registry defines searchGroup, use that one
    // 2. If both define searchGroup and they're the same, use either
    // 3. If both define searchGroup and they're different, this is an error (should be caught in
    // validation)
    String mergedSearchGroup = determineSearchGroup(existingEntitySpec, newEntitySpec);

    // If the base is a config spec, always create another config spec.
    if (existingEntitySpec instanceof ConfigEntitySpec) {
      // Create a new EntityAnnotation with the merged searchGroup
      EntityAnnotation mergedAnnotation =
          new EntityAnnotation(
              existingEntitySpec.getEntityAnnotation().getName(),
              existingEntitySpec.getEntityAnnotation().getKeyAspect(),
              mergedSearchGroup);

      return new ConfigEntitySpec(
          mergedAnnotation.getName(),
          mergedAnnotation.getKeyAspect(),
          aspectSpecMap.values(),
          mergedSearchGroup);
    }
    // For DefaultEntitySpec, create a new EntityAnnotation with the merged searchGroup
    EntityAnnotation mergedAnnotation =
        new EntityAnnotation(
            existingEntitySpec.getEntityAnnotation().getName(),
            existingEntitySpec.getEntityAnnotation().getKeyAspect(),
            mergedSearchGroup);

    return new DefaultEntitySpec(
        aspectSpecMap.values(),
        mergedAnnotation,
        existingEntitySpec.getSnapshotSchema(),
        existingEntitySpec.getAspectTyperefSchema());
  }

  /**
   * Determines the searchGroup to use when merging two entity specs. Rules: 1. If only one registry
   * defines searchGroup (not default), use that one 2. If both define the same searchGroup, use
   * that one 3. If both define different searchGroups, this is an error (should be caught in
   * validation) 4. If neither defines searchGroup, use default
   */
  private String determineSearchGroup(EntitySpec existingEntitySpec, EntitySpec newEntitySpec) {
    String existingSearchGroup = existingEntitySpec.getSearchGroup();
    String newSearchGroup = newEntitySpec.getSearchGroup();

    // If both have the same searchGroup, use it
    if (existingSearchGroup.equals(newSearchGroup)) {
      return existingSearchGroup;
    }

    // If one has default and the other doesn't, use the non-default one
    if (EntityAnnotation.DEFAULT_SEARCH_GROUP.equals(existingSearchGroup)
        && !EntityAnnotation.DEFAULT_SEARCH_GROUP.equals(newSearchGroup)) {
      return newSearchGroup;
    }

    if (!EntityAnnotation.DEFAULT_SEARCH_GROUP.equals(existingSearchGroup)
        && EntityAnnotation.DEFAULT_SEARCH_GROUP.equals(newSearchGroup)) {
      return existingSearchGroup;
    }

    // If both have non-default different searchGroups, this is an error
    // This should be caught in validation, but if we get here, use the existing one
    // and log a warning
    log.warn(
        "Conflicting searchGroups for entity {}: existing='{}', new='{}'. Using existing searchGroup.",
        existingEntitySpec.getName(),
        existingSearchGroup,
        newSearchGroup);
    return existingSearchGroup;
  }

  @Nonnull
  @Override
  public EntitySpec getEntitySpec(@Nonnull String entityName) {
    String lowercaseEntityName = entityName.toLowerCase();
    if (!entityNameToSpec.containsKey(lowercaseEntityName)) {
      throw new IllegalArgumentException(
          String.format("Failed to find entity with name %s in EntityRegistry", entityName));
    }
    return entityNameToSpec.get(lowercaseEntityName);
  }

  @Nonnull
  @Override
  public EventSpec getEventSpec(@Nonnull String eventName) {
    String lowercaseEventSpec = eventName.toLowerCase();
    if (!eventNameToSpec.containsKey(lowercaseEventSpec)) {
      throw new IllegalArgumentException(
          String.format("Failed to find event with name %s in EntityRegistry", eventName));
    }
    return eventNameToSpec.get(lowercaseEventSpec);
  }

  @Nonnull
  @Override
  public Map<String, EntitySpec> getEntitySpecs() {
    return entityNameToSpec;
  }

  @Nonnull
  @Override
  public Map<String, AspectSpec> getAspectSpecs() {
    return _aspectNameToSpec;
  }

  @Nonnull
  @Override
  public Map<String, EventSpec> getEventSpecs() {
    return eventNameToSpec;
  }

  @Nonnull
  @Override
  public AspectTemplateEngine getAspectTemplateEngine() {
    return _aspectTemplateEngine;
  }

  @Setter
  @Getter
  private static class ValidationResult {
    boolean valid = true;
    List<String> validationFailures = new ArrayList<>();
  }
}
