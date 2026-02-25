package com.linkedin.metadata.models.registry;

import com.linkedin.metadata.aspect.patch.template.AspectTemplateEngine;
import com.linkedin.metadata.aspect.plugins.PluginFactory;
import com.linkedin.metadata.aspect.plugins.config.PluginConfiguration;
import com.linkedin.metadata.aspect.plugins.hooks.MCLSideEffect;
import com.linkedin.metadata.aspect.plugins.hooks.MCPSideEffect;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.DefaultEntitySpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.EventSpec;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * The Entity Registry provides a mechanism to retrieve metadata about entities modeled in GMA.
 * Metadata includes the entity's common name, the aspects that comprise it, and search index +
 * relationship index information about the entity.
 */
public interface EntityRegistry {

  default String getIdentifier() {
    return "Unknown";
  }

  /**
   * Given an entity name, returns an instance of {@link DefaultEntitySpec}
   *
   * @param entityName the name of the entity to be retrieved
   * @return an {@link DefaultEntitySpec} corresponding to the entity name provided, null if none
   *     exists.
   */
  @Nullable
  EntitySpec getEntitySpec(@Nonnull final String entityName);

  /**
   * Given an event name, returns an instance of {@link EventSpec}.
   *
   * @param eventName the name of the event to be retrieved
   * @return an {@link EventSpec} corresponding to the entity name provided, null if none exists.
   */
  @Nullable
  EventSpec getEventSpec(@Nonnull final String eventName);

  /**
   * Returns all {@link DefaultEntitySpec}s that the registry is aware of.
   *
   * @return a map of String to {@link DefaultEntitySpec}s, empty map if none exists.
   */
  @Nonnull
  Map<String, EntitySpec> getEntitySpecs();

  /**
   * Returns all {@link DefaultEntitySpec}s that belong to a specific search group.
   *
   * @param searchGroup the search group to filter by
   * @return a map of String to {@link DefaultEntitySpec}s for the specified group, empty map if
   *     none exists.
   */
  @Nonnull
  default Map<String, EntitySpec> getEntitySpecsBySearchGroup(@Nonnull final String searchGroup) {
    return getEntitySpecs().entrySet().stream()
        .filter(entry -> searchGroup.equals(entry.getValue().getSearchGroup()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Returns all unique search group names that exist in the registry.
   *
   * @return a set of all search group names
   */
  @Nonnull
  default Set<String> getSearchGroups() {
    return getEntitySpecs().values().stream()
        .map(EntitySpec::getSearchGroup)
        .collect(Collectors.toSet());
  }

  /**
   * Returns all {@link AspectSpec}s that the registry is aware of.
   *
   * @return a map of String to {@link AspectSpec}s, empty map if none exists.
   */
  @Nonnull
  Map<String, AspectSpec> getAspectSpecs();

  /** Returns all {@link EventSpec}s that the registry is aware of. */
  @Nonnull
  Map<String, EventSpec> getEventSpecs();

  /**
   * Returns an {@link AspectTemplateEngine} that is used for generating templates from {@link
   * com.linkedin.metadata.models.AspectSpec}s
   *
   * @return a template engine instance associated with this registry
   */
  @Nonnull
  AspectTemplateEngine getAspectTemplateEngine();

  /**
   * Prefer {@link com.linkedin.metadata.aspect.batch.AspectsBatch} instead of using this method
   * directly.
   *
   * @return List of validator implementations
   */
  @Nonnull
  default List<AspectPayloadValidator> getAllAspectPayloadValidators() {
    return getPluginFactory().getAspectPayloadValidators();
  }

  /**
   * Returns mutation hooks.
   *
   * <p>Prefer {@link com.linkedin.metadata.aspect.batch.AspectsBatch} instead of using this method
   * directly.
   *
   * @return list of mutation hooks.
   */
  @Nonnull
  default List<MutationHook> getAllMutationHooks() {
    return getPluginFactory().getMutationHooks();
  }

  /**
   * Returns the side effects to apply to {@link com.linkedin.mxe.MetadataChangeProposal}. Side
   * effects can generate one or more additional MCPs during write operations.
   *
   * <p>Prefer {@link com.linkedin.metadata.aspect.batch.AspectsBatch} instead of using this method
   * directly.
   *
   * @return MCP side effects
   */
  @Nonnull
  default List<MCPSideEffect> getAllMCPSideEffects() {
    return getPluginFactory().getMcpSideEffects();
  }

  /**
   * Returns the side effects to apply to {@link com.linkedin.mxe.MetadataChangeLog}. Side effects
   * can generate one or more additional MCLs during write operations.
   *
   * <p>Prefer {@link com.linkedin.metadata.aspect.batch.AspectsBatch} instead of using this method
   * directly.
   *
   * @return MCL side effects
   */
  @Nonnull
  default List<MCLSideEffect> getAllMCLSideEffects() {
    return getPluginFactory().getMclSideEffects();
  }

  /**
   * Returns underlying plugin factory
   *
   * @return the plugin factory
   */
  @Nonnull
  default PluginFactory getPluginFactory() {
    return PluginFactory.empty();
  }

  @Nullable
  default BiFunction<PluginConfiguration, List<ClassLoader>, PluginFactory>
      getPluginFactoryProvider() {
    return null;
  }
}
