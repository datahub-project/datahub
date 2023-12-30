package com.linkedin.metadata.aspect.plugins;

import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.models.AspectSpec;
import javax.annotation.Nonnull;

public interface ConfigurableEntityAspectPlugin {
  String ENTITY_WILDCARD = "*";

  AspectPluginConfig getConfig();

  default boolean shouldApply(
      @Nonnull ChangeType changeType, @Nonnull Urn entityUrn, @Nonnull AspectSpec aspectSpec) {
    return shouldApply(changeType, entityUrn.getEntityType(), aspectSpec);
  }

  default boolean shouldApply(
      @Nonnull ChangeType changeType, @Nonnull String entityName, @Nonnull AspectSpec aspectSpec) {
    return getConfig().isEnabled()
        && isChangeTypeSupported(changeType)
        && isEntityAspectSupported(entityName, aspectSpec);
  }

  default boolean isEntityAspectSupported(
      @Nonnull String entityName, @Nonnull AspectSpec aspectSpec) {
    return (ENTITY_WILDCARD.equals(entityName)
            || getConfig().getSupportedEntityAspectNames().stream()
                .anyMatch(supported -> supported.getEntityName().equals(entityName)))
        && isAspectSupported(aspectSpec.getName());
  }

  default boolean isEntityAspectSupported(@Nonnull Urn entityUrn, @Nonnull AspectSpec aspectSpec) {
    return isEntityAspectSupported(entityUrn.getEntityType(), aspectSpec);
  }

  default boolean isAspectSupported(@Nonnull AspectSpec aspectSpec) {
    return isAspectSupported(aspectSpec.getName());
  }

  default boolean isAspectSupported(@Nonnull String aspectName) {
    return getConfig().getSupportedEntityAspectNames().stream()
        .anyMatch(supported -> supported.getAspectName().equals(aspectName));
  }

  default boolean isChangeTypeSupported(@Nonnull ChangeType changeType) {
    return getConfig().getSupportedOperations().stream()
        .anyMatch(supported -> changeType.toString().equals(supported));
  }
}
