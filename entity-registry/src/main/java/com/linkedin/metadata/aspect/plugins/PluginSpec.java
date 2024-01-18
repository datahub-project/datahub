package com.linkedin.metadata.aspect.plugins;

import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.models.AspectSpec;
import javax.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;

@AllArgsConstructor
@EqualsAndHashCode
public abstract class PluginSpec {
  protected static String ENTITY_WILDCARD = "*";

  private final AspectPluginConfig aspectPluginConfig;

  protected AspectPluginConfig getConfig() {
    return this.aspectPluginConfig;
  }

  public boolean shouldApply(
      @Nonnull ChangeType changeType, @Nonnull Urn entityUrn, @Nonnull AspectSpec aspectSpec) {
    return shouldApply(changeType, entityUrn.getEntityType(), aspectSpec);
  }

  public boolean shouldApply(
      @Nonnull ChangeType changeType, @Nonnull String entityName, @Nonnull AspectSpec aspectSpec) {
    return shouldApply(changeType, entityName, aspectSpec.getName());
  }

  public boolean shouldApply(
      @Nonnull ChangeType changeType, @Nonnull String entityName, @Nonnull String aspectName) {
    return getConfig().isEnabled()
        && isChangeTypeSupported(changeType)
        && isEntityAspectSupported(entityName, aspectName);
  }

  protected boolean isEntityAspectSupported(
      @Nonnull String entityName, @Nonnull String aspectName) {
    return (ENTITY_WILDCARD.equals(entityName)
            || getConfig().getSupportedEntityAspectNames().stream()
                .anyMatch(supported -> supported.getEntityName().equals(entityName)))
        && isAspectSupported(aspectName);
  }

  protected boolean isAspectSupported(@Nonnull String aspectName) {
    return getConfig().getSupportedEntityAspectNames().stream()
        .anyMatch(supported -> supported.getAspectName().equals(aspectName));
  }

  protected boolean isChangeTypeSupported(@Nonnull ChangeType changeType) {
    return getConfig().getSupportedOperations().stream()
        .anyMatch(supported -> changeType.toString().equals(supported));
  }
}
