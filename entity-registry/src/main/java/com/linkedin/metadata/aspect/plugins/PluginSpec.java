package com.linkedin.metadata.aspect.plugins;

import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.models.EntitySpec;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public abstract class PluginSpec {
  protected static String WILDCARD = "*";

  @Nonnull
  public abstract AspectPluginConfig getConfig();

  public abstract PluginSpec setConfig(@Nonnull AspectPluginConfig config);

  public boolean enabled() {
    return true;
  }

  public boolean shouldApply(
      @Nullable ChangeType changeType, @Nonnull Urn entityUrn, @Nonnull String aspectName) {
    return shouldApply(changeType, entityUrn.getEntityType(), aspectName);
  }

  public boolean shouldApply(
      @Nullable ChangeType changeType, @Nonnull EntitySpec entitySpec, @Nonnull String aspectName) {
    return shouldApply(changeType, entitySpec.getName(), aspectName);
  }

  public boolean shouldApply(
      @Nullable ChangeType changeType, @Nonnull String entityName, @Nonnull String aspectName) {
    return getConfig().isEnabled()
        && isChangeTypeSupported(changeType)
        && isEntityAspectSupported(entityName, aspectName);
  }

  protected boolean isEntityAspectSupported(
      @Nonnull EntitySpec entitySpec, @Nonnull String aspectName) {
    return isEntityAspectSupported(entitySpec.getName(), aspectName);
  }

  protected boolean isEntityAspectSupported(
      @Nonnull String entityName, @Nonnull String aspectName) {
    return (getConfig().getSupportedEntityAspectNames().stream()
            .anyMatch(
                supported ->
                    WILDCARD.equals(supported.getEntityName())
                        || supported.getEntityName().equals(entityName)))
        && isAspectSupported(aspectName);
  }

  protected boolean isAspectSupported(@Nonnull String aspectName) {
    return getConfig().getSupportedEntityAspectNames().stream()
        .anyMatch(
            supported ->
                WILDCARD.equals(supported.getAspectName())
                    || supported.getAspectName().equals(aspectName));
  }

  protected boolean isChangeTypeSupported(@Nullable ChangeType changeType) {
    return (changeType == null && getConfig().getSupportedOperations().isEmpty())
        || getConfig().getSupportedOperations().stream()
            .anyMatch(
                supported ->
                    WILDCARD.equals(supported)
                        || supported.equalsIgnoreCase(String.valueOf(changeType)));
  }

  // Explicit equals and hash code using abstract fields due to lombok EqualsAndHashCode returning
  // equal for
  // invalid cases

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PluginSpec that = (PluginSpec) o;
    return Objects.equals(getConfig(), that.getConfig());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), getConfig());
  }
}
