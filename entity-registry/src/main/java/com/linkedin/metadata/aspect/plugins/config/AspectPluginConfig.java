package com.linkedin.metadata.aspect.plugins.config;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AspectPluginConfig {
  @Nonnull private String className;
  @Nullable private List<String> packageScan;

  private boolean enabled;

  @Nullable private List<String> supportedOperations;
  @Nonnull private List<EntityAspectName> supportedEntityAspectNames;

  @Nullable private SpringPluginConfig spring;

  @Nonnull
  public List<String> getSupportedOperations() {
    return supportedOperations != null ? supportedOperations : Collections.emptyList();
  }

  /**
   * Used to determine is an earlier plugin is disabled by a subsequent plugin
   *
   * @param o the other plugin
   * @return whether this plugin should be disabled based on another plugin
   */
  public boolean isDisabledBy(AspectPluginConfig o) {
    return enabled && this.isEqualExcludingEnabled(o) && !o.enabled;
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @Builder
  public static class EntityAspectName {
    public static final EntityAspectName ALL = new EntityAspectName("*", "*");

    @Nonnull private String entityName;
    @Nonnull private String aspectName;
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @Builder
  public static class SpringPluginConfig {
    private boolean enabled;
    @Nullable private String name;
  }

  private boolean isEqualExcludingEnabled(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    AspectPluginConfig that = (AspectPluginConfig) o;

    if (!className.equals(that.className)) return false;
    if (!Objects.equals(packageScan, that.getPackageScan())) return false;
    if (!Objects.equals(supportedOperations, that.supportedOperations)) return false;
    if (!Objects.equals(spring, that.spring)) return false;
    return supportedEntityAspectNames.equals(that.supportedEntityAspectNames);
  }
}
