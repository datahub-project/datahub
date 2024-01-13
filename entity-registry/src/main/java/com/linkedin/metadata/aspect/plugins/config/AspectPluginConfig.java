package com.linkedin.metadata.aspect.plugins.config;

import java.util.List;
import javax.annotation.Nonnull;
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
  private boolean enabled;

  @Nonnull private List<String> supportedOperations;
  @Nonnull private List<EntityAspectName> supportedEntityAspectNames;

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @Builder
  public static class EntityAspectName {
    @Nonnull private String entityName;
    @Nonnull private String aspectName;
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

  private boolean isEqualExcludingEnabled(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    AspectPluginConfig that = (AspectPluginConfig) o;

    if (!className.equals(that.className)) return false;
    if (!supportedOperations.equals(that.supportedOperations)) return false;
    return supportedEntityAspectNames.equals(that.supportedEntityAspectNames);
  }
}
