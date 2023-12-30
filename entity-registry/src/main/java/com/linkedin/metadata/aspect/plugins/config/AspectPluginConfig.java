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
  private String className;
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
}
