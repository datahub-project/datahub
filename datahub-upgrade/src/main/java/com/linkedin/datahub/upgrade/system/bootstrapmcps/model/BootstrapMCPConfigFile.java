package com.linkedin.datahub.upgrade.system.bootstrapmcps.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class BootstrapMCPConfigFile {
  private Bootstrap bootstrap;

  @AllArgsConstructor
  @NoArgsConstructor
  @Data
  @Builder
  public static class Bootstrap {
    private List<MCPTemplate> templates;
  }

  @Slf4j
  @AllArgsConstructor
  @NoArgsConstructor
  @Data
  @Builder
  public static class MCPTemplate {
    @Nonnull private String name;
    @Nonnull private String version;
    @Builder.Default private boolean force = false;
    @Builder.Default private boolean blocking = false;
    @Builder.Default private boolean async = true;
    @Builder.Default private boolean optional = false;
    @Nonnull private String mcps_location;
    @Nullable private String values_env;
    @Nullable private String revision_env;

    public MCPTemplate withOverride(ObjectMapper objectMapper) {
      if (revision_env != null) {
        String overrideJson = System.getenv().getOrDefault(revision_env, "{}");
        try {
          return objectMapper.readerForUpdating(this).readValue(overrideJson);
        } catch (IOException e) {
          log.error("Error applying override {} to {}", overrideJson, this);
          throw new RuntimeException(e);
        }
      }
      return this;
    }
  }
}
