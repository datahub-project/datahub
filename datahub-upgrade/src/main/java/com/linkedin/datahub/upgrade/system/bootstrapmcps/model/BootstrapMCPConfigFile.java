package com.linkedin.datahub.upgrade.system.bootstrapmcps.model;

import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

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
  }
}
