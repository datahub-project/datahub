package com.datahub.plugins.auth.pojo;

import com.datahub.plugins.common.PluginConfigImpl;
import com.datahub.plugins.common.PluginConfigWithJar;
import com.datahub.plugins.common.PluginType;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
public class AuthPluginConfig extends PluginConfigImpl implements PluginConfigWithJar {
  private Path pluginDirectoryPath;
  private Path pluginJarPath;

  public AuthPluginConfig(PluginType type, String name, Boolean enabled, String className,
      Path pluginDirectoryPath, Path pluginJarPath, Optional<Map<String, Object>> configs) {
    super(type, name, enabled, className, configs);
    this.pluginDirectoryPath = pluginDirectoryPath;
    this.pluginJarPath = pluginJarPath;
  }
}
