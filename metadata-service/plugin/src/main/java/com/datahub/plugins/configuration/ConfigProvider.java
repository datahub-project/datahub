package com.datahub.plugins.configuration;

import com.datahub.plugins.common.YamlMapper;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class ConfigProvider {
  public static final String CONFIG_FILE_NAME = "config.yml";

  /**
   * Yaml file path of plugin configuration file. Content of this file should match with {@link Config}
   */
  private final Path configFilePath;

  /**
   * Directory where all plugins are mounted in DataHub GMS.
   * Default pluginBaseDir is /etc/datahub/plugins/auth.
   */
  private final Path pluginBaseDir;

  public ConfigProvider(@Nonnull Path pluginBaseDirectory) {
    this.pluginBaseDir = pluginBaseDirectory.toAbsolutePath();
    this.configFilePath = Paths.get(this.pluginBaseDir.toString(), CONFIG_FILE_NAME);
  }

  private void setPluginDir(@Nonnull PluginConfig pluginConfig) {
    Path pluginDir = Paths.get(this.pluginBaseDir.toString(), pluginConfig.getName());
    pluginConfig.setPluginHomeDirectory(pluginDir);
  }

  public Optional<Config> load() {
    // Check config file should exist
    if (!this.configFilePath.toFile().exists()) {
      log.warn("Configuration {} file not found at location {}", CONFIG_FILE_NAME, this.pluginBaseDir);
      return Optional.empty();
    }

    Config config = new YamlMapper<Config>().fromFile(this.configFilePath, Config.class);
    // set derived attributes
    config.getPlugins().forEach(this::setPluginDir);
    return Optional.of(config);
  }
}