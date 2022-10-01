package com.datahub.plugins.common;

import java.nio.file.Path;


public interface PluginConfigWithJar extends PluginConfig {
  public Path getPluginHomeDirectory();

  public Path getPluginJarPath();
}
