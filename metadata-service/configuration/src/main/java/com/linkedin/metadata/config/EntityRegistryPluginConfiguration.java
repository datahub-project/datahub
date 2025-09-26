package com.linkedin.metadata.config;

import lombok.Data;

@Data
public class EntityRegistryPluginConfiguration {
  String path;
  int loadDelaySeconds;
  boolean ignoreFailureWhenLoadingRegistry = true;
}
