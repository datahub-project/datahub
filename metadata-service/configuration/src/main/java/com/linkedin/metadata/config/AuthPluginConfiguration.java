package com.linkedin.metadata.config;

import lombok.Data;

@Data
public class AuthPluginConfiguration {
  /** Plugin base directory path, default to /etc/datahub/plugins/auth */
  String path;
}
