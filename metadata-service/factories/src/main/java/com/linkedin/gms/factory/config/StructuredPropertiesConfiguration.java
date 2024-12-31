package com.linkedin.gms.factory.config;

import lombok.Data;

@Data
public class StructuredPropertiesConfiguration {
  private boolean enabled;
  private boolean writeEnabled;
  private boolean systemUpdateEnabled;
}
