package com.linkedin.metadata.config;

import com.linkedin.metadata.config.structuredProperties.extensions.ModelExtensionValidationConfiguration;
import lombok.Data;

@Data
public class MCPValidationConfig {
  private ModelExtensionValidationConfiguration extensions;
}
