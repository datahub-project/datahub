package com.linkedin.metadata.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class StructuredPropertiesConfiguration {

  /** Whether structured properties mappings are applied */
  private boolean enabled;

  /** Whether structured property values can be written */
  private boolean writeEnabled;

  /** Whether structured property mappings are applied in system update job */
  private boolean systemUpdateEnabled;
}
