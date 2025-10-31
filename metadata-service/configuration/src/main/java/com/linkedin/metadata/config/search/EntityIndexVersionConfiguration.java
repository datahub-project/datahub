package com.linkedin.metadata.config.search;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class EntityIndexVersionConfiguration {
  private boolean enabled;
  private boolean cleanup;
  private String analyzerConfig;
  private String mappingConfig;
  private Integer maxFieldsLimit;
}
