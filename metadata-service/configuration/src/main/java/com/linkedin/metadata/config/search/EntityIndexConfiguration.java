package com.linkedin.metadata.config.search;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class EntityIndexConfiguration {

  private EntityIndexVersionConfiguration v2;
  private EntityIndexVersionConfiguration v3;
  private SemanticSearchConfiguration semanticSearch;
}
