package com.linkedin.metadata.config.search;

import lombok.Data;

@Data
public class ElasticSearchConfiguration {

  private BuildIndicesConfiguration buildIndices;
  public String implementation;
  private SearchConfiguration search;
}
