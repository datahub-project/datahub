package com.linkedin.metadata.config;

import lombok.Data;


@Data
public class ElasticSearchConfiguration {

  private BuildIndicesConfiguration buildIndices;
  public String implementation;
}
