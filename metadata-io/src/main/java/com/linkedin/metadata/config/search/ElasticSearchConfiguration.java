package com.linkedin.metadata.config.search;

import lombok.Data;
import org.springframework.context.annotation.Configuration;


@Data
@Configuration
public class ElasticSearchConfiguration {

  private BuildIndicesConfiguration buildIndices;
  public String implementation;
  private SearchConfiguration search;
}
