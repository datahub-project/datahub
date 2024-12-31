package com.linkedin.metadata.config.search;

import lombok.Data;

@Data
public class WordGramConfiguration {
  private float twoGramFactor;
  private float threeGramFactor;
  private float fourGramFactor;
}
