package com.linkedin.metadata.config.search;

import lombok.Data;

@Data
public class SearchConfiguration {

  private int maxTermBucketSize;
  private ExactMatchConfiguration exactMatch;
  private PartialConfiguration partial;
  private CustomConfiguration custom;
  private GraphQueryConfiguration graph;
  private WordGramConfiguration wordGram;
}
