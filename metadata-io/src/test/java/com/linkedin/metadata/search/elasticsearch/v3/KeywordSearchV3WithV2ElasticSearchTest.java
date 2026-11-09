package com.linkedin.metadata.search.elasticsearch.v3;

/** Search V3 keyword reads while the V2 entity indices are still enabled. */
public class KeywordSearchV3WithV2ElasticSearchTest extends KeywordSearchV3ElasticSearchTest {
  @Override
  protected boolean isV2Enabled() {
    return true;
  }
}
