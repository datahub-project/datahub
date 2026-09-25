package com.linkedin.metadata.search.opensearch;

/** Search V3 keyword reads while the V2 entity indices are still enabled. */
public class KeywordSearchV3WithV2OpenSearchTest extends KeywordSearchV3OpenSearchTest {
  @Override
  protected boolean isV2Enabled() {
    return true;
  }
}
