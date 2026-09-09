package com.linkedin.datahub.graphql.types.common.mappers;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import com.linkedin.datahub.graphql.generated.SearchFlags;
import org.testng.annotations.Test;

public class SearchFlagsInputMapperTest {

  @Test
  public void testMinScoreMappedThrough() {
    SearchFlags input = new SearchFlags();
    input.setMinScore(0.75f);
    com.linkedin.metadata.query.SearchFlags result = SearchFlagsInputMapper.map(null, input);
    assertEquals(result.getMinScore(), 0.75f);
  }

  @Test
  public void testMinScoreOmittedWhenNull() {
    com.linkedin.metadata.query.SearchFlags result =
        SearchFlagsInputMapper.map(null, new SearchFlags());
    assertFalse(result.hasMinScore());
  }
}
