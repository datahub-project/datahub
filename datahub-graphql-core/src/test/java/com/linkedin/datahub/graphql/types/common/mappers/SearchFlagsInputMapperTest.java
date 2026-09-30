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

  @Test
  public void testExplainAndSearchTypeMappedThrough() {
    SearchFlags input = new SearchFlags();
    input.setIncludeExplain(true);
    input.setSearchType("DFS_QUERY_THEN_FETCH");
    com.linkedin.metadata.query.SearchFlags result = SearchFlagsInputMapper.map(null, input);
    assertEquals(result.isIncludeExplain(), Boolean.TRUE);
    assertEquals(result.getSearchType(), "DFS_QUERY_THEN_FETCH");

    // Unset flags stay unset, so the schema defaults apply
    com.linkedin.metadata.query.SearchFlags defaults =
        SearchFlagsInputMapper.map(null, new SearchFlags());
    assertFalse(defaults.hasIncludeExplain());
    assertFalse(defaults.hasSearchType());
  }
}
