package com.linkedin.datahub.graphql.types.common.mappers;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.graphql.exception.ValidationException;
import com.linkedin.datahub.graphql.generated.HighlightFieldOverrides;
import com.linkedin.datahub.graphql.generated.SearchFlags;
import java.util.List;
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

  @Test
  public void testHighlightFieldsMappedThrough() {
    HighlightFieldOverrides overrides = new HighlightFieldOverrides();
    overrides.setAdd(List.of("name"));
    overrides.setRemove(List.of("tags_1"));
    SearchFlags input = new SearchFlags();
    input.setHighlightFields(overrides);
    com.linkedin.metadata.query.SearchFlags result = SearchFlagsInputMapper.map(null, input);
    assertEquals(result.getHighlightFields().getAdd().get(0), "name");
    assertEquals(result.getHighlightFields().getRemove().get(0), "tags_1");

    SearchFlags empty = new SearchFlags();
    empty.setHighlightFields(new HighlightFieldOverrides());
    assertTrue(SearchFlagsInputMapper.map(null, empty).hasHighlightFields());
  }

  @Test
  public void testSearchTemplateMappedThrough() {
    SearchFlags input = new SearchFlags();
    input.setSearchTemplate("urn:li:searchTemplate:search");
    com.linkedin.metadata.query.SearchFlags result = SearchFlagsInputMapper.map(null, input);
    assertEquals(result.getSearchTemplate().toString(), "urn:li:searchTemplate:search");
  }

  @Test
  public void testSearchTemplateBlankIsUnsetAndInvalidUrnsAreRejected() {
    SearchFlags blank = new SearchFlags();
    blank.setSearchTemplate("");
    assertFalse(SearchFlagsInputMapper.map(null, blank).hasSearchTemplate());

    SearchFlags whitespace = new SearchFlags();
    whitespace.setSearchTemplate("  ");
    assertFalse(SearchFlagsInputMapper.map(null, whitespace).hasSearchTemplate());

    SearchFlags invalid = new SearchFlags();
    invalid.setSearchTemplate("not a urn");
    assertThrows(ValidationException.class, () -> SearchFlagsInputMapper.map(null, invalid));

    SearchFlags dataset = new SearchFlags();
    dataset.setSearchTemplate("urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)");
    assertThrows(ValidationException.class, () -> SearchFlagsInputMapper.map(null, dataset));
  }
}
