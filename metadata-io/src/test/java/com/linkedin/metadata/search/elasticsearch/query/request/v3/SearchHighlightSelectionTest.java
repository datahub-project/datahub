package com.linkedin.metadata.search.elasticsearch.query.request.v3;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateException;
import com.linkedin.search.template.HighlightFieldOverrides;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class SearchHighlightSelectionTest {

  @Test
  public void testRequestAdjustsTheTemplateSetAndSkipClearsIt() {
    SearchFlags add = new SearchFlags();
    HighlightFieldOverrides request = new HighlightFieldOverrides();
    request.setAdd(new StringArray("description"));
    add.setHighlightFields(request);
    assertEquals(
        SearchHighlightSelection.resolve(List.of("name"), add), List.of("name", "description"));

    SearchFlags remove = new SearchFlags();
    HighlightFieldOverrides removed = new HighlightFieldOverrides();
    removed.setRemove(new StringArray("name.ngram"));
    remove.setHighlightFields(removed);
    assertEquals(
        SearchHighlightSelection.resolve(List.of("name", "tags_1"), remove), List.of("tags_1"));

    SearchFlags replace = new SearchFlags();
    HighlightFieldOverrides replaced = new HighlightFieldOverrides();
    replaced.setReplace(new StringArray("qualifiedName"));
    replace.setHighlightFields(replaced);
    assertEquals(
        SearchHighlightSelection.resolve(List.of("name"), replace), List.of("qualifiedName"));

    SearchFlags empty = new SearchFlags();
    empty.setHighlightFields(new HighlightFieldOverrides());
    assertTrue(SearchHighlightSelection.resolve(List.of("name"), empty).isEmpty());

    HighlightFieldOverrides both = new HighlightFieldOverrides();
    both.setReplace(new StringArray("name"));
    both.setAdd(new StringArray("description"));
    SearchFlags invalid = new SearchFlags().setHighlightFields(both);
    assertThrows(
        SearchTemplateException.class,
        () -> SearchHighlightSelection.resolve(List.of("tags_1"), invalid));

    SearchFlags skipped = new SearchFlags().setSkipHighlighting(true).setHighlightFields(request);
    assertTrue(SearchHighlightSelection.resolve(List.of("name"), skipped).isEmpty());
  }

  @Test
  public void testCustomFieldsReplaceTheTemplateSet() {
    SearchFlags flags = new SearchFlags();
    flags.setCustomHighlightingFields(new StringArray(List.of("description.*", "_aspects.secret")));
    HighlightFieldOverrides request = new HighlightFieldOverrides();
    request.setAdd(new StringArray("name"));
    flags.setHighlightFields(request);

    assertEquals(
        SearchHighlightSelection.resolve(List.of("tags_1"), flags), List.of("description"));
  }

  @Test
  public void testSourcePathsStayInsideTheQueriedFields() {
    Map<String, List<String>> searched =
        Map.of(
            "name", List.of("name", "_aspects.editableDatasetProperties.editedName"),
            "description", List.of("description"),
            "tags_1", List.of("tags"));
    assertEquals(
        SearchHighlightSelection.sourcePaths(
            List.of("name", "description", "notSearched"),
            searched,
            List.of("name", "tags_1"),
            List.of()),
        List.of("name"));
  }

  /** A structured property is its own source path, whether queried directly or through copyTo. */
  @Test
  public void testSourcePathsKeepSelectedStructuredProperties() {
    Map<String, List<String>> searched = Map.of("name", List.of("name"));
    assertEquals(
        SearchHighlightSelection.sourcePaths(
            List.of("structuredProperties.copied", "structuredProperties.queried"),
            searched,
            List.of("name", "structuredProperties.queried"),
            List.of("structuredProperties.copied")),
        List.of("structuredProperties.copied", "structuredProperties.queried"));

    // A property this query does not read is dropped, as any other unqueried field is
    assertTrue(
        SearchHighlightSelection.sourcePaths(
                List.of("structuredProperties.other"),
                searched,
                List.of("name"),
                List.of("structuredProperties.copied"))
            .isEmpty());
  }
}
