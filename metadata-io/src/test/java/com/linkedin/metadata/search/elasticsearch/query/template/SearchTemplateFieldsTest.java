package com.linkedin.metadata.search.elasticsearch.query.template;

import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.search.elasticsearch.query.request.SearchFieldConfig;
import java.util.List;
import java.util.Set;
import org.testng.annotations.Test;

public class SearchTemplateFieldsTest {

  @Test
  public void testWithoutLabelDropsDegreeCopies() {
    List<String> fields =
        List.of(
            "name.text^10",
            "name_1.text^1",
            "qualifiedName.text^10",
            "qualifiedName_1.text^5",
            "qualifiedName_2^3",
            "description.text^1");
    assertEquals(
        SearchTemplateFields.withoutLabel(fields, "qualifiedName"),
        List.of("name.text^10", "name_1.text^1", "description.text^1"));
  }

  @Test
  public void testWithoutLabelsDropsAllIdentityDegreeCopies() {
    List<String> fields =
        List.of(
            "name.text^10",
            "name_1.text^1",
            "qualifiedName.text^10",
            "qualifiedName_1.text^5",
            "id.text^10",
            "id_1.text^5",
            "description.text^1");
    assertEquals(
        SearchTemplateFields.withoutLabels(fields, Set.of("name", "qualifiedName", "id")),
        List.of("description.text^1"));
  }

  @Test
  public void testLabelFieldsStayOnOneDegree() {
    List<SearchFieldConfig> fields =
        List.of(
            field("qualifiedName.text", 10f),
            field("qualifiedName_1.text", 5f),
            field("name.text", 10f),
            field("name_1.text", 1f));
    assertEquals(
        SearchTemplateFields.labelAnalyzedFields(fields, "qualifiedName"),
        List.of("qualifiedName.text^10"));
    assertEquals(
        SearchTemplateFields.labelAnalyzedFields(fields, "qualifiedName_1"),
        List.of("qualifiedName_1.text^5"));
    assertEquals(SearchTemplateFields.labelAnalyzedFields(fields, "name"), List.of("name.text^10"));
  }

  @Test
  public void testIdentityFieldsExcludeDegreeCopies() {
    List<SearchFieldConfig> fields =
        List.of(
            field("name.text", 10f),
            field("name_1.text", 1f),
            field("qualifiedName.text", 10f),
            field("qualifiedName_1.text", 1f),
            field("id.text", 10f),
            field("id_1.text", 1f));
    assertEquals(
        SearchTemplateFields.identityAnalyzedFields(fields),
        List.of("name.text^10", "qualifiedName.text^10", "id.text^10"));
  }

  private static SearchFieldConfig field(String name, float boost) {
    return SearchFieldConfig.builder().fieldName(name).boost(boost).build();
  }
}
