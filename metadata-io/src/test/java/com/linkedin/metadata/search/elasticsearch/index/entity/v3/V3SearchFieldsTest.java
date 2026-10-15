package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class V3SearchFieldsTest {

  private static final EntityRegistry REGISTRY = TestOperationContexts.defaultEntityRegistry();

  @Test
  public void testDatasetQueryFieldsUseRootUnifiedNames() {
    Map<String, List<String>> fields = V3SearchFields.queryFields(List.of(dataset()));

    assertTrue(fields.get("name").containsAll(List.of("name", "editedName")), fields.toString());
    assertEquals(fields.get("qualifiedName"), List.of("qualifiedName"));
    assertTrue(
        fields
            .get("description")
            .containsAll(List.of("description", "editedDescription", "fieldDescriptions")),
        fields.toString());
    assertTrue(
        fields.get("columns").containsAll(List.of("fieldPaths", "fieldLabels")), fields.toString());
    assertTrue(fields.get("tags").contains("fieldTags"), fields.toString());
    assertEquals(fields.get("urn"), List.of("urn"));
    assertFalse(fields.containsKey("structuredProperties"), fields.toString());
    assertFalse(fields.containsKey("other"), fields.toString());
    assertFalse(fields.containsKey("_entityName"), fields.toString());
  }

  @Test
  public void testMatchedFieldSourcesLeaveOutColumns() {
    Map<String, List<String>> fields = V3SearchFields.queryFields(List.of(dataset()));
    List<String> sources = V3SearchFields.matchedFieldSources(fields);

    assertTrue(
        sources.containsAll(List.of("name", "description", "fieldDescriptions")),
        sources.toString());
    assertFalse(sources.contains("fieldPaths"), sources.toString());
    assertFalse(sources.contains("fieldLabels"), sources.toString());
  }

  @Test
  public void testNameAndUrnAreNotStemmed() {
    assertFalse(V3SearchFields.stems("name"));
    assertFalse(V3SearchFields.stems("qualifiedName"));
    assertFalse(V3SearchFields.stems("urn"));
    assertTrue(V3SearchFields.stems("description"));
    assertTrue(V3SearchFields.stems("description_1"));
    assertTrue(V3SearchFields.stems("columns"));
    assertEquals(V3SearchFields.baseName("description_1"), "description");
  }

  @Test
  public void testFullTextMappingShapes() {
    Map<String, Object> name = V3SearchFields.fullTextMapping("name");
    @SuppressWarnings("unchecked")
    Map<String, Object> nameFields = (Map<String, Object>) name.get("fields");
    assertEquals(name.get("type"), "keyword");
    assertTrue(nameFields.containsKey("text"));
    assertTrue(nameFields.containsKey("keyword"));
    assertFalse(nameFields.containsKey("stemmed"), nameFields.toString());

    @SuppressWarnings("unchecked")
    Map<String, Object> descriptionFields =
        (Map<String, Object>) V3SearchFields.fullTextMapping("description").get("fields");
    assertTrue(descriptionFields.containsKey("text"));
    assertTrue(descriptionFields.containsKey("stemmed"));
  }

  @Test
  public void testAnnotationBoostIsTheDefaultWeight() {
    assertEquals(V3SearchFields.boost(List.of(dataset()), "name"), 10f);
    assertEquals(V3SearchFields.boost(List.of(dataset()), "description"), 1f);
    assertTrue(V3SearchFields.analyzedQueryField(List.of(dataset()), "name"));
    assertTrue(V3SearchFields.analyzedQueryField(List.of(dataset()), "urn"));
  }

  @Test
  public void testIndexedNameUsesSearchLabel() {
    SearchableAnnotation description =
        dataset().getSearchableFieldSpecs().stream()
            .map(field -> field.getSearchableAnnotation())
            .filter(annotation -> "fieldDescriptions".equals(annotation.getFieldName()))
            .findFirst()
            .orElseThrow();
    assertEquals(description.indexedName(), "description");
  }

  private static EntitySpec dataset() {
    return REGISTRY.getEntitySpec("dataset");
  }
}
