package com.linkedin.metadata.search.semantic;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SearchableFieldExtractorTest {

  private EntityRegistry mockEntityRegistry;
  private SearchableFieldExtractor extractor;

  @BeforeMethod
  public void setup() {
    mockEntityRegistry = mock(EntityRegistry.class);
  }

  @Test
  public void testExtractTextFieldsForDataset() {
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    AspectSpec mockAspectSpec = mock(AspectSpec.class);
    SearchableFieldSpec mockFieldSpec = mock(SearchableFieldSpec.class);
    SearchableAnnotation mockAnnotation = mock(SearchableAnnotation.class);

    when(mockEntityRegistry.getEntitySpecs()).thenReturn(Map.of("dataset", mockEntitySpec));
    when(mockEntitySpec.getName()).thenReturn("dataset");
    when(mockEntitySpec.getAspectSpecs()).thenReturn(List.of(mockAspectSpec));
    when(mockAspectSpec.getName()).thenReturn("datasetProperties");
    when(mockAspectSpec.getSearchableFieldSpecs()).thenReturn(List.of(mockFieldSpec));
    when(mockFieldSpec.getSearchableAnnotation()).thenReturn(mockAnnotation);
    when(mockAnnotation.getFieldType()).thenReturn(SearchableAnnotation.FieldType.TEXT);
    when(mockFieldSpec.getPath()).thenReturn(new com.linkedin.data.schema.PathSpec("description"));
    when(mockAnnotation.getFieldName()).thenReturn("description");

    extractor = new SearchableFieldExtractor(mockEntityRegistry);

    List<SearchableTextField> fields = extractor.getSearchableTextFields("dataset");

    assertEquals(fields.size(), 1);
    assertEquals(fields.get(0).getFieldPath(), "description");
    assertEquals(fields.get(0).getAspectName(), "datasetProperties");
    assertFalse(fields.get(0).isNested());
  }

  @Test
  public void testExtractNestedFieldsForDataset() {
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    AspectSpec mockAspectSpec = mock(AspectSpec.class);
    SearchableFieldSpec mockFieldSpec = mock(SearchableFieldSpec.class);
    SearchableAnnotation mockAnnotation = mock(SearchableAnnotation.class);

    when(mockEntityRegistry.getEntitySpecs()).thenReturn(Map.of("dataset", mockEntitySpec));
    when(mockEntitySpec.getName()).thenReturn("dataset");
    when(mockEntitySpec.getAspectSpecs()).thenReturn(List.of(mockAspectSpec));
    when(mockAspectSpec.getName()).thenReturn("schemaMetadata");
    when(mockAspectSpec.getSearchableFieldSpecs()).thenReturn(List.of(mockFieldSpec));
    when(mockFieldSpec.getSearchableAnnotation()).thenReturn(mockAnnotation);
    when(mockAnnotation.getFieldType()).thenReturn(SearchableAnnotation.FieldType.TEXT);
    when(mockFieldSpec.getPath())
        .thenReturn(new com.linkedin.data.schema.PathSpec("fields/*/description"));
    when(mockAnnotation.getFieldName()).thenReturn("fieldDescriptions");

    extractor = new SearchableFieldExtractor(mockEntityRegistry);

    List<SearchableTextField> fields = extractor.getSearchableTextFields("dataset");

    assertEquals(fields.size(), 1);
    assertTrue(fields.get(0).isNested());
    assertTrue(fields.get(0).getFieldPath().contains("*"));
  }

  @Test
  public void testFilterNonTextFields() {
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    AspectSpec mockAspectSpec = mock(AspectSpec.class);

    SearchableFieldSpec textField = mock(SearchableFieldSpec.class);
    SearchableAnnotation textAnnotation = mock(SearchableAnnotation.class);
    when(textField.getSearchableAnnotation()).thenReturn(textAnnotation);
    when(textAnnotation.getFieldType()).thenReturn(SearchableAnnotation.FieldType.TEXT);
    when(textField.getPath()).thenReturn(new com.linkedin.data.schema.PathSpec("description"));
    when(textAnnotation.getFieldName()).thenReturn("description");

    SearchableFieldSpec keywordField = mock(SearchableFieldSpec.class);
    SearchableAnnotation keywordAnnotation = mock(SearchableAnnotation.class);
    when(keywordField.getSearchableAnnotation()).thenReturn(keywordAnnotation);
    when(keywordAnnotation.getFieldType()).thenReturn(SearchableAnnotation.FieldType.KEYWORD);

    when(mockEntityRegistry.getEntitySpecs()).thenReturn(Map.of("dataset", mockEntitySpec));
    when(mockEntitySpec.getName()).thenReturn("dataset");
    when(mockEntitySpec.getAspectSpecs()).thenReturn(List.of(mockAspectSpec));
    when(mockAspectSpec.getName()).thenReturn("datasetProperties");
    when(mockAspectSpec.getSearchableFieldSpecs()).thenReturn(List.of(textField, keywordField));

    extractor = new SearchableFieldExtractor(mockEntityRegistry);

    List<SearchableTextField> fields = extractor.getSearchableTextFields("dataset");

    assertEquals(fields.size(), 1);
    assertEquals(fields.get(0).getFieldPath(), "description");
  }

  @Test
  public void testGetEntitiesWithTextFields() {
    Map<String, EntitySpec> entitySpecs = new HashMap<>();
    entitySpecs.put("dataset", mock(EntitySpec.class));
    entitySpecs.put("chart", mock(EntitySpec.class));

    when(mockEntityRegistry.getEntitySpecs()).thenReturn(entitySpecs);

    for (String entityName : entitySpecs.keySet()) {
      EntitySpec spec = entitySpecs.get(entityName);
      when(spec.getName()).thenReturn(entityName);
      when(spec.getAspectSpecs()).thenReturn(List.of());
    }

    extractor = new SearchableFieldExtractor(mockEntityRegistry);

    Set<String> entities = extractor.getEntitiesWithTextFields();

    assertTrue(entities.contains("dataset"));
    assertTrue(entities.contains("chart"));
  }

  @Test
  public void testGetSearchableTextFieldsForNonexistentEntity() {
    when(mockEntityRegistry.getEntitySpecs()).thenReturn(Map.of());

    extractor = new SearchableFieldExtractor(mockEntityRegistry);

    List<SearchableTextField> fields = extractor.getSearchableTextFields("nonexistent");

    assertTrue(fields.isEmpty());
  }

  @Test
  public void testHasSearchableTextFields() {
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    AspectSpec mockAspectSpec = mock(AspectSpec.class);
    SearchableFieldSpec mockFieldSpec = mock(SearchableFieldSpec.class);
    SearchableAnnotation mockAnnotation = mock(SearchableAnnotation.class);

    when(mockEntityRegistry.getEntitySpecs()).thenReturn(Map.of("dataset", mockEntitySpec));
    when(mockEntitySpec.getName()).thenReturn("dataset");
    when(mockEntitySpec.getAspectSpecs()).thenReturn(List.of(mockAspectSpec));
    when(mockAspectSpec.getName()).thenReturn("datasetProperties");
    when(mockAspectSpec.getSearchableFieldSpecs()).thenReturn(List.of(mockFieldSpec));
    when(mockFieldSpec.getSearchableAnnotation()).thenReturn(mockAnnotation);
    when(mockAnnotation.getFieldType()).thenReturn(SearchableAnnotation.FieldType.TEXT);
    when(mockFieldSpec.getPath()).thenReturn(new com.linkedin.data.schema.PathSpec("description"));
    when(mockAnnotation.getFieldName()).thenReturn("description");

    extractor = new SearchableFieldExtractor(mockEntityRegistry);

    assertTrue(extractor.hasSearchableTextFields("dataset"));
    assertFalse(extractor.hasSearchableTextFields("nonexistent"));
  }
}
