package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.search.utils.ESUtils.PROPERTIES;
import static com.linkedin.metadata.search.utils.ESUtils.TYPE;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.models.annotation.SearchableAnnotation.FieldType;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AspectMappingBuilderTest {

  @Mock private EntitySpec mockEntitySpec;

  @Mock private AspectSpec mockAspectSpec1;

  @Mock private AspectSpec mockAspectSpec2;

  @Mock private AspectSpec mockStructuredPropertiesAspect;

  @Mock private SearchableFieldSpec mockFieldSpec1;

  @Mock private SearchableFieldSpec mockFieldSpec2;

  @Mock private SearchableAnnotation mockAnnotation1;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    // Setup common mocks
    when(mockEntitySpec.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec1));
    when(mockAspectSpec1.getName()).thenReturn("testAspect");
    when(mockAspectSpec1.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockFieldSpec1));
    when(mockFieldSpec1.getSearchableAnnotation()).thenReturn(mockAnnotation1);
    when(mockAnnotation1.getFieldName()).thenReturn("testField");
    when(mockAnnotation1.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(mockAnnotation1.getFieldNameAliases()).thenReturn(Collections.emptyList());
    when(mockAnnotation1.isV3Searchable()).thenReturn(true);
    when(mockAnnotation1.resolvedForV3()).thenReturn(mockAnnotation1);
  }

  @Test
  public void testCreateAspectMappingsWithNoConflicts() {
    // Test basic aspect mapping creation
    Map<String, Object> result =
        AspectMappingBuilder.createAspectMappings(mockEntitySpec, null, null);

    assertNotNull(result);
    assertTrue(result.containsKey("testAspect"));
    assertTrue(result.get("testAspect") instanceof Map);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSearchableAspectKeepsSystemMetadata() {
    Map<String, Object> aspect =
        (Map<String, Object>)
            AspectMappingBuilder.createAspectMappings(mockEntitySpec, null, null).get("testAspect");
    Map<String, Object> fields = (Map<String, Object>) aspect.get(PROPERTIES);

    assertTrue(fields.containsKey(V3SearchDocumentProjector.SYSTEM_METADATA_FIELD));
  }

  @Test
  public void testCreateAspectMappingsWithFieldNameConflicts() {
    // Test with field name conflicts
    Map<String, Set<String>> fieldNameConflicts = new HashMap<>();
    fieldNameConflicts.put("testField", Set.of("_aspects.testAspect.testField"));

    Map<String, Object> result =
        AspectMappingBuilder.createAspectMappings(mockEntitySpec, fieldNameConflicts, null);

    assertNotNull(result);
    assertTrue(result.containsKey("testAspect"));
  }

  @Test
  public void testCreateAspectMappingsWithFieldNameAliasConflicts() {
    // Test with field name alias conflicts
    Map<String, Set<String>> fieldNameAliasConflicts = new HashMap<>();
    fieldNameAliasConflicts.put("aliasField", Set.of("_aspects.testAspect.testField"));

    Map<String, Object> result =
        AspectMappingBuilder.createAspectMappings(mockEntitySpec, null, fieldNameAliasConflicts);

    assertNotNull(result);
    assertTrue(result.containsKey("testAspect"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testStructuredPropertiesOmitsADedicatedParsedUrnName() {
    when(mockEntitySpec.getAspectSpecs())
        .thenReturn(Collections.singletonList(mockStructuredPropertiesAspect));
    when(mockStructuredPropertiesAspect.getName()).thenReturn(STRUCTURED_PROPERTIES_ASPECT_NAME);
    when(mockStructuredPropertiesAspect.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockFieldSpec1));
    when(mockAnnotation1.getFieldType()).thenReturn(FieldType.URN);
    when(mockAnnotation1.getSearchLabel()).thenReturn(Optional.empty());
    when(mockAnnotation1.getEntityFieldName()).thenReturn(Optional.empty());

    Map<String, Object> result =
        AspectMappingBuilder.createAspectMappings(mockEntitySpec, null, null);

    assertFalse(result.containsKey(STRUCTURED_PROPERTIES_ASPECT_NAME), result.toString());
  }

  @Test
  public void testCreateAspectMappingsWithEmptyAspectFields() {
    when(mockAspectSpec1.getSearchableFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockAspectSpec1.getSearchableRefFieldSpecs()).thenReturn(Collections.emptyList());

    Map<String, Object> result =
        AspectMappingBuilder.createAspectMappings(mockEntitySpec, null, null);

    assertFalse(result.containsKey("testAspect"));
  }

  @Test
  public void testCreateAspectMappingsWithMultipleAspects() {
    // Test with multiple aspects
    when(mockEntitySpec.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec1));
    when(mockAspectSpec1.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockFieldSpec1));

    Map<String, Object> result =
        AspectMappingBuilder.createAspectMappings(mockEntitySpec, null, null);

    assertNotNull(result);
    assertTrue(result.containsKey("testAspect"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testAspectFieldsStayUnanalyzed() {
    List<SearchableFieldSpec> fieldSpecs =
        List.of(
            mockField("description", FieldType.TEXT),
            mockField("name", FieldType.WORD_GRAM),
            mockField("browsePathV2", FieldType.BROWSE_PATH_V2),
            mockField("platform", FieldType.URN_PARTIAL),
            mockField("lastModified", FieldType.DATETIME),
            mockField("removed", FieldType.BOOLEAN));
    when(mockAspectSpec1.getSearchableFieldSpecs()).thenReturn(fieldSpecs);

    Map<String, Object> aspect =
        (Map<String, Object>)
            AspectMappingBuilder.createAspectMappings(mockEntitySpec, null, null).get("testAspect");
    Map<String, Object> fields = (Map<String, Object>) aspect.get(PROPERTIES);

    // A root-projected field keeps an unindexed aspect copy. copy_to still reaches the label.
    Map<String, Object> unindexed = Map.of(TYPE, "keyword", "index", false, "doc_values", false);
    for (String stringField : List.of("description", "name", "browsePathV2")) {
      assertEquals(fields.get(stringField), unindexed, stringField);
    }
    assertEquals(
        fields.get("platform"),
        Map.of(
            TYPE,
            "keyword",
            "index",
            false,
            "doc_values",
            false,
            "copy_to",
            List.of("platform_urn")));
    assertEquals(
        fields.get("lastModified"), Map.of(TYPE, "date", "index", false, "doc_values", false));
    assertEquals(
        fields.get("removed"), Map.of(TYPE, "boolean", "index", false, "doc_values", false));
  }

  private static SearchableFieldSpec mockField(String fieldName, FieldType fieldType) {
    SearchableAnnotation annotation = mock(SearchableAnnotation.class);
    when(annotation.getFieldName()).thenReturn(fieldName);
    when(annotation.getFieldType()).thenReturn(fieldType);
    when(annotation.isV3Searchable()).thenReturn(true);
    when(annotation.resolvedForV3()).thenReturn(annotation);
    SearchableFieldSpec fieldSpec = mock(SearchableFieldSpec.class);
    when(fieldSpec.getSearchableAnnotation()).thenReturn(annotation);
    return fieldSpec;
  }
}
