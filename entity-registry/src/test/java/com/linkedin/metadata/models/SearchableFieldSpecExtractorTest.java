package com.linkedin.metadata.models;

import static org.testng.Assert.*;

import com.datahub.test.SearchFeatures;
import com.datahub.test.TestEntityInfo;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import java.util.List;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

/** Tests for {@link SearchableFieldSpecExtractor} */
public class SearchableFieldSpecExtractorTest {

  @Test
  public void testSearchableFieldSpecExtractionWithValidAnnotations() {
    // Test that valid searchable annotations are extracted correctly
    EntitySpecBuilder builder = new EntitySpecBuilder();
    AspectSpec aspectSpec =
        builder.buildAspectSpec(new SearchFeatures().schema(), RecordTemplate.class);

    List<SearchableFieldSpec> searchableFieldSpecs = aspectSpec.getSearchableFieldSpecs();
    // Note: SearchFeatures might not have searchable fields, so we just verify the extraction works
    // without throwing exceptions and that any extracted fields are valid

    // Verify that all extracted specs have valid field names (not null)
    for (SearchableFieldSpec spec : searchableFieldSpecs) {
      SearchableAnnotation annotation = spec.getSearchableAnnotation();
      assertNotNull(
          annotation.getFieldName(), "Field name should not be null for valid annotations");
      assertFalse(
          annotation.getFieldName().contains("__"),
          "Field name should not contain double underscores for valid annotations");
    }
  }

  @Test
  public void testSearchableFieldSpecExtractionWithTestEntity() {
    // Test with TestEntityInfo which should have valid searchable annotations
    EntitySpecBuilder builder = new EntitySpecBuilder();
    AspectSpec aspectSpec =
        builder.buildAspectSpec(new TestEntityInfo().schema(), RecordTemplate.class);

    List<SearchableFieldSpec> searchableFieldSpecs = aspectSpec.getSearchableFieldSpecs();
    // Note: TestEntityInfo might not have searchable fields, so we just verify the extraction works
    // without throwing exceptions and that any extracted fields are valid

    // Verify that all extracted specs have valid field names
    for (SearchableFieldSpec spec : searchableFieldSpecs) {
      SearchableAnnotation annotation = spec.getSearchableAnnotation();
      assertNotNull(
          annotation.getFieldName(), "Field name should not be null for valid annotations");
      assertFalse(
          annotation.getFieldName().contains("__"),
          "Field name should not contain double underscores for valid annotations");
    }
  }

  @Test
  public void testSearchableFieldSpecExtractionFiltersNullValues() {
    // This test verifies that the extractor properly filters out null values
    // by ensuring that no field specs are generated with problematic names

    EntitySpecBuilder builder = new EntitySpecBuilder();
    AspectSpec aspectSpec =
        builder.buildAspectSpec(new SearchFeatures().schema(), RecordTemplate.class);

    List<SearchableFieldSpec> searchableFieldSpecs = aspectSpec.getSearchableFieldSpecs();

    // Check that no field names contain double underscores (which would indicate
    // that null values were not properly filtered)
    List<String> problematicFieldNames =
        searchableFieldSpecs.stream()
            .map(spec -> spec.getSearchableAnnotation().getFieldName())
            .filter(fieldName -> fieldName != null && fieldName.contains("__"))
            .collect(Collectors.toList());

    assertTrue(
        problematicFieldNames.isEmpty(),
        "No field names should contain double underscores. Found: " + problematicFieldNames);
  }

  @Test
  public void testSearchableFieldSpecExtractionFieldNameValidation() {
    // Test that extracted field names are valid and meaningful
    EntitySpecBuilder builder = new EntitySpecBuilder();
    AspectSpec aspectSpec =
        builder.buildAspectSpec(new SearchFeatures().schema(), RecordTemplate.class);

    List<SearchableFieldSpec> searchableFieldSpecs = aspectSpec.getSearchableFieldSpecs();

    for (SearchableFieldSpec spec : searchableFieldSpecs) {
      SearchableAnnotation annotation = spec.getSearchableAnnotation();
      String fieldName = annotation.getFieldName();

      // Field name should not be null
      assertNotNull(fieldName, "Field name should not be null");

      // Field name should not be empty
      assertFalse(fieldName.trim().isEmpty(), "Field name should not be empty");

      // Field name should not contain double underscores (path-based names)
      assertFalse(
          fieldName.contains("__"),
          "Field name should not contain double underscores: " + fieldName);

      // Field name should be a reasonable length
      assertTrue(
          fieldName.length() > 0 && fieldName.length() < 100,
          "Field name should be reasonable length: " + fieldName);
    }
  }

  @Test
  public void testCustomPropertiesRespectsExplicitFieldType() {
    // Test that customProperties MAP field with explicit TEXT field type is not converted to
    // MAP_ARRAY
    EntitySpecBuilder builder = new EntitySpecBuilder();
    AspectSpec aspectSpec =
        builder.buildAspectSpec(new DatasetProperties().schema(), RecordTemplate.class);

    List<SearchableFieldSpec> searchableFieldSpecs = aspectSpec.getSearchableFieldSpecs();

    // Find the customProperties field (inherited from CustomProperties)
    SearchableFieldSpec customPropsFieldSpec =
        searchableFieldSpecs.stream()
            .filter(
                spec -> spec.getSearchableAnnotation().getFieldName().equals("customProperties"))
            .findFirst()
            .orElse(null);

    assertNotNull(customPropsFieldSpec, "Should find customProperties field");

    SearchableAnnotation annotation = customPropsFieldSpec.getSearchableAnnotation();

    // customProperties should keep its explicitly specified TEXT field type, not be converted to
    // MAP_ARRAY
    assertEquals(
        annotation.getFieldType(),
        SearchableAnnotation.FieldType.TEXT,
        "customProperties with explicit TEXT field type should not be converted to MAP_ARRAY");

    // Verify the field name is correct
    assertEquals(annotation.getFieldName(), "customProperties", "Field name should be preserved");

    // Verify queryByDefault is true as specified in the annotation
    assertTrue(
        annotation.isQueryByDefault(),
        "customProperties should have queryByDefault=true as specified in annotation");
  }
}
