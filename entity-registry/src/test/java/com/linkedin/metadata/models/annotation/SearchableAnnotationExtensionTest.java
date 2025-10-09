package com.linkedin.metadata.models.annotation;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.data.DataMap;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.metadata.models.ModelValidationException;
import java.util.Map;
import org.testng.annotations.Test;

/** Tests the new extensions to {@link SearchableAnnotation} */
public class SearchableAnnotationExtensionTest {

  @Test
  public void testSearchableAnnotationWithNewFields() {
    // Create a test annotation map with the new fields
    DataMap annotationMap = new DataMap();
    annotationMap.put("fieldType", "TEXT");
    annotationMap.put("searchTier", 1);
    annotationMap.put("searchLabel", "priorityField");
    annotationMap.put("queryByDefault", true);
    annotationMap.put("enableAutocomplete", false);
    annotationMap.put("addToFilters", false);
    annotationMap.put("boostScore", 2.0);

    // Create SearchableAnnotation from the map
    SearchableAnnotation annotation =
        SearchableAnnotation.fromPegasusAnnotationObject(
            annotationMap, "testField", DataSchema.Type.STRING, "test context");

    // Verify existing fields still work
    assertEquals(annotation.getFieldName(), "testField");
    assertEquals(annotation.getFieldType(), SearchableAnnotation.FieldType.TEXT);
    assertTrue(annotation.isQueryByDefault());
    assertEquals(annotation.getBoostScore(), 2.0);

    // Verify new fields are parsed correctly
    assertTrue(annotation.getSearchTier().isPresent());
    assertEquals(annotation.getSearchTier().get(), Integer.valueOf(1));

    assertTrue(annotation.getSearchLabel().isPresent());
    assertEquals(annotation.getSearchLabel().get(), "priorityField");
  }

  @Test
  public void testSearchableAnnotationWithoutNewFields() {
    // Create a test annotation map without the new fields (backward compatibility)
    DataMap annotationMap = new DataMap();
    annotationMap.put("fieldType", "KEYWORD");
    annotationMap.put("queryByDefault", false);
    annotationMap.put("enableAutocomplete", true);
    annotationMap.put("addToFilters", true);
    annotationMap.put("boostScore", 1.5);

    // Create SearchableAnnotation from the map
    SearchableAnnotation annotation =
        SearchableAnnotation.fromPegasusAnnotationObject(
            annotationMap, "legacyField", DataSchema.Type.STRING, "legacy test context");

    // Verify existing fields still work
    assertEquals(annotation.getFieldName(), "legacyField");
    assertEquals(annotation.getFieldType(), SearchableAnnotation.FieldType.KEYWORD);
    assertEquals(annotation.getBoostScore(), 1.5);

    // Verify new fields are empty (backward compatibility)
    assertTrue(annotation.getSearchTier().isEmpty());
    assertTrue(annotation.getSearchLabel().isEmpty());
    assertTrue(annotation.getSearchLabel().isEmpty());
  }

  @Test
  public void testSearchableAnnotationWithPartialNewFields() {
    // Create a test annotation map with only some of the new fields
    DataMap annotationMap = new DataMap();
    annotationMap.put("fieldType", "TEXT_PARTIAL");
    annotationMap.put("searchTier", 3);
    annotationMap.put("searchLabel", "relevance");

    // Create SearchableAnnotation from the map
    SearchableAnnotation annotation =
        SearchableAnnotation.fromPegasusAnnotationObject(
            annotationMap, "partialField", DataSchema.Type.STRING, "partial test context");

    // Verify existing fields still work
    assertEquals(annotation.getFieldName(), "partialField");
    assertEquals(annotation.getFieldType(), SearchableAnnotation.FieldType.TEXT_PARTIAL);

    // Verify new fields are parsed correctly
    assertTrue(annotation.getSearchTier().isPresent());
    assertEquals(annotation.getSearchTier().get(), Integer.valueOf(3));

    assertTrue(annotation.getSearchLabel().isPresent());
    assertEquals(annotation.getSearchLabel().get(), "relevance");
  }

  @Test
  public void testSearchTierValidation() {
    // Test valid searchTier
    DataMap validAnnotationMap = new DataMap();
    validAnnotationMap.put("fieldType", "TEXT");
    validAnnotationMap.put("searchTier", 1);

    // Should not throw exception
    SearchableAnnotation annotation =
        SearchableAnnotation.fromPegasusAnnotationObject(
            validAnnotationMap, "testField", DataSchema.Type.STRING, "test context");
    assertEquals(annotation.getSearchTier().get(), Integer.valueOf(1));

    // Test invalid searchTier (< 1)
    DataMap invalidAnnotationMap = new DataMap();
    invalidAnnotationMap.put("fieldType", "TEXT");
    invalidAnnotationMap.put("searchTier", 0);

    assertThrows(
        ModelValidationException.class,
        () -> {
          SearchableAnnotation.fromPegasusAnnotationObject(
              invalidAnnotationMap, "testField", DataSchema.Type.STRING, "test context");
        });

    // Test invalid searchTier (negative)
    DataMap negativeAnnotationMap = new DataMap();
    negativeAnnotationMap.put("fieldType", "TEXT");
    negativeAnnotationMap.put("searchTier", -1);

    assertThrows(
        ModelValidationException.class,
        () -> {
          SearchableAnnotation.fromPegasusAnnotationObject(
              negativeAnnotationMap, "testField", DataSchema.Type.STRING, "test context");
        });
  }

  @Test
  public void testSortLabelValidation() {
    // Test valid searchLabel
    DataMap validAnnotationMap = new DataMap();
    validAnnotationMap.put("fieldType", "TEXT");
    validAnnotationMap.put("searchLabel", "validLabel");

    // Should not throw exception
    SearchableAnnotation annotation =
        SearchableAnnotation.fromPegasusAnnotationObject(
            validAnnotationMap, "testField", DataSchema.Type.STRING, "test context");
    assertEquals(annotation.getSearchLabel().get(), "validLabel");

    // Test searchLabel with spaces
    DataMap spacesAnnotationMap = new DataMap();
    spacesAnnotationMap.put("fieldType", "TEXT");
    spacesAnnotationMap.put("searchLabel", "invalid label");

    assertThrows(
        ModelValidationException.class,
        () -> {
          SearchableAnnotation.fromPegasusAnnotationObject(
              spacesAnnotationMap, "testField", DataSchema.Type.STRING, "test context");
        });

    // Test searchLabel starting with underscore
    DataMap underscoreAnnotationMap = new DataMap();
    underscoreAnnotationMap.put("fieldType", "TEXT");
    underscoreAnnotationMap.put("searchLabel", "_invalidLabel");

    assertThrows(
        ModelValidationException.class,
        () -> {
          SearchableAnnotation.fromPegasusAnnotationObject(
              underscoreAnnotationMap, "testField", DataSchema.Type.STRING, "test context");
        });

    // Test empty searchLabel
    DataMap emptyAnnotationMap = new DataMap();
    emptyAnnotationMap.put("fieldType", "TEXT");
    emptyAnnotationMap.put("searchLabel", "");

    assertThrows(
        ModelValidationException.class,
        () -> {
          SearchableAnnotation.fromPegasusAnnotationObject(
              emptyAnnotationMap, "testField", DataSchema.Type.STRING, "test context");
        });

    // Test searchLabel with invalid characters
    DataMap invalidCharsAnnotationMap = new DataMap();
    invalidCharsAnnotationMap.put("fieldType", "TEXT");
    invalidCharsAnnotationMap.put("searchLabel", "invalid@label");

    assertThrows(
        ModelValidationException.class,
        () -> {
          SearchableAnnotation.fromPegasusAnnotationObject(
              invalidCharsAnnotationMap, "testField", DataSchema.Type.STRING, "test context");
        });
  }

  @Test
  public void testRankingLabelValidation() {
    // Test valid searchLabel
    DataMap validAnnotationMap = new DataMap();
    validAnnotationMap.put("fieldType", "TEXT");
    validAnnotationMap.put("searchLabel", "validRanking");

    // Should not throw exception
    SearchableAnnotation annotation =
        SearchableAnnotation.fromPegasusAnnotationObject(
            validAnnotationMap, "testField", DataSchema.Type.STRING, "test context");
    assertEquals(annotation.getSearchLabel().get(), "validRanking");

    // Test searchLabel with spaces
    DataMap spacesAnnotationMap = new DataMap();
    spacesAnnotationMap.put("fieldType", "TEXT");
    spacesAnnotationMap.put("searchLabel", "invalid ranking");

    assertThrows(
        ModelValidationException.class,
        () -> {
          SearchableAnnotation.fromPegasusAnnotationObject(
              spacesAnnotationMap, "testField", DataSchema.Type.STRING, "test context");
        });

    // Test searchLabel starting with underscore
    DataMap underscoreAnnotationMap = new DataMap();
    underscoreAnnotationMap.put("fieldType", "TEXT");
    underscoreAnnotationMap.put("searchLabel", "_invalidRanking");

    assertThrows(
        ModelValidationException.class,
        () -> {
          SearchableAnnotation.fromPegasusAnnotationObject(
              underscoreAnnotationMap, "testField", DataSchema.Type.STRING, "test context");
        });
  }

  @Test
  public void testValidElasticsearchFieldNames() {
    // Test various valid field name patterns
    String[] validNames = {
      "validName",
      "valid123",
      "valid.name",
      "valid-name",
      "valid_name",
      "a",
      "a1",
      "name.with.dots",
      "name-with-hyphens",
      "name_with_underscores"
    };

    for (String validName : validNames) {
      DataMap annotationMap = new DataMap();
      annotationMap.put("fieldType", "TEXT");
      annotationMap.put("searchLabel", validName);

      // Should not throw exception
      SearchableAnnotation annotation =
          SearchableAnnotation.fromPegasusAnnotationObject(
              annotationMap, "testField", DataSchema.Type.STRING, "test context for " + validName);
      assertEquals(annotation.getSearchLabel().get(), validName);
    }
  }

  @Test
  public void testInvalidElasticsearchFieldNames() {
    // Test various invalid field name patterns
    String[] invalidNames = {
      "123invalid", // starts with number
      "_invalid", // starts with underscore
      "invalid name", // contains space
      "invalid@name", // contains @
      "invalid#name", // contains #
      "invalid$name", // contains $
      "invalid%name", // contains %
      "", // empty
      "   ", // whitespace only
    };

    for (String invalidName : invalidNames) {
      DataMap annotationMap = new DataMap();
      annotationMap.put("fieldType", "TEXT");
      annotationMap.put("searchLabel", invalidName);

      // Should throw exception
      assertThrows(
          ModelValidationException.class,
          () -> {
            SearchableAnnotation.fromPegasusAnnotationObject(
                annotationMap,
                "testField",
                DataSchema.Type.STRING,
                "test context for " + invalidName);
          });
    }
  }

  @Test
  public void testSearchTierFieldTypeValidation() {
    // Test valid case: string field with searchTier
    DataMap validAnnotationMap = new DataMap();
    validAnnotationMap.put("fieldType", "TEXT");
    validAnnotationMap.put("searchTier", 1);

    // Should not throw exception
    SearchableAnnotation annotation =
        SearchableAnnotation.fromPegasusAnnotationObject(
            validAnnotationMap, "testField", DataSchema.Type.STRING, "test context");

    assertTrue(annotation.getSearchTier().isPresent());
    assertEquals(annotation.getSearchTier().get(), Integer.valueOf(1));
  }

  @Test
  public void testSearchTierFieldTypeValidationFailure() {
    // Test invalid case: non-string field with searchTier
    DataMap invalidAnnotationMap = new DataMap();
    invalidAnnotationMap.put("fieldType", "COUNT");
    invalidAnnotationMap.put("searchTier", 1);

    assertThrows(
        ModelValidationException.class,
        () ->
            SearchableAnnotation.fromPegasusAnnotationObject(
                invalidAnnotationMap, "testField", DataSchema.Type.INT, "test context"));
  }

  @Test
  public void testSearchTierFieldTypeValidationWithDifferentTypes() {
    // Test various invalid field types for searchTier
    String[] invalidFieldTypes = {
      "COUNT", "BOOLEAN", "DATETIME", "OBJECT", "BROWSE_PATH", "URN_PARTIAL"
    };

    for (String invalidFieldType : invalidFieldTypes) {
      DataMap annotationMap = new DataMap();
      annotationMap.put("fieldType", invalidFieldType);
      annotationMap.put("searchTier", 1);

      assertThrows(
          ModelValidationException.class,
          () ->
              SearchableAnnotation.fromPegasusAnnotationObject(
                  annotationMap, "testField", DataSchema.Type.STRING, "test context"));
    }
  }

  @Test
  public void testSearchTierWithWordGramFieldType() {
    // Test that WORD_GRAM is a valid field type for searchTier
    DataMap annotationMap = new DataMap();
    annotationMap.put("fieldType", "WORD_GRAM");
    annotationMap.put("searchTier", 1);

    // Should not throw exception
    SearchableAnnotation annotation =
        SearchableAnnotation.fromPegasusAnnotationObject(
            annotationMap, "testField", DataSchema.Type.STRING, "test context");

    assertTrue(annotation.getSearchTier().isPresent());
    assertEquals(annotation.getSearchTier().get(), Integer.valueOf(1));
    assertEquals(annotation.getFieldType(), SearchableAnnotation.FieldType.WORD_GRAM);
  }

  @Test
  public void testSearchTierWithUrnFieldType() {
    // Test that URN is a valid field type for searchTier
    DataMap annotationMap = new DataMap();
    annotationMap.put("fieldType", "URN");
    annotationMap.put("searchTier", 2);

    // Should not throw exception
    SearchableAnnotation annotation =
        SearchableAnnotation.fromPegasusAnnotationObject(
            annotationMap, "testField", DataSchema.Type.STRING, "test context");

    assertTrue(annotation.getSearchTier().isPresent());
    assertEquals(annotation.getSearchTier().get(), Integer.valueOf(2));
    assertEquals(annotation.getFieldType(), SearchableAnnotation.FieldType.URN);
  }

  @Test
  public void testDefaultFieldTypeForString() {
    // Test that string fields without explicit fieldType default to TEXT
    DataMap annotationMap = new DataMap();
    // No fieldType specified
    annotationMap.put("queryByDefault", true);

    SearchableAnnotation annotation =
        SearchableAnnotation.fromPegasusAnnotationObject(
            annotationMap, "testField", DataSchema.Type.STRING, "test context");

    assertEquals(annotation.getFieldType(), SearchableAnnotation.FieldType.TEXT);
  }

  @Test
  public void testDefaultFieldTypeForStringWithSearchTier() {
    // Test that string fields with searchTier and no explicit fieldType default to TEXT
    DataMap annotationMap = new DataMap();
    annotationMap.put("searchTier", 1);
    // No fieldType specified

    SearchableAnnotation annotation =
        SearchableAnnotation.fromPegasusAnnotationObject(
            annotationMap, "testField", DataSchema.Type.STRING, "test context");

    assertEquals(annotation.getFieldType(), SearchableAnnotation.FieldType.TEXT);
    assertTrue(annotation.getSearchTier().isPresent());
    assertEquals(annotation.getSearchTier().get(), Integer.valueOf(1));
  }

  @Test
  public void testDefaultFieldTypeForOtherTypes() {
    // Test default field types for various schema types
    Map<DataSchema.Type, SearchableAnnotation.FieldType> expectedDefaults =
        Map.of(
            DataSchema.Type.INT, SearchableAnnotation.FieldType.COUNT,
            DataSchema.Type.FLOAT, SearchableAnnotation.FieldType.DOUBLE,
            DataSchema.Type.DOUBLE, SearchableAnnotation.FieldType.DOUBLE,
            DataSchema.Type.BOOLEAN, SearchableAnnotation.FieldType.TEXT // falls to default case
            );

    for (Map.Entry<DataSchema.Type, SearchableAnnotation.FieldType> entry :
        expectedDefaults.entrySet()) {
      DataMap annotationMap = new DataMap();
      // No fieldType specified

      SearchableAnnotation annotation =
          SearchableAnnotation.fromPegasusAnnotationObject(
              annotationMap, "testField", entry.getKey(), "test context");

      assertEquals(
          annotation.getFieldType(),
          entry.getValue(),
          "Default field type for " + entry.getKey() + " should be " + entry.getValue());
    }
  }

  @Test
  public void testExplicitFieldTypeOverridesDefault() {
    // Test that explicit fieldType overrides the default
    DataMap annotationMap = new DataMap();
    annotationMap.put("fieldType", "TEXT"); // Explicit fieldType
    annotationMap.put("searchTier", 1);

    SearchableAnnotation annotation =
        SearchableAnnotation.fromPegasusAnnotationObject(
            annotationMap, "testField", DataSchema.Type.STRING, "test context");

    assertEquals(annotation.getFieldType(), SearchableAnnotation.FieldType.TEXT);
    assertTrue(annotation.getSearchTier().isPresent());
  }

  @Test
  public void testSearchTierWithDifferentValues() {
    // Test various valid searchTier values
    int[] validTiers = {1, 2, 5, 10, 100};

    for (int tier : validTiers) {
      DataMap annotationMap = new DataMap();
      annotationMap.put("searchTier", tier);

      SearchableAnnotation annotation =
          SearchableAnnotation.fromPegasusAnnotationObject(
              annotationMap, "testField", DataSchema.Type.STRING, "test context");

      assertTrue(annotation.getSearchTier().isPresent());
      assertEquals(annotation.getSearchTier().get(), Integer.valueOf(tier));
    }
  }

  @Test
  public void testSearchTierWithLabelsCombination() {
    // Test that searchTier works correctly with searchLabel and searchLabel
    DataMap annotationMap = new DataMap();
    annotationMap.put("searchTier", 2);
    annotationMap.put("searchLabel", "priority_sort");

    SearchableAnnotation annotation =
        SearchableAnnotation.fromPegasusAnnotationObject(
            annotationMap, "testField", DataSchema.Type.STRING, "test context");

    assertTrue(annotation.getSearchTier().isPresent());
    assertEquals(annotation.getSearchTier().get(), Integer.valueOf(2));
    assertTrue(annotation.getSearchLabel().isPresent());
    assertEquals(annotation.getSearchLabel().get(), "priority_sort");
  }

  @Test
  public void testEmptyLabelsAreEmpty() {
    // Test that fields without labels have empty optionals
    DataMap annotationMap = new DataMap();
    annotationMap.put("fieldType", "TEXT");
    // No searchLabel or searchLabel specified

    SearchableAnnotation annotation =
        SearchableAnnotation.fromPegasusAnnotationObject(
            annotationMap, "testField", DataSchema.Type.STRING, "test context");

    assertTrue(annotation.getSearchLabel().isEmpty());
    assertTrue(annotation.getSearchLabel().isEmpty());
  }

  @Test
  public void testWhitespaceLabelsAreInvalid() {
    // Test that whitespace-only labels are rejected
    String[] whitespaceLabels = {" ", "  ", "\t", "\n", " \t \n "};

    for (String whitespaceLabel : whitespaceLabels) {
      DataMap annotationMap = new DataMap();
      annotationMap.put("fieldType", "TEXT");
      annotationMap.put("searchLabel", whitespaceLabel);

      assertThrows(
          ModelValidationException.class,
          () ->
              SearchableAnnotation.fromPegasusAnnotationObject(
                  annotationMap, "testField", DataSchema.Type.STRING, "test context"));
    }
  }
}
