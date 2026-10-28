package com.linkedin.metadata.models.annotation;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.data.DataList;
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
    DataMap v3 = new DataMap();
    v3.put("searchLabel", "priorityField");
    DataMap annotationMap = new DataMap();
    annotationMap.put("fieldType", "TEXT");
    annotationMap.put("v3", v3);
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

    assertEquals(annotation.resolvedForV3().getSearchLabel().get(), "priorityField");
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
    assertTrue(annotation.getSearchLabel().isEmpty());
    assertTrue(annotation.getSearchLabel().isEmpty());
  }

  @Test
  public void testSearchableAnnotationWithPartialNewFields() {
    // Create a test annotation map with only some of the new fields
    DataMap v3 = new DataMap();
    v3.put("searchLabel", "relevance");
    DataMap annotationMap = new DataMap();
    annotationMap.put("fieldType", "TEXT_PARTIAL");
    annotationMap.put("v3", v3);

    // Create SearchableAnnotation from the map
    SearchableAnnotation annotation =
        SearchableAnnotation.fromPegasusAnnotationObject(
            annotationMap, "partialField", DataSchema.Type.STRING, "partial test context");

    // Verify existing fields still work
    assertEquals(annotation.getFieldName(), "partialField");
    assertEquals(annotation.getFieldType(), SearchableAnnotation.FieldType.TEXT_PARTIAL);

    assertEquals(annotation.resolvedForV3().getSearchLabel().get(), "relevance");
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
      SearchableAnnotation annotation =
          SearchableAnnotation.fromPegasusAnnotationObject(
              labeled("TEXT", validName),
              "testField",
              DataSchema.Type.STRING,
              "test context for " + validName);
      assertEquals(annotation.resolvedForV3().getSearchLabel().get(), validName);
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
      assertThrows(
          ModelValidationException.class,
          () ->
              SearchableAnnotation.fromPegasusAnnotationObject(
                  labeled("TEXT", invalidName),
                  "testField",
                  DataSchema.Type.STRING,
                  "test context for " + invalidName));
    }
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

    SearchableAnnotation annotation =
        SearchableAnnotation.fromPegasusAnnotationObject(
            annotationMap, "testField", DataSchema.Type.STRING, "test context");

    assertEquals(annotation.getFieldType(), SearchableAnnotation.FieldType.TEXT);
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
      assertThrows(
          ModelValidationException.class,
          () ->
              SearchableAnnotation.fromPegasusAnnotationObject(
                  labeled("TEXT", whitespaceLabel),
                  "testField",
                  DataSchema.Type.STRING,
                  "test context"));
    }
  }

  @Test
  public void testInvalidValueTypeProvided() {
    // Test that non-Map objects throw ModelValidationException with updated error message format
    String testContext = "testField";

    // Test with String instead of Map
    assertThrows(
        ModelValidationException.class,
        () -> {
          SearchableAnnotation.fromPegasusAnnotationObject(
              "invalidString", "testField", DataSchema.Type.STRING, testContext);
        });

    // Test with Integer instead of Map
    assertThrows(
        ModelValidationException.class,
        () -> {
          SearchableAnnotation.fromPegasusAnnotationObject(
              123, "testField", DataSchema.Type.STRING, testContext);
        });

    // Test with List instead of Map
    assertThrows(
        ModelValidationException.class,
        () -> {
          SearchableAnnotation.fromPegasusAnnotationObject(
              java.util.List.of("item1", "item2"),
              "testField",
              DataSchema.Type.STRING,
              testContext);
        });
  }

  @Test
  public void testInvalidValueTypeErrorMessageFormat() {
    // Test that the error message includes the actual class type in the format:
    // "Failed to validate @Searchable annotation declared at {context}: Invalid value type
    // {actualType} provided (Expected Map)"
    String testContext = "testField";

    try {
      SearchableAnnotation.fromPegasusAnnotationObject(
          "invalidString", "testField", DataSchema.Type.STRING, testContext);
      assert false : "Expected ModelValidationException to be thrown";
    } catch (ModelValidationException e) {
      String expectedMessage =
          String.format(
              "Failed to validate @%s annotation declared at %s: Invalid value type %s provided (Expected Map)",
              "Searchable", testContext, String.class);
      assert e.getMessage().contains(expectedMessage)
          : "Error message should contain the actual class type. Expected: "
              + expectedMessage
              + ", Actual: "
              + e.getMessage();
    }

    try {
      SearchableAnnotation.fromPegasusAnnotationObject(
          123, "testField", DataSchema.Type.STRING, testContext);
      assert false : "Expected ModelValidationException to be thrown";
    } catch (ModelValidationException e) {
      String expectedMessage =
          String.format(
              "Failed to validate @%s annotation declared at %s: Invalid value type %s provided (Expected Map)",
              "Searchable", testContext, Integer.class);
      assert e.getMessage().contains(expectedMessage)
          : "Error message should contain the actual class type. Expected: "
              + expectedMessage
              + ", Actual: "
              + e.getMessage();
    }
  }

  @Test
  public void testSearchIndexedWithInvalidFieldType() {
    // Test that searchIndexed=true with invalid field types throws ModelValidationException
    String[] invalidFieldTypes = {
      "COUNT",
      "BOOLEAN",
      "DATETIME",
      "DOUBLE",
      "OBJECT",
      "BROWSE_PATH",
      "URN",
      "URN_PARTIAL",
      "TEXT_PARTIAL",
      "WORD_GRAM"
    };

    for (String invalidFieldType : invalidFieldTypes) {
      DataMap annotationMap = new DataMap();
      annotationMap.put("fieldType", invalidFieldType);
      annotationMap.put("searchIndexed", true);

      assertThrows(
          ModelValidationException.class,
          () -> {
            SearchableAnnotation.fromPegasusAnnotationObject(
                annotationMap, "testField", DataSchema.Type.STRING, "test context");
          });
    }
  }

  @Test
  public void testSearchIndexedWithValidFieldTypes() {
    // Test that searchIndexed=true with valid field types (KEYWORD and TEXT) works correctly
    String[] validFieldTypes = {"KEYWORD", "TEXT"};

    for (String validFieldType : validFieldTypes) {
      DataMap annotationMap = new DataMap();
      annotationMap.put("fieldType", validFieldType);
      annotationMap.put("searchIndexed", true);

      // Should not throw exception
      SearchableAnnotation annotation =
          SearchableAnnotation.fromPegasusAnnotationObject(
              annotationMap, "testField", DataSchema.Type.STRING, "test context");

      assertTrue(annotation.getSearchIndexed().isPresent());
      assertTrue(annotation.getSearchIndexed().get());
    }
  }

  @Test
  public void testSearchIndexedFalse() {
    DataMap annotationMap = new DataMap();
    annotationMap.put("fieldType", "TEXT");
    annotationMap.put("searchIndexed", false);

    // Should not throw exception
    SearchableAnnotation annotation =
        SearchableAnnotation.fromPegasusAnnotationObject(
            annotationMap, "testField", DataSchema.Type.STRING, "test context");

    assertTrue(annotation.getSearchIndexed().isPresent());
    assertFalse(annotation.getSearchIndexed().get());
  }

  @Test
  public void testSearchIndexedErrorMessageFormat() {
    DataMap invalidFieldTypeMap = new DataMap();
    invalidFieldTypeMap.put("fieldType", "TEXT_PARTIAL");
    invalidFieldTypeMap.put("searchIndexed", true);

    try {
      SearchableAnnotation.fromPegasusAnnotationObject(
          invalidFieldTypeMap, "testField", DataSchema.Type.STRING, "testField");
      assert false : "Expected ModelValidationException to be thrown";
    } catch (ModelValidationException e) {
      String expectedMessage =
          String.format(
              "Failed to validate @%s annotation declared at %s: searchIndexed can only be used with KEYWORD or TEXT field types, but was %s",
              "Searchable", "testField", SearchableAnnotation.FieldType.TEXT_PARTIAL);
      assert e.getMessage().contains(expectedMessage)
          : "Error message should match expected format. Expected: "
              + expectedMessage
              + ", Actual: "
              + e.getMessage();
    }
  }

  @Test
  public void testEntityFieldNameLengthValidation() {
    // Test that entityFieldName longer than 255 characters throws ModelValidationException
    String longFieldName = "a".repeat(256); // 256 characters

    DataMap annotationMap = new DataMap();
    annotationMap.put("fieldType", "TEXT");
    annotationMap.put("entityFieldName", longFieldName);

    assertThrows(
        ModelValidationException.class,
        () -> {
          SearchableAnnotation.fromPegasusAnnotationObject(
              annotationMap, "testField", DataSchema.Type.STRING, "test context");
        });
  }

  @Test
  public void testEntityFieldNameReservedFieldValidation() {
    // Test that reserved Elasticsearch field names are rejected
    String[] reservedFields = {
      "_id",
      "_index",
      "_type",
      "_source",
      "_all",
      "_routing",
      "_parent",
      "_timestamp",
      "_ttl",
      "_version",
      "_score",
      "_explanation",
      "_shards",
      "_nodes",
      "_cluster_name",
      "_cluster_uuid",
      "_name",
      "_uuid",
      "_version_type",
      "_seq_no",
      "_primary_term"
    };

    for (String reservedField : reservedFields) {
      DataMap annotationMap = new DataMap();
      annotationMap.put("fieldType", "TEXT");
      annotationMap.put("entityFieldName", reservedField);

      assertThrows(
          ModelValidationException.class,
          () -> {
            SearchableAnnotation.fromPegasusAnnotationObject(
                annotationMap, "testField", DataSchema.Type.STRING, "test context");
          });
    }
  }

  @Test
  public void testEntityFieldNameDotPatternValidation() {
    // Test that entityFieldName with problematic dot patterns are rejected
    String[] invalidDotPatterns = {
      ".field", // starts with dot
      "field.", // ends with dot
      "field..name", // consecutive dots
      "field...name", // multiple consecutive dots
      ".field.name.", // starts and ends with dots
      "field..name..test" // multiple consecutive dots
    };

    for (String invalidPattern : invalidDotPatterns) {
      DataMap annotationMap = new DataMap();
      annotationMap.put("fieldType", "TEXT");
      annotationMap.put("entityFieldName", invalidPattern);

      assertThrows(
          ModelValidationException.class,
          () -> {
            SearchableAnnotation.fromPegasusAnnotationObject(
                annotationMap, "testField", DataSchema.Type.STRING, "test context");
          });
    }
  }

  @Test
  public void testEntityFieldNameLowercaseValidation() {
    // Test that entityFieldName with uppercase letters are rejected
    String[] invalidUppercaseNames = {
      "FieldName", // mixed case
      "FIELDNAME", // all uppercase
      "fieldName", // camelCase
      "Field_Name", // mixed case with underscore
      "field-Name", // mixed case with hyphen
      "Field.name" // mixed case with dot
    };

    for (String invalidName : invalidUppercaseNames) {
      DataMap annotationMap = new DataMap();
      annotationMap.put("fieldType", "TEXT");
      annotationMap.put("entityFieldName", invalidName);

      assertThrows(
          ModelValidationException.class,
          () -> {
            SearchableAnnotation.fromPegasusAnnotationObject(
                annotationMap, "testField", DataSchema.Type.STRING, "test context");
          });
    }
  }

  @Test
  public void testEntityFieldNameValidValues() {
    // Test that valid entityFieldName values work correctly
    String[] validNames = {
      "fieldname", // simple lowercase
      "field_name", // lowercase with underscore
      "field-name", // lowercase with hyphen
      "field.name", // lowercase with dot
      "field_name123", // lowercase with numbers
      "field-name.test", // multiple valid separators
      "a", // single character
      "field_name_test", // multiple underscores
      "field-name-test" // multiple hyphens
    };

    for (String validName : validNames) {
      DataMap annotationMap = new DataMap();
      annotationMap.put("fieldType", "TEXT");
      annotationMap.put("entityFieldName", validName);

      // Should not throw exception
      SearchableAnnotation annotation =
          SearchableAnnotation.fromPegasusAnnotationObject(
              annotationMap, "testField", DataSchema.Type.STRING, "test context");

      assertTrue(annotation.getEntityFieldName().isPresent());
      assertEquals(annotation.getEntityFieldName().get(), validName);
    }
  }

  @Test
  public void testEntityFieldNameMaxLengthBoundary() {
    // Test the boundary case - exactly 255 characters should be valid
    String maxLengthFieldName = "a".repeat(255); // exactly 255 characters

    DataMap annotationMap = new DataMap();
    annotationMap.put("fieldType", "TEXT");
    annotationMap.put("entityFieldName", maxLengthFieldName);

    // Should not throw exception
    SearchableAnnotation annotation =
        SearchableAnnotation.fromPegasusAnnotationObject(
            annotationMap, "testField", DataSchema.Type.STRING, "test context");

    assertTrue(annotation.getEntityFieldName().isPresent());
    assertEquals(annotation.getEntityFieldName().get(), maxLengthFieldName);
  }

  @Test
  public void testEagerGlobalOrdinalsWithInvalidFieldTypes() {
    // Test that eagerGlobalOrdinals=true with invalid field types throws ModelValidationException
    String[] invalidFieldTypes = {
      "TEXT",
      "TEXT_PARTIAL",
      "BROWSE_PATH",
      "BOOLEAN",
      "COUNT",
      "DATETIME",
      "OBJECT",
      "BROWSE_PATH_V2",
      "WORD_GRAM",
      "DOUBLE",
      "MAP_ARRAY"
    };

    for (String invalidFieldType : invalidFieldTypes) {
      DataMap annotationMap = new DataMap();
      annotationMap.put("fieldType", invalidFieldType);
      annotationMap.put("eagerGlobalOrdinals", true);

      assertThrows(
          ModelValidationException.class,
          () -> {
            SearchableAnnotation.fromPegasusAnnotationObject(
                annotationMap, "testField", DataSchema.Type.STRING, "test context");
          });
    }
  }

  @Test
  public void testEagerGlobalOrdinalsWithValidFieldTypes() {
    // Test that eagerGlobalOrdinals=true with valid field types works correctly
    String[] validFieldTypes = {"KEYWORD", "URN", "URN_PARTIAL"};

    for (String validFieldType : validFieldTypes) {
      DataMap annotationMap = new DataMap();
      annotationMap.put("fieldType", validFieldType);
      annotationMap.put("eagerGlobalOrdinals", true);

      // Should not throw exception
      SearchableAnnotation annotation =
          SearchableAnnotation.fromPegasusAnnotationObject(
              annotationMap, "testField", DataSchema.Type.STRING, "test context");

      assertTrue(annotation.getEagerGlobalOrdinals().isPresent());
      assertTrue(annotation.getEagerGlobalOrdinals().get());
    }
  }

  @Test
  public void testEagerGlobalOrdinalsFalse() {
    // Test that eagerGlobalOrdinals=false works with any field type
    String[] fieldTypes = {
      "KEYWORD", "TEXT", "TEXT_PARTIAL", "BROWSE_PATH", "URN", "URN_PARTIAL",
      "BOOLEAN", "COUNT", "DATETIME", "OBJECT", "BROWSE_PATH_V2", "WORD_GRAM",
      "DOUBLE", "MAP_ARRAY"
    };

    for (String fieldType : fieldTypes) {
      DataMap annotationMap = new DataMap();
      annotationMap.put("fieldType", fieldType);
      annotationMap.put("eagerGlobalOrdinals", false);

      // Should not throw exception
      SearchableAnnotation annotation =
          SearchableAnnotation.fromPegasusAnnotationObject(
              annotationMap, "testField", DataSchema.Type.STRING, "test context");

      assertTrue(annotation.getEagerGlobalOrdinals().isPresent());
      assertFalse(annotation.getEagerGlobalOrdinals().get());
    }
  }

  @Test
  public void testEagerGlobalOrdinalsNotSpecified() {
    // Test that when eagerGlobalOrdinals is not specified, it works with any field type
    String[] fieldTypes = {
      "KEYWORD", "TEXT", "TEXT_PARTIAL", "BROWSE_PATH", "URN", "URN_PARTIAL",
      "BOOLEAN", "COUNT", "DATETIME", "OBJECT", "BROWSE_PATH_V2", "WORD_GRAM",
      "DOUBLE", "MAP_ARRAY"
    };

    for (String fieldType : fieldTypes) {
      DataMap annotationMap = new DataMap();
      annotationMap.put("fieldType", fieldType);
      // No eagerGlobalOrdinals specified

      // Should not throw exception
      SearchableAnnotation annotation =
          SearchableAnnotation.fromPegasusAnnotationObject(
              annotationMap, "testField", DataSchema.Type.STRING, "test context");

      assertFalse(annotation.getEagerGlobalOrdinals().isPresent());
    }
  }

  @Test
  public void testEagerGlobalOrdinalsErrorMessageFormat() {
    // Test the specific error message format for eagerGlobalOrdinals validation
    DataMap annotationMap = new DataMap();
    annotationMap.put("fieldType", "TEXT");
    annotationMap.put("eagerGlobalOrdinals", true);

    try {
      SearchableAnnotation.fromPegasusAnnotationObject(
          annotationMap, "testField", DataSchema.Type.STRING, "testField");
      assert false : "Expected ModelValidationException to be thrown";
    } catch (ModelValidationException e) {
      String expectedMessage =
          String.format(
              "Failed to validate @%s annotation declared at %s: eagerGlobalOrdinals can only be true for KEYWORD, URN, or URN_PARTIAL field types, but was %s",
              "Searchable", "testField", SearchableAnnotation.FieldType.TEXT);
      assert e.getMessage().contains(expectedMessage)
          : "Error message should match expected format. Expected: "
              + expectedMessage
              + ", Actual: "
              + e.getMessage();
    }
  }

  @Test
  public void testSearchLabelComesFromV3() {
    DataMap overlay = new DataMap();
    overlay.put("searchLabel", "name");
    DataMap annotationMap = new DataMap();
    annotationMap.put("fieldType", "WORD_GRAM");
    annotationMap.put("searchLabel", "entityName");
    annotationMap.put("boostScore", 10.0);
    annotationMap.put("includeSystemModifiedAt", true);
    annotationMap.put("systemModifiedAtFieldName", "termsModifiedAt");
    annotationMap.put("v3", overlay);

    SearchableAnnotation annotation =
        SearchableAnnotation.fromPegasusAnnotationObject(
            annotationMap, "name", DataSchema.Type.STRING, "testField");

    assertTrue(annotation.getSearchLabel().isEmpty());
    assertEquals(annotation.getV3SearchLabel().get(), "name");
    assertTrue(annotation.isIncludeSystemModifiedAt());
    assertEquals(annotation.getSystemModifiedAtFieldName().get(), "termsModifiedAt");
    assertTrue(annotation.isV3Searchable());
    assertEquals(annotation.resolvedForV3().getSearchLabel().get(), "name");
    assertTrue(annotation.resolvedForV3().isIncludeSystemModifiedAt());
  }

  @Test
  public void testV3OverlayTurnsOffTheSystemModifiedSidecar() {
    DataMap overlay = new DataMap();
    overlay.put("includeSystemModifiedAt", false);
    DataMap annotationMap = new DataMap();
    annotationMap.put("fieldType", "URN");
    annotationMap.put("includeSystemModifiedAt", true);
    annotationMap.put("systemModifiedAtFieldName", "termsModifiedAt");
    annotationMap.put("v3", overlay);

    SearchableAnnotation annotation =
        SearchableAnnotation.fromPegasusAnnotationObject(
            annotationMap, "glossaryTerms", DataSchema.Type.STRING, "testField");

    assertTrue(annotation.isIncludeSystemModifiedAt());
    assertEquals(annotation.getSystemModifiedAtFieldName().get(), "termsModifiedAt");
    assertFalse(annotation.resolvedForV3().isIncludeSystemModifiedAt());
    assertFalse(annotation.resolvedForV3().getSystemModifiedAtFieldName().isPresent());
  }

  @Test
  public void testV3OverlayOverridesQueryByDefault() {
    DataMap overlay = new DataMap();
    overlay.put("queryByDefault", false);
    DataMap annotationMap = new DataMap();
    annotationMap.put("fieldType", "URN");
    annotationMap.put("queryByDefault", true);
    annotationMap.put("v3", overlay);

    SearchableAnnotation annotation =
        SearchableAnnotation.fromPegasusAnnotationObject(
            annotationMap, "foreignKey", DataSchema.Type.STRING, "testField");

    assertTrue(annotation.isQueryByDefault());
    assertFalse(annotation.resolvedForV3().isQueryByDefault());
  }

  @Test
  public void testUnsetV3QueryByDefaultStaysOutOfTheDefaultQuery() {
    DataMap annotationMap = new DataMap();
    annotationMap.put("fieldType", "TEXT");
    annotationMap.put("queryByDefault", true);

    SearchableAnnotation annotation =
        SearchableAnnotation.fromPegasusAnnotationObject(
            annotationMap, "title", DataSchema.Type.STRING, "testField");

    assertTrue(annotation.isQueryByDefault());
    assertFalse(annotation.resolvedForV3().isQueryByDefault());
  }

  @Test
  public void testV3OverlayCanDisableSearch() {
    DataMap overlay = new DataMap();
    overlay.put("enabled", false);
    DataMap annotationMap = new DataMap();
    annotationMap.put("fieldType", "TEXT");
    annotationMap.put("v3", overlay);

    SearchableAnnotation annotation =
        SearchableAnnotation.fromPegasusAnnotationObject(
            annotationMap, "hidden", DataSchema.Type.STRING, "testField");

    assertFalse(annotation.isV3Searchable());
    assertTrue(annotation.isQueryByDefault());
  }

  @Test
  public void testSearchLabelLeavesAspectSourceUnindexedUnlessReenabled() {
    SearchableAnnotation labeled =
        SearchableAnnotation.fromPegasusAnnotationObject(
            labeled("KEYWORD", "name"), "name", DataSchema.Type.STRING, "testField");
    assertFalse(labeled.aspectIndexed());
    assertFalse(labeled.aspectDocValues());
    assertFalse(labeled.resolvedForV3().aspectIndexed());
    assertFalse(labeled.resolvedForV3().aspectDocValues());

    DataMap indexOnlyMap = labeled("KEYWORD", "name");
    DataMap indexOnlySource = new DataMap();
    indexOnlySource.put("index", true);
    ((DataMap) indexOnlyMap.get("v3")).put("source", indexOnlySource);
    SearchableAnnotation indexOnly =
        SearchableAnnotation.fromPegasusAnnotationObject(
            indexOnlyMap, "name", DataSchema.Type.STRING, "testField");
    assertTrue(indexOnly.aspectIndexed());
    assertFalse(indexOnly.aspectDocValues());

    DataMap bothMap = labeled("KEYWORD", "name");
    DataMap bothSource = new DataMap();
    bothSource.put("index", true);
    bothSource.put("docValues", true);
    ((DataMap) bothMap.get("v3")).put("source", bothSource);
    SearchableAnnotation both =
        SearchableAnnotation.fromPegasusAnnotationObject(
            bothMap, "name", DataSchema.Type.STRING, "testField");
    assertTrue(both.aspectIndexed());
    assertTrue(both.aspectDocValues());
    assertTrue(both.resolvedForV3().aspectIndexed());
    assertTrue(both.resolvedForV3().aspectDocValues());

    DataMap docValuesOnlyMap = labeled("COUNT", "rowCount");
    DataMap docValuesOnlySource = new DataMap();
    docValuesOnlySource.put("docValues", true);
    ((DataMap) docValuesOnlyMap.get("v3")).put("source", docValuesOnlySource);
    SearchableAnnotation docValuesOnly =
        SearchableAnnotation.fromPegasusAnnotationObject(
            docValuesOnlyMap, "rowCount", DataSchema.Type.INT, "testField");
    assertFalse(docValuesOnly.aspectIndexed());
    assertTrue(docValuesOnly.aspectDocValues());

    DataMap unlabeled = new DataMap();
    unlabeled.put("fieldType", "BOOLEAN");
    SearchableAnnotation plain =
        SearchableAnnotation.fromPegasusAnnotationObject(
            unlabeled, "removed", DataSchema.Type.BOOLEAN, "testField");
    assertFalse(plain.aspectIndexed());
    assertFalse(plain.aspectDocValues());

    DataMap entityField = new DataMap();
    entityField.put("fieldType", "KEYWORD");
    DataMap entityFieldV3 = new DataMap();
    entityFieldV3.put("entityFieldName", "name");
    entityField.put("v3", entityFieldV3);
    SearchableAnnotation consolidated =
        SearchableAnnotation.fromPegasusAnnotationObject(
            entityField, "title", DataSchema.Type.STRING, "testField");
    assertFalse(consolidated.aspectIndexed());
    assertFalse(consolidated.aspectDocValues());

    DataMap objectField = new DataMap();
    objectField.put("fieldType", "OBJECT");
    SearchableAnnotation objectAnnotation =
        SearchableAnnotation.fromPegasusAnnotationObject(
            objectField, "ownerTypes", DataSchema.Type.MAP, "testField");
    assertTrue(objectAnnotation.aspectIndexed());
    assertTrue(objectAnnotation.aspectDocValues());

    DataMap searchOnly = labeled("KEYWORD", "name");
    DataMap searchOnlyRoot = new DataMap();
    searchOnlyRoot.put("type", "KEYWORD");
    searchOnlyRoot.put("index", false);
    searchOnlyRoot.put("docValues", false);
    DataList searchOnlyMappings = new DataList();
    searchOnlyMappings.add(searchOnlyRoot);
    ((DataMap) searchOnly.get("v3")).put("mappings", searchOnlyMappings);
    SearchableAnnotation searchOnlyLabel =
        SearchableAnnotation.fromPegasusAnnotationObject(
            searchOnly, "name", DataSchema.Type.STRING, "testField");
    assertFalse(searchOnlyLabel.aspectIndexed());
    DataMap reenabled = new DataMap();
    reenabled.put("index", true);
    ((DataMap) searchOnly.get("v3")).put("source", reenabled);
    SearchableAnnotation explicitOverSearchOnly =
        SearchableAnnotation.fromPegasusAnnotationObject(
            searchOnly, "name", DataSchema.Type.STRING, "testField");
    assertTrue(explicitOverSearchOnly.aspectIndexed());
    assertFalse(explicitOverSearchOnly.aspectDocValues());

    DataMap objectUnindexed = new DataMap();
    objectUnindexed.put("fieldType", "OBJECT");
    DataMap objectV3 = new DataMap();
    DataMap objectRoot = new DataMap();
    objectRoot.put("type", "OBJECT");
    objectRoot.put("index", false);
    objectRoot.put("docValues", true);
    DataList objectMappings = new DataList();
    objectMappings.add(objectRoot);
    objectV3.put("mappings", objectMappings);
    objectUnindexed.put("v3", objectV3);
    SearchableAnnotation followsRoot =
        SearchableAnnotation.fromPegasusAnnotationObject(
            objectUnindexed, "ownerTypes", DataSchema.Type.MAP, "testField");
    assertFalse(followsRoot.aspectIndexed());
    assertTrue(followsRoot.aspectDocValues());

    DataMap badSource = labeled("KEYWORD", "name");
    ((DataMap) badSource.get("v3")).put("source", "nope");
    assertThrows(
        ModelValidationException.class,
        () ->
            SearchableAnnotation.fromPegasusAnnotationObject(
                badSource, "name", DataSchema.Type.STRING, "testField"));

    DataMap badFlag = labeled("KEYWORD", "name");
    DataMap badFlagSource = new DataMap();
    badFlagSource.put("index", "yes");
    ((DataMap) badFlag.get("v3")).put("source", badFlagSource);
    assertThrows(
        ModelValidationException.class,
        () ->
            SearchableAnnotation.fromPegasusAnnotationObject(
                badFlag, "name", DataSchema.Type.STRING, "testField"));
  }

  @Test
  public void testCopyToOtherKeepsTheRoot() {
    DataMap v3 = new DataMap();
    v3.put("copyToOther", true);
    v3.put("queryByDefault", false);
    DataMap annotationMap = new DataMap();
    annotationMap.put("fieldType", "URN");
    annotationMap.put("v3", v3);

    SearchableAnnotation annotation =
        SearchableAnnotation.fromPegasusAnnotationObject(
            annotationMap, "schemaField", DataSchema.Type.STRING, "test context");

    assertTrue(annotation.isCopyToOther());
    assertTrue(annotation.resolvedForV3().isCopyToOther());
    assertFalse(annotation.resolvedForV3().isQueryByDefault());
    assertTrue(annotation.resolvedForV3().getSearchLabel().isEmpty());
    assertEquals(annotation.resolvedForV3().getFieldName(), "schemaField");
  }

  @Test
  public void testCopyToOtherAllowsLabelThatIsTheRoot() {
    DataMap v3 = new DataMap();
    v3.put("copyToOther", true);
    v3.put("queryByDefault", false);
    v3.put("searchLabel", "platform");
    DataMap annotationMap = new DataMap();
    annotationMap.put("fieldType", "URN");
    annotationMap.put("v3", v3);

    SearchableAnnotation annotation =
        SearchableAnnotation.fromPegasusAnnotationObject(
            annotationMap, "platform", DataSchema.Type.STRING, "test context");

    assertTrue(annotation.isCopyToOther());
    assertEquals(annotation.resolvedForV3().getSearchLabel().get(), "platform");
  }

  @Test
  public void testCopyToOtherRejectsSearchLabel() {
    DataMap v3 = new DataMap();
    v3.put("copyToOther", true);
    v3.put("searchLabel", "name");
    DataMap annotationMap = new DataMap();
    annotationMap.put("fieldType", "URN");
    annotationMap.put("v3", v3);

    assertThrows(
        ModelValidationException.class,
        () ->
            SearchableAnnotation.fromPegasusAnnotationObject(
                annotationMap, "schemaField", DataSchema.Type.STRING, "test context"));
  }

  @Test
  public void testV3HasValuesFieldNameKeepsTheTopLevelName() {
    DataMap v3 = new DataMap();
    v3.put("searchLabel", "parent");
    v3.put("queryByDefault", false);
    v3.put("hasValuesFieldName", "hasParent");
    DataMap annotationMap = new DataMap();
    annotationMap.put("fieldName", "parentNode");
    annotationMap.put("fieldType", "URN");
    annotationMap.put("hasValuesFieldName", "hasParentNode");
    annotationMap.put("v3", v3);

    SearchableAnnotation annotation =
        SearchableAnnotation.fromPegasusAnnotationObject(
            annotationMap, "parentNode", DataSchema.Type.STRING, "test context");
    SearchableAnnotation resolved = annotation.resolvedForV3();

    assertEquals(resolved.getHasValuesFieldName().get(), "hasParentNode");
    assertEquals(resolved.getV3HasValuesFieldName().get(), "hasParent");
    assertEquals(resolved.indexedHasValuesFieldName().get(), "hasParentNode");
    assertEquals(resolved.indexedSharedHasValuesFieldName().get(), "hasParent");
  }

  private static DataMap labeled(String fieldType, String searchLabel) {
    DataMap v3 = new DataMap();
    v3.put("searchLabel", searchLabel);
    DataMap annotationMap = new DataMap();
    annotationMap.put("fieldType", fieldType);
    annotationMap.put("v3", v3);
    return annotationMap;
  }
}
