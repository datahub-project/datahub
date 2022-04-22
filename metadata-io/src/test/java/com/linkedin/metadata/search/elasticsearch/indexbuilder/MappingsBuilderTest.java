package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import com.google.common.collect.ImmutableMap;
import com.linkedin.metadata.TestEntitySpecBuilder;
import java.util.Map;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class MappingsBuilderTest {

  @Test
  public void testMappingsBuilder() {
    Map<String, Object> result = MappingsBuilder.getMappings(TestEntitySpecBuilder.getSpec());
    assertEquals(result.size(), 1);
    Map<String, Object> properties = (Map<String, Object>) result.get("properties");
    assertEquals(properties.size(), 14);
    assertEquals(properties.get("urn"), ImmutableMap.of("type", "keyword"));
    assertTrue(properties.containsKey("browsePaths"));
    // KEYWORD
    Map<String, Object> keyPart3Field = (Map<String, Object>) properties.get("keyPart3");
    assertEquals(keyPart3Field.get("type"), "keyword");
    assertEquals(keyPart3Field.get("normalizer"), "keyword_normalizer");
    Map<String, Object> keyPart3FieldSubfields = (Map<String, Object>) keyPart3Field.get("fields");
    assertEquals(keyPart3FieldSubfields.size(), 1);
    assertTrue(keyPart3FieldSubfields.containsKey("keyword"));
    Map<String, Object> customPropertiesField = (Map<String, Object>) properties.get("customProperties");
    assertEquals(customPropertiesField.get("type"), "keyword");
    assertEquals(customPropertiesField.get("normalizer"), "keyword_normalizer");
    Map<String, Object> customPropertiesFieldSubfields = (Map<String, Object>) customPropertiesField.get("fields");
    assertEquals(customPropertiesFieldSubfields.size(), 1);
    assertTrue(customPropertiesFieldSubfields.containsKey("keyword"));
    // TEXT
    Map<String, Object> nestedArrayStringField = (Map<String, Object>) properties.get("nestedArrayStringField");
    assertEquals(nestedArrayStringField.get("type"), "keyword");
    assertEquals(nestedArrayStringField.get("normalizer"), "keyword_normalizer");
    Map<String, Object> nestedArrayStringFieldSubfields = (Map<String, Object>) nestedArrayStringField.get("fields");
    assertEquals(nestedArrayStringFieldSubfields.size(), 2);
    assertTrue(nestedArrayStringFieldSubfields.containsKey("delimited"));
    assertTrue(nestedArrayStringFieldSubfields.containsKey("keyword"));
    Map<String, Object> nestedArrayArrayField = (Map<String, Object>) properties.get("nestedArrayArrayField");
    assertEquals(nestedArrayArrayField.get("type"), "keyword");
    assertEquals(nestedArrayArrayField.get("normalizer"), "keyword_normalizer");
    Map<String, Object> nestedArrayArrayFieldSubfields = (Map<String, Object>) nestedArrayArrayField.get("fields");
    assertEquals(nestedArrayArrayFieldSubfields.size(), 2);
    assertTrue(nestedArrayArrayFieldSubfields.containsKey("delimited"));
    assertTrue(nestedArrayArrayFieldSubfields.containsKey("keyword"));

    // TEXT with addToFilters
    Map<String, Object> textField = (Map<String, Object>) properties.get("textFieldOverride");
    assertEquals(textField.get("type"), "keyword");
    assertEquals(textField.get("normalizer"), "keyword_normalizer");
    Map<String, Object> textFieldSubfields = (Map<String, Object>) textField.get("fields");
    assertEquals(textFieldSubfields.size(), 2);
    assertTrue(textFieldSubfields.containsKey("delimited"));
    assertTrue(textFieldSubfields.containsKey("keyword"));

    // TEXT_PARTIAL
    Map<String, Object> textArrayField = (Map<String, Object>) properties.get("textArrayField");
    assertEquals(textArrayField.get("type"), "keyword");
    assertEquals(textArrayField.get("normalizer"), "keyword_normalizer");
    Map<String, Object> textArrayFieldSubfields = (Map<String, Object>) textArrayField.get("fields");
    assertEquals(textArrayFieldSubfields.size(), 3);
    assertTrue(textArrayFieldSubfields.containsKey("delimited"));
    assertTrue(textArrayFieldSubfields.containsKey("ngram"));
    assertTrue(textArrayFieldSubfields.containsKey("keyword"));

    // URN
    Map<String, Object> foreignKey = (Map<String, Object>) properties.get("foreignKey");
    assertEquals(foreignKey.get("type"), "text");
    assertEquals(foreignKey.get("analyzer"), "urn_component");
    Map<String, Object> foreignKeySubfields = (Map<String, Object>) foreignKey.get("fields");
    assertEquals(foreignKeySubfields.size(), 1);
    assertTrue(foreignKeySubfields.containsKey("keyword"));

    // URN_PARTIAL
    Map<String, Object> nestedForeignKey = (Map<String, Object>) properties.get("nestedForeignKey");
    assertEquals(nestedForeignKey.get("type"), "text");
    assertEquals(nestedForeignKey.get("analyzer"), "urn_component");
    Map<String, Object> nestedForeignKeySubfields = (Map<String, Object>) nestedForeignKey.get("fields");
    assertEquals(nestedForeignKeySubfields.size(), 2);
    assertTrue(nestedForeignKeySubfields.containsKey("keyword"));
    assertTrue(nestedForeignKeySubfields.containsKey("ngram"));

    // Scores
    Map<String, Object> feature1 = (Map<String, Object>) properties.get("feature1");
    assertEquals(feature1.get("type"), "double");
    Map<String, Object> feature2 = (Map<String, Object>) properties.get("feature2");
    assertEquals(feature2.get("type"), "double");
  }
}
