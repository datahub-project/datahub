package com.linkedin.metadata.search.indexbuilder;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.*;

import com.datahub.test.TestRefEntity;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.metadata.TestEntitySpecBuilder;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.EntitySpecBuilder;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.MappingsBuilder;
import com.linkedin.metadata.search.query.request.TestSearchFieldConfig;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class MappingsBuilderTest {

  @Test
  public void testMappingsBuilder() {
    Map<String, Object> result =
        MappingsBuilder.getMappings(mock(EntityRegistry.class), TestEntitySpecBuilder.getSpec());
    assertEquals(result.size(), 1);
    Map<String, Object> properties = (Map<String, Object>) result.get("properties");
    assertEquals(properties.size(), 27);
    assertEquals(
        properties.get("urn"),
        ImmutableMap.of(
            "type",
            "keyword",
            "fields",
            ImmutableMap.of(
                "delimited",
                ImmutableMap.of(
                    "type",
                    "text",
                    "analyzer",
                    "urn_component",
                    "search_analyzer",
                    "query_urn_component",
                    "search_quote_analyzer",
                    "quote_analyzer"),
                "ngram",
                ImmutableMap.of(
                    "type",
                    "search_as_you_type",
                    "max_shingle_size",
                    "4",
                    "doc_values",
                    "false",
                    "analyzer",
                    "partial_urn_component"))));
    assertEquals(properties.get("runId"), ImmutableMap.of("type", "keyword"));
    assertEquals(properties.get("systemCreated"), ImmutableMap.of("type", "date"));
    assertTrue(properties.containsKey("browsePaths"));
    assertTrue(properties.containsKey("browsePathV2"));
    assertTrue(properties.containsKey("removed"));
    // KEYWORD
    Map<String, Object> keyPart3Field = (Map<String, Object>) properties.get("keyPart3");
    assertEquals(keyPart3Field.get("type"), "keyword");
    assertEquals(keyPart3Field.get("normalizer"), "keyword_normalizer");
    Map<String, Object> keyPart3FieldSubfields = (Map<String, Object>) keyPart3Field.get("fields");
    assertEquals(keyPart3FieldSubfields.size(), 1);
    assertTrue(keyPart3FieldSubfields.containsKey("keyword"));
    // TEXT
    Map<String, Object> nestedArrayStringField =
        (Map<String, Object>) properties.get("nestedArrayStringField");
    assertEquals(nestedArrayStringField.get("type"), "keyword");
    assertEquals(nestedArrayStringField.get("normalizer"), "keyword_normalizer");
    Map<String, Object> nestedArrayStringFieldSubfields =
        (Map<String, Object>) nestedArrayStringField.get("fields");
    assertEquals(nestedArrayStringFieldSubfields.size(), 2);
    assertTrue(nestedArrayStringFieldSubfields.containsKey("delimited"));
    assertTrue(nestedArrayStringFieldSubfields.containsKey("keyword"));
    Map<String, Object> nestedArrayArrayField =
        (Map<String, Object>) properties.get("nestedArrayArrayField");
    assertEquals(nestedArrayArrayField.get("type"), "keyword");
    assertEquals(nestedArrayArrayField.get("normalizer"), "keyword_normalizer");
    Map<String, Object> nestedArrayArrayFieldSubfields =
        (Map<String, Object>) nestedArrayArrayField.get("fields");
    assertEquals(nestedArrayArrayFieldSubfields.size(), 2);
    assertTrue(nestedArrayArrayFieldSubfields.containsKey("delimited"));
    assertTrue(nestedArrayArrayFieldSubfields.containsKey("keyword"));
    Map<String, Object> customPropertiesField =
        (Map<String, Object>) properties.get("customProperties");
    assertEquals(customPropertiesField.get("type"), "keyword");
    assertEquals(customPropertiesField.get("normalizer"), "keyword_normalizer");
    Map<String, Object> customPropertiesFieldSubfields =
        (Map<String, Object>) customPropertiesField.get("fields");
    assertEquals(customPropertiesFieldSubfields.size(), 2);
    assertTrue(customPropertiesFieldSubfields.containsKey("delimited"));
    assertTrue(customPropertiesFieldSubfields.containsKey("keyword"));

    // TEXT with addToFilters
    Map<String, Object> textField = (Map<String, Object>) properties.get("textFieldOverride");
    assertEquals(textField.get("type"), "keyword");
    assertEquals(textField.get("normalizer"), "keyword_normalizer");
    Map<String, Object> textFieldSubfields = (Map<String, Object>) textField.get("fields");
    assertEquals(textFieldSubfields.size(), 2);
    assertTrue(textFieldSubfields.containsKey("delimited"));
    assertTrue(textFieldSubfields.containsKey("keyword"));

    // TEXT with addToFilters aliased under "_entityName"
    Map<String, Object> textFieldAlias = (Map<String, Object>) properties.get("_entityName");
    assertEquals(textFieldAlias.get("type"), "alias");
    assertEquals(textFieldAlias.get("path"), "textFieldOverride");

    // TEXT_PARTIAL
    Map<String, Object> textArrayField = (Map<String, Object>) properties.get("textArrayField");
    assertEquals(textArrayField.get("type"), "keyword");
    assertEquals(textArrayField.get("normalizer"), "keyword_normalizer");
    Map<String, Object> textArrayFieldSubfields =
        (Map<String, Object>) textArrayField.get("fields");
    assertEquals(textArrayFieldSubfields.size(), 3);
    assertTrue(textArrayFieldSubfields.containsKey("delimited"));
    assertTrue(textArrayFieldSubfields.containsKey("ngram"));
    assertTrue(textArrayFieldSubfields.containsKey("keyword"));

    // WORD_GRAM
    Map<String, Object> wordGramField = (Map<String, Object>) properties.get("wordGramField");
    assertEquals(wordGramField.get("type"), "keyword");
    assertEquals(wordGramField.get("normalizer"), "keyword_normalizer");
    Map<String, Object> wordGramFieldSubfields = (Map<String, Object>) wordGramField.get("fields");
    assertEquals(wordGramFieldSubfields.size(), 6);
    assertTrue(wordGramFieldSubfields.containsKey("delimited"));
    assertTrue(wordGramFieldSubfields.containsKey("ngram"));
    assertTrue(wordGramFieldSubfields.containsKey("keyword"));
    assertTrue(wordGramFieldSubfields.containsKey("wordGrams2"));
    assertTrue(wordGramFieldSubfields.containsKey("wordGrams3"));
    assertTrue(wordGramFieldSubfields.containsKey("wordGrams4"));

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
    Map<String, Object> nestedForeignKeySubfields =
        (Map<String, Object>) nestedForeignKey.get("fields");
    assertEquals(nestedForeignKeySubfields.size(), 2);
    assertTrue(nestedForeignKeySubfields.containsKey("keyword"));
    assertTrue(nestedForeignKeySubfields.containsKey("ngram"));

    // OBJECT
    Map<String, Object> esObjectField = (Map<String, Object>) properties.get("esObjectField");
    assertEquals(esObjectField.get("type"), "object");
    assertEquals(customPropertiesField.get("normalizer"), "keyword_normalizer");

    // Scores
    Map<String, Object> feature1 = (Map<String, Object>) properties.get("feature1");
    assertEquals(feature1.get("type"), "double");
    Map<String, Object> feature2 = (Map<String, Object>) properties.get("feature2");
    assertEquals(feature2.get("type"), "double");

    // DOUBLE
    Map<String, Object> doubleField = (Map<String, Object>) properties.get("doubleField");
    assertEquals(doubleField.get("type"), "double");
  }

  @Test
  public void testGetMappingsWithStructuredProperty() throws URISyntaxException {
    // Baseline comparison: Mappings with no structured props
    Map<String, Object> resultWithoutStructuredProps =
        MappingsBuilder.getMappings(mock(EntityRegistry.class), TestEntitySpecBuilder.getSpec());

    // Test that a structured property that does not apply to the entity does not alter the mappings
    StructuredPropertyDefinition structPropNotForThisEntity =
        new StructuredPropertyDefinition()
            .setVersion(null, SetMode.REMOVE_IF_NULL)
            .setQualifiedName("propNotForThis")
            .setDisplayName("propNotForThis")
            .setEntityTypes(new UrnArray(Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "dataset")))
            .setValueType(Urn.createFromString("urn:li:logicalType:STRING"));
    Map<String, Object> resultWithOnlyUnrelatedStructuredProp =
        MappingsBuilder.getMappings(
            mock(EntityRegistry.class),
            TestEntitySpecBuilder.getSpec(),
            List.of(
                Pair.of(
                    UrnUtils.getUrn("urn:li:structuredProperty:propNotForThis"),
                    structPropNotForThisEntity)));
    assertEquals(resultWithOnlyUnrelatedStructuredProp, resultWithoutStructuredProps);

    // Test that a structured property that does apply to this entity is included in the mappings
    String fqnOfRelatedProp = "propForThis";
    StructuredPropertyDefinition structPropForThisEntity =
        new StructuredPropertyDefinition()
            .setVersion(null, SetMode.REMOVE_IF_NULL)
            .setQualifiedName(fqnOfRelatedProp)
            .setDisplayName("propForThis")
            .setEntityTypes(
                new UrnArray(
                    Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "dataset"),
                    Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "testEntity")))
            .setValueType(Urn.createFromString("urn:li:logicalType:STRING"));
    Map<String, Object> resultWithOnlyRelatedStructuredProp =
        MappingsBuilder.getMappings(
            mock(EntityRegistry.class),
            TestEntitySpecBuilder.getSpec(),
            List.of(
                Pair.of(
                    UrnUtils.getUrn("urn:li:structuredProperty:propForThis"),
                    structPropForThisEntity)));
    assertNotEquals(resultWithOnlyRelatedStructuredProp, resultWithoutStructuredProps);
    Map<String, Object> fieldsBefore =
        (Map<String, Object>) resultWithoutStructuredProps.get("properties");
    Map<String, Object> fieldsAfter =
        (Map<String, Object>) resultWithOnlyRelatedStructuredProp.get("properties");
    assertEquals(fieldsAfter.size(), fieldsBefore.size() + 1);

    Map<String, Object> structProps = (Map<String, Object>) fieldsAfter.get("structuredProperties");
    fieldsAfter = (Map<String, Object>) structProps.get("properties");

    String newField =
        fieldsAfter.keySet().stream()
            .filter(field -> !fieldsBefore.containsKey(field))
            .findFirst()
            .get();
    assertEquals(newField, fqnOfRelatedProp);
    assertEquals(
        fieldsAfter.get(newField),
        Map.of(
            "normalizer",
            "keyword_normalizer",
            "type",
            "keyword",
            "fields",
            Map.of("keyword", Map.of("type", "keyword"))));

    // Test that only structured properties that apply are included
    Map<String, Object> resultWithBothStructuredProps =
        MappingsBuilder.getMappings(
            mock(EntityRegistry.class),
            TestEntitySpecBuilder.getSpec(),
            List.of(
                Pair.of(
                    UrnUtils.getUrn("urn:li:structuredProperty:propForThis"),
                    structPropForThisEntity),
                Pair.of(
                    UrnUtils.getUrn("urn:li:structuredProperty:propNotForThis"),
                    structPropNotForThisEntity)));
    assertEquals(resultWithBothStructuredProps, resultWithOnlyRelatedStructuredProp);
  }

  @Test
  public void testGetMappingsWithStructuredPropertyV1() throws URISyntaxException {
    // Baseline comparison: Mappings with no structured props
    Map<String, Object> resultWithoutStructuredProps =
        MappingsBuilder.getMappings(mock(EntityRegistry.class), TestEntitySpecBuilder.getSpec());

    // Test that a structured property that does not apply to the entity does not alter the mappings
    StructuredPropertyDefinition structPropNotForThisEntity =
        new StructuredPropertyDefinition()
            .setVersion("00000000000001")
            .setQualifiedName("propNotForThis")
            .setDisplayName("propNotForThis")
            .setEntityTypes(new UrnArray(Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "dataset")))
            .setValueType(Urn.createFromString("urn:li:logicalType:STRING"));
    Map<String, Object> resultWithOnlyUnrelatedStructuredProp =
        MappingsBuilder.getMappings(
            mock(EntityRegistry.class),
            TestEntitySpecBuilder.getSpec(),
            List.of(
                Pair.of(
                    UrnUtils.getUrn("urn:li:structuredProperty:propNotForThis"),
                    structPropNotForThisEntity)));
    assertEquals(resultWithOnlyUnrelatedStructuredProp, resultWithoutStructuredProps);

    // Test that a structured property that does apply to this entity is included in the mappings
    String fqnOfRelatedProp = "propForThis";
    StructuredPropertyDefinition structPropForThisEntity =
        new StructuredPropertyDefinition()
            .setVersion("00000000000001")
            .setQualifiedName(fqnOfRelatedProp)
            .setDisplayName("propForThis")
            .setEntityTypes(
                new UrnArray(
                    Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "dataset"),
                    Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "testEntity")))
            .setValueType(Urn.createFromString("urn:li:logicalType:STRING"));
    Map<String, Object> resultWithOnlyRelatedStructuredProp =
        MappingsBuilder.getMappings(
            mock(EntityRegistry.class),
            TestEntitySpecBuilder.getSpec(),
            List.of(
                Pair.of(
                    UrnUtils.getUrn("urn:li:structuredProperty:propForThis"),
                    structPropForThisEntity)));
    assertNotEquals(resultWithOnlyRelatedStructuredProp, resultWithoutStructuredProps);
    Map<String, Object> fieldsBefore =
        (Map<String, Object>) resultWithoutStructuredProps.get("properties");
    Map<String, Object> fieldsAfter =
        (Map<String, Object>) resultWithOnlyRelatedStructuredProp.get("properties");
    assertEquals(fieldsAfter.size(), fieldsBefore.size() + 1);

    Map<String, Object> structProps = (Map<String, Object>) fieldsAfter.get("structuredProperties");
    fieldsAfter = (Map<String, Object>) structProps.get("properties");

    String newField =
        fieldsAfter.keySet().stream()
            .filter(field -> !fieldsBefore.containsKey(field))
            .findFirst()
            .get();
    assertEquals(newField, "_versioned." + fqnOfRelatedProp + ".00000000000001.string");
    assertEquals(
        fieldsAfter.get(newField),
        Map.of(
            "normalizer",
            "keyword_normalizer",
            "type",
            "keyword",
            "fields",
            Map.of("keyword", Map.of("type", "keyword"))));

    // Test that only structured properties that apply are included
    Map<String, Object> resultWithBothStructuredProps =
        MappingsBuilder.getMappings(
            mock(EntityRegistry.class),
            TestEntitySpecBuilder.getSpec(),
            List.of(
                Pair.of(
                    UrnUtils.getUrn("urn:li:structuredProperty:propForThis"),
                    structPropForThisEntity),
                Pair.of(
                    UrnUtils.getUrn("urn:li:structuredProperty:propNotForThis"),
                    structPropNotForThisEntity)));
    assertEquals(resultWithBothStructuredProps, resultWithOnlyRelatedStructuredProp);
  }

  @Test
  public void testGetMappingsForStructuredProperty() throws URISyntaxException {
    StructuredPropertyDefinition testStructProp =
        new StructuredPropertyDefinition()
            .setVersion(null, SetMode.REMOVE_IF_NULL)
            .setQualifiedName("testProp")
            .setDisplayName("exampleProp")
            .setEntityTypes(
                new UrnArray(
                    Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "dataset"),
                    Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "testEntity")))
            .setValueType(Urn.createFromString("urn:li:logicalType:STRING"));
    Map<String, Object> structuredPropertyFieldMappings =
        MappingsBuilder.getMappingsForStructuredProperty(
            List.of(
                (Pair.of(
                    UrnUtils.getUrn("urn:li:structuredProperties:testProp"), testStructProp))));
    assertEquals(structuredPropertyFieldMappings.size(), 1);
    String keyInMap = structuredPropertyFieldMappings.keySet().stream().findFirst().get();
    assertEquals(keyInMap, "testProp");

    Object mappings = structuredPropertyFieldMappings.get(keyInMap);
    assertEquals(
        mappings,
        Map.of(
            "type",
            "keyword",
            "normalizer",
            "keyword_normalizer",
            "fields",
            Map.of("keyword", Map.of("type", "keyword"))));

    StructuredPropertyDefinition propWithNumericType =
        new StructuredPropertyDefinition()
            .setVersion(null, SetMode.REMOVE_IF_NULL)
            .setQualifiedName("testPropNumber")
            .setDisplayName("examplePropNumber")
            .setEntityTypes(
                new UrnArray(
                    Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "dataset"),
                    Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "testEntity")))
            .setValueType(Urn.createFromString("urn:li:logicalType:NUMBER"));
    Map<String, Object> structuredPropertyFieldMappingsNumber =
        MappingsBuilder.getMappingsForStructuredProperty(
            List.of(
                Pair.of(
                    UrnUtils.getUrn("urn:li:structuredProperties:testPropNumber"),
                    propWithNumericType)));
    assertEquals(structuredPropertyFieldMappingsNumber.size(), 1);
    keyInMap = structuredPropertyFieldMappingsNumber.keySet().stream().findFirst().get();
    assertEquals("testPropNumber", keyInMap);
    mappings = structuredPropertyFieldMappingsNumber.get(keyInMap);
    assertEquals(Map.of("type", "double"), mappings);
  }

  @Test
  public void testGetMappingsForStructuredPropertyV1() throws URISyntaxException {
    StructuredPropertyDefinition testStructProp =
        new StructuredPropertyDefinition()
            .setVersion("00000000000001")
            .setQualifiedName("testProp")
            .setDisplayName("exampleProp")
            .setEntityTypes(
                new UrnArray(
                    Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "dataset"),
                    Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "testEntity")))
            .setValueType(Urn.createFromString("urn:li:logicalType:STRING"));
    Map<String, Object> structuredPropertyFieldMappings =
        MappingsBuilder.getMappingsForStructuredProperty(
            List.of(
                (Pair.of(
                    UrnUtils.getUrn("urn:li:structuredProperties:testProp"), testStructProp))));
    assertEquals(structuredPropertyFieldMappings.size(), 1);
    String keyInMap = structuredPropertyFieldMappings.keySet().stream().findFirst().get();
    assertEquals(keyInMap, "_versioned.testProp.00000000000001.string");

    Object mappings = structuredPropertyFieldMappings.get(keyInMap);
    assertEquals(
        mappings,
        Map.of(
            "type",
            "keyword",
            "normalizer",
            "keyword_normalizer",
            "fields",
            Map.of("keyword", Map.of("type", "keyword"))));

    StructuredPropertyDefinition propWithNumericType =
        new StructuredPropertyDefinition()
            .setVersion("00000000000001")
            .setQualifiedName("testPropNumber")
            .setDisplayName("examplePropNumber")
            .setEntityTypes(
                new UrnArray(
                    Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "dataset"),
                    Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "testEntity")))
            .setValueType(Urn.createFromString("urn:li:logicalType:NUMBER"));
    Map<String, Object> structuredPropertyFieldMappingsNumber =
        MappingsBuilder.getMappingsForStructuredProperty(
            List.of(
                Pair.of(
                    UrnUtils.getUrn("urn:li:structuredProperty:testPropNumber"),
                    propWithNumericType)));
    assertEquals(structuredPropertyFieldMappingsNumber.size(), 1);
    keyInMap = structuredPropertyFieldMappingsNumber.keySet().stream().findFirst().get();
    assertEquals(keyInMap, "_versioned.testPropNumber.00000000000001.number");
    mappings = structuredPropertyFieldMappingsNumber.get(keyInMap);
    assertEquals(Map.of("type", "double"), mappings);
  }

  @Test
  public void testRefMappingsBuilder() {
    EntityRegistry entityRegistry = getTestEntityRegistry();

    EntitySpec entitySpec = new EntitySpecBuilder().buildEntitySpec(new TestRefEntity().schema());
    Map<String, Object> result = MappingsBuilder.getMappings(entityRegistry, entitySpec);
    assertEquals(result.size(), 1);
    Map<String, Object> properties = (Map<String, Object>) result.get("properties");
    assertEquals(properties.size(), 7);
    ImmutableMap<String, Serializable> expectedURNField =
        ImmutableMap.of(
            "type",
            "keyword",
            "fields",
            ImmutableMap.of(
                "delimited",
                ImmutableMap.of(
                    "type",
                    "text",
                    "analyzer",
                    "urn_component",
                    "search_analyzer",
                    "query_urn_component",
                    "search_quote_analyzer",
                    "quote_analyzer"),
                "ngram",
                ImmutableMap.of(
                    "type",
                    "search_as_you_type",
                    "max_shingle_size",
                    "4",
                    "doc_values",
                    "false",
                    "analyzer",
                    "partial_urn_component")));
    assertEquals(properties.get("urn"), expectedURNField);
    assertEquals(properties.get("runId"), ImmutableMap.of("type", "keyword"));
    assertTrue(properties.containsKey("editedFieldDescriptions"));
    assertTrue(properties.containsKey("displayName"));
    assertTrue(properties.containsKey("refEntityUrns"));
    // @SearchableRef Field
    Map<String, Object> refField = (Map<String, Object>) properties.get("refEntityUrns");
    assertEquals(refField.size(), 1);
    Map<String, Object> refFieldProperty = (Map<String, Object>) refField.get("properties");

    assertEquals(refFieldProperty.get("urn"), expectedURNField);
    assertTrue(refFieldProperty.containsKey("displayName"));
    assertTrue(refFieldProperty.containsKey("editedFieldDescriptions"));
  }

  private EntityRegistry getTestEntityRegistry() {
    return new ConfigEntityRegistry(
        TestSearchFieldConfig.class
            .getClassLoader()
            .getResourceAsStream("test-entity-registry.yaml"));
  }
}
