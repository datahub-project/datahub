package com.linkedin.metadata.graph.elastic;

import static com.linkedin.metadata.aspect.models.graph.Edge.EDGE_FIELD_DESTINATION;
import static com.linkedin.metadata.aspect.models.graph.Edge.EDGE_FIELD_PROPERTIES;
import static com.linkedin.metadata.aspect.models.graph.Edge.EDGE_FIELD_SOURCE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.utils.elasticsearch.SearchClientShim.SearchEngineType;
import java.util.Map;
import org.testng.annotations.Test;

/**
 * Tests for engine-type-aware emission of explicit {@code type: object} on the {@code source},
 * {@code destination}, and {@code properties} fields (PFP-3594).
 *
 * <p>ES8+ returns {@code "type": "object"} explicitly when reading mappings back from the index; if
 * DataHub's target mapping omits it, the diff comparator perpetually sees a difference and triggers
 * a reindex loop. The fix emits it explicitly on ES8/9 only — ES7 and OpenSearch keep the legacy
 * implicit form to avoid a transitional reindex of existing indexes.
 */
public class GraphRelationshipMappingsBuilderTest {

  // The three edge fields whose value is a nested object (source/destination entities, edge props)
  // and which therefore need explicit type=object on ES8+.
  private static final String[] OBJECT_FIELDS = {
    EDGE_FIELD_SOURCE, EDGE_FIELD_DESTINATION, EDGE_FIELD_PROPERTIES
  };

  @SuppressWarnings("unchecked")
  private Map<String, Object> getProperties(Map<String, Object> mappings) {
    Object props = mappings.get("properties");
    assertNotNull(props, "Mapping must have a top-level properties block");
    assertTrue(props instanceof Map, "properties must be a Map");
    return (Map<String, Object>) props;
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> getField(Map<String, Object> properties, String fieldName) {
    Object field = properties.get(fieldName);
    assertNotNull(field, fieldName + " must be present in properties");
    assertTrue(field instanceof Map, fieldName + " must be a Map");
    return (Map<String, Object>) field;
  }

  // Legacy behavior: type=object must NOT be emitted for null / ES7 / OpenSearch.
  @Test
  public void testNullEngineTypeOmitsTypeObject() {
    Map<String, Object> mappings = GraphRelationshipMappingsBuilder.getMappings(null);
    Map<String, Object> properties = getProperties(mappings);

    for (String fieldName : OBJECT_FIELDS) {
      Map<String, Object> field = getField(properties, fieldName);
      assertFalse(
          field.containsKey("type"),
          fieldName
              + " must NOT have an explicit type when engineType is null (legacy behavior); ES7"
              + " stored mappings don't include it and emitting it would cause a transitional"
              + " reindex");
    }
  }

  @Test
  public void testElasticsearch7OmitsTypeObject() {
    Map<String, Object> mappings =
        GraphRelationshipMappingsBuilder.getMappings(SearchEngineType.ELASTICSEARCH_7);
    Map<String, Object> properties = getProperties(mappings);

    for (String fieldName : OBJECT_FIELDS) {
      Map<String, Object> field = getField(properties, fieldName);
      assertFalse(
          field.containsKey("type"),
          fieldName + " must NOT have explicit type=object on ES7 (legacy behavior)");
    }
  }

  @Test
  public void testOpenSearch2OmitsTypeObject() {
    Map<String, Object> mappings =
        GraphRelationshipMappingsBuilder.getMappings(SearchEngineType.OPENSEARCH_2);
    Map<String, Object> properties = getProperties(mappings);

    for (String fieldName : OBJECT_FIELDS) {
      Map<String, Object> field = getField(properties, fieldName);
      assertFalse(
          field.containsKey("type"),
          fieldName + " must NOT have explicit type=object on OpenSearch 2 (legacy behavior)");
    }
  }

  // ES8+ behavior: type=object MUST be emitted for source / destination / properties.
  @Test
  public void testElasticsearch8EmitsTypeObject() {
    Map<String, Object> mappings =
        GraphRelationshipMappingsBuilder.getMappings(SearchEngineType.ELASTICSEARCH_8);
    Map<String, Object> properties = getProperties(mappings);

    for (String fieldName : OBJECT_FIELDS) {
      Map<String, Object> field = getField(properties, fieldName);
      assertEquals(
          field.get("type"),
          "object",
          fieldName
              + " must explicitly declare type=object on ES8 to match what ES8 returns when"
              + " reading mappings back; otherwise the diff comparator triggers a perpetual"
              + " reindex loop (PFP-3594)");
    }
  }

  @Test
  public void testElasticsearch9EmitsTypeObject() {
    Map<String, Object> mappings =
        GraphRelationshipMappingsBuilder.getMappings(SearchEngineType.ELASTICSEARCH_9);
    Map<String, Object> properties = getProperties(mappings);

    for (String fieldName : OBJECT_FIELDS) {
      Map<String, Object> field = getField(properties, fieldName);
      assertEquals(
          field.get("type"), "object", fieldName + " must explicitly declare type=object on ES9");
    }
  }

  // Structural invariants: nested fields and other top-level keys must NOT change
  // based on engine type. Only the type=object marker should differ.
  @Test
  public void testSourceAndDestinationNestedFieldsUnchanged() {
    // Both source and destination should carry the same nested fields (urn, entityType, removed)
    // regardless of engine type. The fix only adds the type=object marker; sub-field structure is
    // identical.
    Map<String, Object> legacyMappings =
        GraphRelationshipMappingsBuilder.getMappings(SearchEngineType.ELASTICSEARCH_7);
    Map<String, Object> es8Mappings =
        GraphRelationshipMappingsBuilder.getMappings(SearchEngineType.ELASTICSEARCH_8);

    for (String fieldName : new String[] {EDGE_FIELD_SOURCE, EDGE_FIELD_DESTINATION}) {
      Map<String, Object> legacyField = getField(getProperties(legacyMappings), fieldName);
      Map<String, Object> es8Field = getField(getProperties(es8Mappings), fieldName);

      assertEquals(
          legacyField.get("properties"),
          es8Field.get("properties"),
          fieldName
              + ".properties (urn/entityType/removed) must be identical between ES7 and ES8 — the"
              + " fix should only add the type marker, not change sub-field definitions");
    }
  }

  @Test
  public void testEdgePropertiesNestedFieldUnchanged() {
    Map<String, Object> legacyMappings =
        GraphRelationshipMappingsBuilder.getMappings(SearchEngineType.ELASTICSEARCH_7);
    Map<String, Object> es8Mappings =
        GraphRelationshipMappingsBuilder.getMappings(SearchEngineType.ELASTICSEARCH_8);

    Map<String, Object> legacyField =
        getField(getProperties(legacyMappings), EDGE_FIELD_PROPERTIES);
    Map<String, Object> es8Field = getField(getProperties(es8Mappings), EDGE_FIELD_PROPERTIES);

    assertEquals(
        legacyField.get("properties"),
        es8Field.get("properties"),
        EDGE_FIELD_PROPERTIES + ".properties (source field) must be identical between ES7 and ES8");
  }

  @Test
  public void testTopLevelFieldSetUnchanged() {
    // The set of top-level fields (source, destination, relationshipType, properties,
    // lifecycleOwner, via, status flags, audit timestamps) must be identical regardless of
    // engine type. The fix only mutates the *value* shape of source/destination/properties,
    // never the field set.
    Map<String, Object> legacyProperties =
        getProperties(
            GraphRelationshipMappingsBuilder.getMappings(SearchEngineType.ELASTICSEARCH_7));
    Map<String, Object> es8Properties =
        getProperties(
            GraphRelationshipMappingsBuilder.getMappings(SearchEngineType.ELASTICSEARCH_8));

    assertEquals(
        legacyProperties.keySet(),
        es8Properties.keySet(),
        "Top-level field set in graph mappings must be identical between ES7 and ES8");
  }

  @Test
  public void testNonObjectFieldsUnaffectedByEngineType() {
    // Keyword/Boolean/Long fields like relationshipType, lifecycleOwner, createdOn, etc. must
    // NOT gain a type=object marker on ES8 — only the genuine object fields should.
    Map<String, Object> es8Properties =
        getProperties(
            GraphRelationshipMappingsBuilder.getMappings(SearchEngineType.ELASTICSEARCH_8));

    String[] scalarFields = {
      "relationshipType",
      "lifecycleOwner",
      "via",
      "createdOn",
      "createdActor",
      "updatedOn",
      "updatedActor"
    };
    for (String fieldName : scalarFields) {
      Map<String, Object> field = getField(es8Properties, fieldName);
      Object type = field.get("type");
      assertNotNull(type, fieldName + " must declare a type");
      assertFalse(
          "object".equals(type),
          fieldName
              + " is a scalar field and must NOT have type=object — got "
              + type
              + ". The fix should only add type=object to genuinely nested object fields.");
    }
  }
}
