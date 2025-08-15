package com.linkedin.metadata.search.transformer;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.test.TestEntitySnapshot;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMapBuilder;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.dataset.EditableDatasetProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.TestEntitySpecBuilder;
import com.linkedin.metadata.TestEntityUtil;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableRefFieldSpec;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.query.request.TestSearchFieldConfig;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class SearchDocumentTransformerTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final EntityRegistry ENTITY_REGISTRY =
      TestOperationContexts.defaultEntityRegistry();
  private static final EntityRegistry TEST_ENTITY_REGISTRY;

  static {
    TEST_ENTITY_REGISTRY =
        new ConfigEntityRegistry(
            TestSearchFieldConfig.class
                .getClassLoader()
                .getResourceAsStream("test-entity-registry.yaml"));
  }

  static {
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    OBJECT_MAPPER
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());
  }

  @Test
  public void testTransform() throws IOException {
    SearchDocumentTransformer searchDocumentTransformer =
        new SearchDocumentTransformer(1000, 1000, 1000);
    TestEntitySnapshot snapshot = TestEntityUtil.getSnapshot();
    EntitySpec testEntitySpec = TestEntitySpecBuilder.getSpec();
    Optional<String> result =
        searchDocumentTransformer.transformSnapshot(snapshot, testEntitySpec, false);
    assertTrue(result.isPresent());
    ObjectNode parsedJson = (ObjectNode) OBJECT_MAPPER.readTree(result.get());
    assertEquals(parsedJson.get("urn").asText(), snapshot.getUrn().toString());
    assertEquals(parsedJson.get("doubleField").asDouble(), 100.456);
    assertEquals(parsedJson.get("keyPart1").asText(), "key");
    assertFalse(parsedJson.has("keyPart2"));
    assertEquals(parsedJson.get("keyPart3").asText(), "VALUE_1");
    assertFalse(parsedJson.has("textField"));
    assertEquals(parsedJson.get("textFieldOverride").asText(), "test");
    ArrayNode textArrayField = (ArrayNode) parsedJson.get("textArrayField");
    assertEquals(textArrayField.size(), 2);
    assertEquals(textArrayField.get(0).asText(), "testArray1");
    assertEquals(textArrayField.get(1).asText(), "testArray2");
    assertEquals(parsedJson.get("nestedIntegerField").asInt(), 1);
    assertEquals(parsedJson.get("nestedForeignKey").asText(), snapshot.getUrn().toString());
    ArrayNode nextedArrayField = (ArrayNode) parsedJson.get("nestedArrayStringField");
    assertEquals(nextedArrayField.size(), 2);
    assertEquals(nextedArrayField.get(0).asText(), "nestedArray1");
    assertEquals(nextedArrayField.get(1).asText(), "nestedArray2");
    ArrayNode browsePaths = (ArrayNode) parsedJson.get("browsePaths");
    assertEquals(browsePaths.size(), 2);
    assertEquals(browsePaths.get(0).asText(), "/a/b/c");
    assertEquals(browsePaths.get(1).asText(), "d/e/f");
    assertEquals(parsedJson.get("feature1").asInt(), 2);
    assertEquals(parsedJson.get("feature2").asInt(), 1);
    JsonNode browsePathV2 = (JsonNode) parsedJson.get("browsePathV2");
    assertEquals(browsePathV2.asText(), "␟levelOne␟levelTwo");
    assertEquals(
        parsedJson.get("esObjectFieldBoolean").get("key1").getNodeType(),
        JsonNodeFactory.instance.booleanNode(true).getNodeType());
    assertEquals(
        parsedJson.get("esObjectFieldLong").get("key1").getNodeType(),
        JsonNodeFactory.instance.numberNode(1L).getNodeType());
    assertEquals(
        parsedJson.get("esObjectFieldFloat").get("key2").getNodeType(),
        JsonNodeFactory.instance.numberNode(2.0f).getNodeType());
    assertEquals(
        parsedJson.get("esObjectFieldDouble").get("key1").getNodeType(),
        JsonNodeFactory.instance.numberNode(1.2).getNodeType());
    assertEquals(
        parsedJson.get("esObjectFieldInteger").get("key2").getNodeType(),
        JsonNodeFactory.instance.numberNode(456).getNodeType());
    assertEquals(
        parsedJson.get("esObjectFieldBoolean").get("key2").getNodeType(),
        JsonNodeFactory.instance.booleanNode(false).getNodeType());
    assertEquals(
        parsedJson.get("esObjectFieldLong").get("key2").getNodeType(),
        JsonNodeFactory.instance.numberNode(2L).getNodeType());
    assertEquals(
        parsedJson.get("esObjectFieldFloat").get("key1").getNodeType(),
        JsonNodeFactory.instance.numberNode(1.0f).getNodeType());
    assertEquals(
        parsedJson.get("esObjectFieldDouble").get("key2").getNodeType(),
        JsonNodeFactory.instance.numberNode(2.4).getNodeType());
    assertEquals(
        parsedJson.get("esObjectFieldInteger").get("key1").getNodeType(),
        JsonNodeFactory.instance.numberNode(123).getNodeType());
    assertEquals(parsedJson.get("esObjectField").get("key3").asText(), "");
    assertEquals(
        parsedJson.get("esObjectFieldBoolean").get("key2").getNodeType(),
        JsonNodeFactory.instance.booleanNode(false).getNodeType());
    assertEquals(
        parsedJson.get("esObjectFieldLong").get("key2").getNodeType(),
        JsonNodeFactory.instance.numberNode(2L).getNodeType());
    assertEquals(
        parsedJson.get("esObjectFieldFloat").get("key1").getNodeType(),
        JsonNodeFactory.instance.numberNode(1.0f).getNodeType());
    assertEquals(
        parsedJson.get("esObjectFieldDouble").get("key2").getNodeType(),
        JsonNodeFactory.instance.numberNode(2.4).getNodeType());
    assertEquals(
        parsedJson.get("esObjectFieldInteger").get("key1").getNodeType(),
        JsonNodeFactory.instance.numberNode(123).getNodeType());
  }

  @Test
  public void testTransformForDelete() throws IOException {
    SearchDocumentTransformer searchDocumentTransformer =
        new SearchDocumentTransformer(1000, 1000, 1000);
    TestEntitySnapshot snapshot = TestEntityUtil.getSnapshot();
    EntitySpec testEntitySpec = TestEntitySpecBuilder.getSpec();
    Optional<String> result =
        searchDocumentTransformer.transformSnapshot(snapshot, testEntitySpec, true);
    assertTrue(result.isPresent());
    ObjectNode parsedJson = (ObjectNode) OBJECT_MAPPER.readTree(result.get());
    assertEquals(parsedJson.get("urn").asText(), snapshot.getUrn().toString());
    parsedJson.get("keyPart1").getNodeType().equals(JsonNodeType.NULL);
    parsedJson.get("keyPart3").getNodeType().equals(JsonNodeType.NULL);
    parsedJson.get("textFieldOverride").getNodeType().equals(JsonNodeType.NULL);
    parsedJson.get("foreignKey").getNodeType().equals(JsonNodeType.NULL);
    parsedJson.get("textArrayField").getNodeType().equals(JsonNodeType.NULL);
    parsedJson.get("browsePaths").getNodeType().equals(JsonNodeType.NULL);
    parsedJson.get("nestedArrayStringField").getNodeType().equals(JsonNodeType.NULL);
    parsedJson.get("nestedIntegerField").getNodeType().equals(JsonNodeType.NULL);
    parsedJson.get("feature1").getNodeType().equals(JsonNodeType.NULL);
    parsedJson.get("feature2").getNodeType().equals(JsonNodeType.NULL);
    parsedJson.get("doubleField").getNodeType().equals(JsonNodeType.NULL);
  }

  @Test
  public void testTransformMaxFieldValue() throws IOException {
    SearchDocumentTransformer searchDocumentTransformer =
        new SearchDocumentTransformer(1000, 1000, 5);
    TestEntitySnapshot snapshot = TestEntityUtil.getSnapshot();
    EntitySpec testEntitySpec = TestEntitySpecBuilder.getSpec();
    Optional<String> result =
        searchDocumentTransformer.transformSnapshot(snapshot, testEntitySpec, false);
    assertTrue(result.isPresent());
    ObjectNode parsedJson = (ObjectNode) OBJECT_MAPPER.readTree(result.get());

    assertEquals(
        parsedJson.get("customProperties"),
        JsonNodeFactory.instance.arrayNode().add("shortValue=123"));
    assertEquals(
        parsedJson.get("esObjectField"), JsonNodeFactory.instance.arrayNode().add("123").add(""));

    searchDocumentTransformer = new SearchDocumentTransformer(1000, 1000, 20);
    snapshot = TestEntityUtil.getSnapshot();
    testEntitySpec = TestEntitySpecBuilder.getSpec();
    result = searchDocumentTransformer.transformSnapshot(snapshot, testEntitySpec, false);
    assertTrue(result.isPresent());
    parsedJson = (ObjectNode) OBJECT_MAPPER.readTree(result.get());

    assertEquals(
        parsedJson.get("customProperties"),
        JsonNodeFactory.instance
            .arrayNode()
            .add("key1=value1")
            .add("key2=value2")
            .add("shortValue=123")
            .add("longValue=0123456789"));
    assertEquals(
        parsedJson.get("esObjectField"),
        JsonNodeFactory.instance
            .arrayNode()
            .add("value1")
            .add("value2")
            .add("123")
            .add("")
            .add("0123456789"));
  }

  /**
   *
   *
   * <ul>
   *   <li>{@link SearchDocumentTransformer#setSearchableRefValue(OperationContext,
   *       SearchableRefFieldSpec, List, ObjectNode, Boolean, AuditStamp)}
   * </ul>
   */
  @Test
  public void testSetSearchableRefValue() throws URISyntaxException, RemoteInvocationException {
    AspectRetriever aspectRetriever = Mockito.mock(AspectRetriever.class);
    SearchDocumentTransformer searchDocumentTransformer =
        new SearchDocumentTransformer(1000, 1000, 1000);

    List<Object> urnList = List.of(Urn.createFromString("urn:li:refEntity:1"));

    DataMapBuilder dataMapBuilder = new DataMapBuilder();
    dataMapBuilder.addKVPair("fieldPath", "refEntityUrn");
    dataMapBuilder.addKVPair("name", "refEntityUrnName");
    dataMapBuilder.addKVPair("description", "refEntityUrn1 description details");
    Aspect aspect = new Aspect(dataMapBuilder.convertToDataMap());

    ObjectNode searchDocument = JsonNodeFactory.instance.objectNode();
    SearchableRefFieldSpec searchableRefFieldSpec =
        TEST_ENTITY_REGISTRY.getEntitySpec("testRefEntity").getSearchableRefFieldSpecs().get(0);

    // Mock Behaviour
    Mockito.when(aspectRetriever.getEntityRegistry()).thenReturn(TEST_ENTITY_REGISTRY);
    Mockito.when(aspectRetriever.getLatestAspectObject(any(), anyString())).thenReturn(aspect);
    OperationContext opContext =
        TestOperationContexts.systemContextNoSearchAuthorization(
            RetrieverContext.builder()
                .aspectRetriever(aspectRetriever)
                .cachingAspectRetriever(
                    TestOperationContexts.emptyActiveUsersAspectRetriever(
                        () -> TEST_ENTITY_REGISTRY))
                .graphRetriever(mock(GraphRetriever.class))
                .searchRetriever(mock(SearchRetriever.class))
                .build());

    searchDocumentTransformer.setSearchableRefValue(
        opContext,
        searchableRefFieldSpec,
        urnList,
        searchDocument,
        false,
        AuditStampUtils.createDefaultAuditStamp());
    assertTrue(searchDocument.has("refEntityUrns"));
    assertEquals(searchDocument.get("refEntityUrns").size(), 3);
    assertTrue(searchDocument.get("refEntityUrns").has("urn"));
    assertTrue(searchDocument.get("refEntityUrns").has("editedFieldDescriptions"));
    assertTrue(searchDocument.get("refEntityUrns").has("displayName"));
    assertEquals(searchDocument.get("refEntityUrns").get("urn").asText(), "urn:li:refEntity:1");
    assertEquals(
        searchDocument.get("refEntityUrns").get("editedFieldDescriptions").asText(),
        "refEntityUrn1 description details");
    assertEquals(
        searchDocument.get("refEntityUrns").get("displayName").asText(), "refEntityUrnName");
  }

  @Test
  public void testSetSearchableRefValue_WithNonURNField() throws URISyntaxException {
    SearchDocumentTransformer searchDocumentTransformer =
        new SearchDocumentTransformer(1000, 1000, 1000);

    OperationContext opContext =
        TestOperationContexts.systemContextNoSearchAuthorization(TEST_ENTITY_REGISTRY);
    List<Object> urnList = List.of(Urn.createFromString("urn:li:refEntity:1"));

    ObjectNode searchDocument = JsonNodeFactory.instance.objectNode();
    SearchableRefFieldSpec searchableRefFieldSpecText =
        TEST_ENTITY_REGISTRY.getEntitySpec("testRefEntity").getSearchableRefFieldSpecs().get(1);
    searchDocumentTransformer.setSearchableRefValue(
        opContext,
        searchableRefFieldSpecText,
        urnList,
        searchDocument,
        false,
        AuditStampUtils.createDefaultAuditStamp());
    assertTrue(searchDocument.isEmpty());
  }

  @Test
  public void testSetSearchableRefValue_RuntimeException()
      throws URISyntaxException, RemoteInvocationException {
    AspectRetriever aspectRetriever = Mockito.mock(AspectRetriever.class);
    SearchDocumentTransformer searchDocumentTransformer =
        new SearchDocumentTransformer(1000, 1000, 1000);

    List<Object> urnList = List.of(Urn.createFromString("urn:li:refEntity:1"));

    Mockito.when(aspectRetriever.getEntityRegistry()).thenReturn(TEST_ENTITY_REGISTRY);
    Mockito.when(
            aspectRetriever.getLatestAspectObject(
                eq(Urn.createFromString("urn:li:refEntity:1")), anyString()))
        .thenThrow(new RuntimeException("Error"));
    OperationContext opContext =
        TestOperationContexts.systemContextNoSearchAuthorization(
            RetrieverContext.builder()
                .aspectRetriever(aspectRetriever)
                .cachingAspectRetriever(
                    TestOperationContexts.emptyActiveUsersAspectRetriever(
                        () -> TEST_ENTITY_REGISTRY))
                .graphRetriever(mock(GraphRetriever.class))
                .searchRetriever(mock(SearchRetriever.class))
                .build());

    ObjectNode searchDocument = JsonNodeFactory.instance.objectNode();
    SearchableRefFieldSpec searchableRefFieldSpec =
        TEST_ENTITY_REGISTRY.getEntitySpec("testRefEntity").getSearchableRefFieldSpecs().get(0);
    searchDocumentTransformer.setSearchableRefValue(
        opContext,
        searchableRefFieldSpec,
        urnList,
        searchDocument,
        false,
        AuditStampUtils.createDefaultAuditStamp());
    assertTrue(searchDocument.isEmpty());
  }

  @Test
  public void testSetSearchableRefValue_RuntimeException_URNExist()
      throws URISyntaxException, RemoteInvocationException {
    AspectRetriever aspectRetriever = Mockito.mock(AspectRetriever.class);
    SearchDocumentTransformer searchDocumentTransformer =
        new SearchDocumentTransformer(1000, 1000, 1000);

    List<Object> urnList = List.of(Urn.createFromString("urn:li:refEntity:1"));
    DataMapBuilder dataMapBuilder = new DataMapBuilder();
    dataMapBuilder.addKVPair("fieldPath", "refEntityUrn");
    dataMapBuilder.addKVPair("name", "refEntityUrnName");
    dataMapBuilder.addKVPair("description", "refEntityUrn1 description details");

    Aspect aspect = new Aspect(dataMapBuilder.convertToDataMap());
    Mockito.when(aspectRetriever.getEntityRegistry()).thenReturn(TEST_ENTITY_REGISTRY);
    Mockito.when(
            aspectRetriever.getLatestAspectObject(
                eq(Urn.createFromString("urn:li:refEntity:1")), anyString()))
        .thenReturn(aspect)
        .thenThrow(new RuntimeException("Error"));
    OperationContext opContext =
        TestOperationContexts.systemContextNoSearchAuthorization(
            RetrieverContext.builder()
                .aspectRetriever(aspectRetriever)
                .cachingAspectRetriever(
                    TestOperationContexts.emptyActiveUsersAspectRetriever(
                        () -> TEST_ENTITY_REGISTRY))
                .graphRetriever(mock(GraphRetriever.class))
                .searchRetriever(mock(SearchRetriever.class))
                .build());

    ObjectNode searchDocument = JsonNodeFactory.instance.objectNode();
    SearchableRefFieldSpec searchableRefFieldSpec =
        TEST_ENTITY_REGISTRY.getEntitySpec("testRefEntity").getSearchableRefFieldSpecs().get(0);
    searchDocumentTransformer.setSearchableRefValue(
        opContext,
        searchableRefFieldSpec,
        urnList,
        searchDocument,
        false,
        AuditStampUtils.createDefaultAuditStamp());
    assertTrue(searchDocument.has("refEntityUrns"));
    assertEquals(searchDocument.get("refEntityUrns").size(), 1);
    assertTrue(searchDocument.get("refEntityUrns").has("urn"));
    assertEquals(searchDocument.get("refEntityUrns").get("urn").asText(), "urn:li:refEntity:1");
  }

  @Test
  void testSetSearchableRefValue_WithInvalidURN()
      throws URISyntaxException, RemoteInvocationException {
    AspectRetriever aspectRetriever = Mockito.mock(AspectRetriever.class);
    SearchDocumentTransformer searchDocumentTransformer =
        new SearchDocumentTransformer(1000, 1000, 1000);

    List<Object> urnList = List.of(Urn.createFromString("urn:li:refEntity:1"));

    Mockito.when(aspectRetriever.getEntityRegistry()).thenReturn(TEST_ENTITY_REGISTRY);
    Mockito.when(aspectRetriever.getLatestAspectObject(any(), anyString())).thenReturn(null);
    SearchableRefFieldSpec searchableRefFieldSpec =
        TEST_ENTITY_REGISTRY.getEntitySpec("testRefEntity").getSearchableRefFieldSpecs().get(0);
    OperationContext opContext =
        TestOperationContexts.systemContextNoSearchAuthorization(
            RetrieverContext.builder()
                .aspectRetriever(aspectRetriever)
                .cachingAspectRetriever(
                    TestOperationContexts.emptyActiveUsersAspectRetriever(
                        () -> TEST_ENTITY_REGISTRY))
                .graphRetriever(mock(GraphRetriever.class))
                .searchRetriever(mock(SearchRetriever.class))
                .build());

    ObjectNode searchDocument = JsonNodeFactory.instance.objectNode();
    searchDocumentTransformer.setSearchableRefValue(
        opContext,
        searchableRefFieldSpec,
        urnList,
        searchDocument,
        false,
        AuditStampUtils.createDefaultAuditStamp());
    assertTrue(searchDocument.has("refEntityUrns"));
    assertTrue(searchDocument.get("refEntityUrns").getNodeType().equals(JsonNodeType.NULL));
  }

  @Test
  public void testEmptyDescription() throws RemoteInvocationException, URISyntaxException {
    String entityUrn = "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)";
    SearchDocumentTransformer test = new SearchDocumentTransformer(1000, 1000, 1000);

    // editedDescription - empty string
    Optional<ObjectNode> transformed =
        test.transformAspect(
            mock(OperationContext.class),
            UrnUtils.getUrn(entityUrn),
            new EditableDatasetProperties().setDescription(""),
            ENTITY_REGISTRY
                .getEntitySpec(DATASET_ENTITY_NAME)
                .getAspectSpec(EDITABLE_DATASET_PROPERTIES_ASPECT_NAME),
            false,
            AuditStampUtils.createDefaultAuditStamp());

    assertTrue(transformed.isPresent());
    assertEquals(transformed.get().get("urn").asText(), entityUrn);
    assertTrue(transformed.get().has("editedDescription"));
    assertTrue(transformed.get().get("editedDescription").isNull());

    // description - empty string
    transformed =
        test.transformAspect(
            mock(OperationContext.class),
            UrnUtils.getUrn(entityUrn),
            new DatasetProperties().setDescription(""),
            ENTITY_REGISTRY
                .getEntitySpec(DATASET_ENTITY_NAME)
                .getAspectSpec(DATASET_PROPERTIES_ASPECT_NAME),
            false,
            AuditStampUtils.createDefaultAuditStamp());

    assertTrue(transformed.isPresent());
    assertEquals(transformed.get().get("urn").asText(), entityUrn);
    assertTrue(transformed.get().has("description"));
    assertTrue(transformed.get().get("description").isNull());
    assertFalse(transformed.get().get("hasDescription").asBoolean());
  }

  @Test
  public void testHandleRemoveFieldsWithStructuredProperties() throws IOException {
    ObjectNode previousDoc = JsonNodeFactory.instance.objectNode();
    previousDoc.put("structuredProperties.prop1", "value1");
    previousDoc.put("structuredProperties.prop2", "value2");
    previousDoc.put("otherField", "value3");

    ObjectNode newDoc = JsonNodeFactory.instance.objectNode();
    newDoc.put("structuredProperties.prop1", "updatedValue1");
    newDoc.put("otherField", "updatedValue3");

    ObjectNode result = SearchDocumentTransformer.handleRemoveFields(newDoc, previousDoc);

    assertEquals(result.get("structuredProperties.prop1").asText(), "updatedValue1");
    assertTrue(result.has("structuredProperties.prop2"));
    assertTrue(result.get("structuredProperties.prop2").isNull());
    assertEquals(result.get("otherField").asText(), "updatedValue3");
  }

  @Test
  public void testHandleRemoveFieldsWithNullPreviousDoc() {
    // When previous doc is null, should return new doc unchanged
    ObjectNode newDoc = JsonNodeFactory.instance.objectNode();
    newDoc.put("field1", "value1");
    newDoc.put("field2", "value2");

    ObjectNode result = SearchDocumentTransformer.handleRemoveFields(newDoc, null);

    assertEquals(result, newDoc);
    assertEquals(result.get("field1").asText(), "value1");
    assertEquals(result.get("field2").asText(), "value2");
  }

  @Test
  public void testHandleRemoveFieldsWithEmptyPreviousDoc() {
    // When previous doc is empty, should return new doc unchanged
    ObjectNode newDoc = JsonNodeFactory.instance.objectNode();
    newDoc.put("field1", "value1");
    ObjectNode previousDoc = JsonNodeFactory.instance.objectNode();

    ObjectNode result = SearchDocumentTransformer.handleRemoveFields(newDoc, previousDoc);

    assertEquals(result, newDoc);
    assertEquals(result.get("field1").asText(), "value1");
  }

  @Test
  public void testHandleRemoveFieldsBasicFieldRemoval() {
    // Test basic field removal - fields in previous but not in new should be nulled
    ObjectNode previousDoc = JsonNodeFactory.instance.objectNode();
    previousDoc.put("field1", "value1");
    previousDoc.put("field2", "value2");
    previousDoc.put("field3", "value3");

    ObjectNode newDoc = JsonNodeFactory.instance.objectNode();
    newDoc.put("field1", "updatedValue1");
    newDoc.put("field3", "updatedValue3");

    ObjectNode result = SearchDocumentTransformer.handleRemoveFields(newDoc, previousDoc);

    assertEquals(result.get("field1").asText(), "updatedValue1");
    assertTrue(result.has("field2"));
    assertTrue(result.get("field2").isNull());
    assertEquals(result.get("field3").asText(), "updatedValue3");
  }

  @Test
  public void testHandleRemoveFieldsWithArrays() {
    // Test handling of array fields
    ObjectNode previousDoc = JsonNodeFactory.instance.objectNode();
    ArrayNode array1 = JsonNodeFactory.instance.arrayNode();
    array1.add("item1");
    array1.add("item2");
    previousDoc.set("arrayField", array1);
    previousDoc.put("regularField", "value");

    ObjectNode newDoc = JsonNodeFactory.instance.objectNode();
    newDoc.put("regularField", "newValue");

    ObjectNode result = SearchDocumentTransformer.handleRemoveFields(newDoc, previousDoc);

    assertEquals(result.get("regularField").asText(), "newValue");
    assertTrue(result.has("arrayField"));
    assertTrue(result.get("arrayField").isNull());
  }

  @Test
  public void testHandleRemoveFieldsWithNestedObjects() {
    // Test handling of nested object fields
    ObjectNode previousDoc = JsonNodeFactory.instance.objectNode();
    ObjectNode nestedObj = JsonNodeFactory.instance.objectNode();
    nestedObj.put("innerField1", "innerValue1");
    nestedObj.put("innerField2", "innerValue2");
    previousDoc.set("nestedObject", nestedObj);
    previousDoc.put("topLevel", "topValue");

    ObjectNode newDoc = JsonNodeFactory.instance.objectNode();
    newDoc.put("topLevel", "newTopValue");

    ObjectNode result = SearchDocumentTransformer.handleRemoveFields(newDoc, previousDoc);

    assertEquals(result.get("topLevel").asText(), "newTopValue");
    assertTrue(result.has("nestedObject"));
    assertTrue(result.get("nestedObject").isNull());
  }

  @Test
  public void testHandleRemoveFieldsWithMultipleStructuredProperties() {
    // Test multiple structured properties with dot notation
    ObjectNode previousDoc = JsonNodeFactory.instance.objectNode();
    previousDoc.put("structuredProperties.prop1", "value1");
    previousDoc.put("structuredProperties.prop2", "value2");
    previousDoc.put("structuredProperties.prop3", "value3");
    previousDoc.put("structuredProperties.prop4", "value4");
    previousDoc.put("regularField", "regularValue");

    ObjectNode newDoc = JsonNodeFactory.instance.objectNode();
    newDoc.put("structuredProperties.prop1", "updatedValue1");
    newDoc.put("structuredProperties.prop3", "updatedValue3");
    newDoc.put("regularField", "updatedRegularValue");

    ObjectNode result = SearchDocumentTransformer.handleRemoveFields(newDoc, previousDoc);

    assertEquals(result.get("structuredProperties.prop1").asText(), "updatedValue1");
    assertTrue(result.has("structuredProperties.prop2"));
    assertTrue(result.get("structuredProperties.prop2").isNull());
    assertEquals(result.get("structuredProperties.prop3").asText(), "updatedValue3");
    assertTrue(result.has("structuredProperties.prop4"));
    assertTrue(result.get("structuredProperties.prop4").isNull());
    assertEquals(result.get("regularField").asText(), "updatedRegularValue");
  }

  @Test
  public void testHandleRemoveFieldsWithMixedTypes() {
    // Test with mixed field types
    ObjectNode previousDoc = JsonNodeFactory.instance.objectNode();
    previousDoc.put("stringField", "string");
    previousDoc.put("numberField", 123);
    previousDoc.put("booleanField", true);
    previousDoc.put("nullField", (String) null);
    ArrayNode arrayField = JsonNodeFactory.instance.arrayNode();
    arrayField.add("item");
    previousDoc.set("arrayField", arrayField);

    ObjectNode newDoc = JsonNodeFactory.instance.objectNode();
    newDoc.put("stringField", "newString");
    newDoc.put("booleanField", false);

    ObjectNode result = SearchDocumentTransformer.handleRemoveFields(newDoc, previousDoc);

    assertEquals(result.get("stringField").asText(), "newString");
    assertTrue(result.has("numberField"));
    assertTrue(result.get("numberField").isNull());
    assertEquals(result.get("booleanField").asBoolean(), false);
    assertTrue(result.has("nullField"));
    assertTrue(result.get("nullField").isNull());
    assertTrue(result.has("arrayField"));
    assertTrue(result.get("arrayField").isNull());
  }

  @Test
  public void testHandleRemoveFieldsNoFieldsRemoved() {
    // Test when all fields are preserved (no removals needed)
    ObjectNode previousDoc = JsonNodeFactory.instance.objectNode();
    previousDoc.put("field1", "value1");
    previousDoc.put("field2", "value2");

    ObjectNode newDoc = JsonNodeFactory.instance.objectNode();
    newDoc.put("field1", "updatedValue1");
    newDoc.put("field2", "updatedValue2");
    newDoc.put("field3", "newField");

    ObjectNode result = SearchDocumentTransformer.handleRemoveFields(newDoc, previousDoc);

    assertEquals(result.get("field1").asText(), "updatedValue1");
    assertEquals(result.get("field2").asText(), "updatedValue2");
    assertEquals(result.get("field3").asText(), "newField");
    assertFalse(result.has("nullField"));
  }

  @Test
  public void testHandleRemoveFieldsAllFieldsRemoved() {
    // Test when all fields from previous doc are removed
    ObjectNode previousDoc = JsonNodeFactory.instance.objectNode();
    previousDoc.put("field1", "value1");
    previousDoc.put("field2", "value2");
    previousDoc.put("field3", "value3");

    ObjectNode newDoc = JsonNodeFactory.instance.objectNode();
    newDoc.put("newField1", "newValue1");
    newDoc.put("newField2", "newValue2");

    ObjectNode result = SearchDocumentTransformer.handleRemoveFields(newDoc, previousDoc);

    assertEquals(result.get("newField1").asText(), "newValue1");
    assertEquals(result.get("newField2").asText(), "newValue2");
    assertTrue(result.has("field1"));
    assertTrue(result.get("field1").isNull());
    assertTrue(result.has("field2"));
    assertTrue(result.get("field2").isNull());
    assertTrue(result.has("field3"));
    assertTrue(result.get("field3").isNull());
  }

  @Test
  public void testHandleRemoveFieldsWithSpecialCharacters() {
    // Test field names with special characters
    ObjectNode previousDoc = JsonNodeFactory.instance.objectNode();
    previousDoc.put("field-with-dash", "value1");
    previousDoc.put("field_with_underscore", "value2");
    previousDoc.put("field@with@at", "value3");
    previousDoc.put("field.with.dots", "value4");

    ObjectNode newDoc = JsonNodeFactory.instance.objectNode();
    newDoc.put("field-with-dash", "updated1");
    newDoc.put("field@with@at", "updated3");

    ObjectNode result = SearchDocumentTransformer.handleRemoveFields(newDoc, previousDoc);

    assertEquals(result.get("field-with-dash").asText(), "updated1");
    assertTrue(result.has("field_with_underscore"));
    assertTrue(result.get("field_with_underscore").isNull());
    assertEquals(result.get("field@with@at").asText(), "updated3");
    assertTrue(result.has("field.with.dots"));
    assertTrue(result.get("field.with.dots").isNull());
  }

  @Test
  public void testHandleRemoveFieldsWithDeepStructuredProperties() {
    // Test deeply nested structured properties
    ObjectNode previousDoc = JsonNodeFactory.instance.objectNode();
    previousDoc.put("structuredProperties.level1.prop1", "value1");
    previousDoc.put("structuredProperties.level1.prop2", "value2");
    previousDoc.put("structuredProperties.level2.prop1", "value3");
    previousDoc.put("structuredProperties.level2.prop2", "value4");

    ObjectNode newDoc = JsonNodeFactory.instance.objectNode();
    newDoc.put("structuredProperties.level1.prop1", "updated1");
    newDoc.put("structuredProperties.level2.prop2", "updated4");

    ObjectNode result = SearchDocumentTransformer.handleRemoveFields(newDoc, previousDoc);

    assertEquals(result.get("structuredProperties.level1.prop1").asText(), "updated1");
    assertTrue(result.has("structuredProperties.level1.prop2"));
    assertTrue(result.get("structuredProperties.level1.prop2").isNull());
    assertTrue(result.has("structuredProperties.level2.prop1"));
    assertTrue(result.get("structuredProperties.level2.prop1").isNull());
    assertEquals(result.get("structuredProperties.level2.prop2").asText(), "updated4");
  }

  @Test
  public void testHandleRemoveFieldsIdempotency() {
    // Test that applying handleRemoveFields multiple times gives same result
    ObjectNode previousDoc = JsonNodeFactory.instance.objectNode();
    previousDoc.put("field1", "value1");
    previousDoc.put("field2", "value2");

    ObjectNode newDoc = JsonNodeFactory.instance.objectNode();
    newDoc.put("field1", "updated1");

    ObjectNode result1 = SearchDocumentTransformer.handleRemoveFields(newDoc, previousDoc);
    ObjectNode result2 =
        SearchDocumentTransformer.handleRemoveFields(result1.deepCopy(), previousDoc);

    assertEquals(result1, result2);
    assertEquals(result2.get("field1").asText(), "updated1");
    assertTrue(result2.get("field2").isNull());
  }

  @Test
  public void testHandleRemoveFieldsPreservesNullValues() {
    // Test that existing null values in new doc are preserved
    ObjectNode previousDoc = JsonNodeFactory.instance.objectNode();
    previousDoc.put("field1", "value1");
    previousDoc.put("field2", "value2");

    ObjectNode newDoc = JsonNodeFactory.instance.objectNode();
    newDoc.put("field1", (String) null);
    newDoc.put("field3", (String) null);

    ObjectNode result = SearchDocumentTransformer.handleRemoveFields(newDoc, previousDoc);

    assertTrue(result.has("field1"));
    assertTrue(result.get("field1").isNull());
    assertTrue(result.has("field2"));
    assertTrue(result.get("field2").isNull());
    assertTrue(result.has("field3"));
    assertTrue(result.get("field3").isNull());
  }

  @Test
  public void testHandleRemoveFieldsWithEmptyStringValues() {
    // Test handling of empty string values
    ObjectNode previousDoc = JsonNodeFactory.instance.objectNode();
    previousDoc.put("field1", "value1");
    previousDoc.put("field2", "");
    previousDoc.put("field3", "value3");

    ObjectNode newDoc = JsonNodeFactory.instance.objectNode();
    newDoc.put("field1", "");
    newDoc.put("field3", "updated3");

    ObjectNode result = SearchDocumentTransformer.handleRemoveFields(newDoc, previousDoc);

    assertEquals(result.get("field1").asText(), "");
    assertTrue(result.has("field2"));
    assertTrue(result.get("field2").isNull());
    assertEquals(result.get("field3").asText(), "updated3");
  }

  @Test
  public void testHandleRemoveFieldsLargeDocument() {
    // Test with a large number of fields
    ObjectNode previousDoc = JsonNodeFactory.instance.objectNode();
    ObjectNode newDoc = JsonNodeFactory.instance.objectNode();

    // Add 100 fields to previous doc
    for (int i = 0; i < 100; i++) {
      previousDoc.put("field" + i, "value" + i);
    }

    // Only keep even numbered fields in new doc
    for (int i = 0; i < 100; i += 2) {
      newDoc.put("field" + i, "updated" + i);
    }

    ObjectNode result = SearchDocumentTransformer.handleRemoveFields(newDoc, previousDoc);

    // Check that even fields are updated
    for (int i = 0; i < 100; i += 2) {
      assertEquals(result.get("field" + i).asText(), "updated" + i);
    }

    // Check that odd fields are nulled
    for (int i = 1; i < 100; i += 2) {
      assertTrue(result.has("field" + i));
      assertTrue(result.get("field" + i).isNull());
    }
  }

  @Test
  public void testImprovedHandleRemoveFieldsWithNestedStructure() {
    // Test with complex nested structure
    ObjectNode previousDoc = JsonNodeFactory.instance.objectNode();

    // Create nested structure in previous
    ObjectNode level1 = JsonNodeFactory.instance.objectNode();
    level1.put("field1", "value1");
    level1.put("field2", "value2");

    ObjectNode level2 = JsonNodeFactory.instance.objectNode();
    level2.put("nestedField1", "nestedValue1");
    level2.put("nestedField2", "nestedValue2");
    level1.set("nested", level2);

    previousDoc.set("topLevel", level1);
    previousDoc.put("simpleField", "simpleValue");

    // New doc has some fields removed at various levels
    ObjectNode newDoc = JsonNodeFactory.instance.objectNode();
    ObjectNode newLevel1 = JsonNodeFactory.instance.objectNode();
    newLevel1.put("field1", "updatedValue1");
    // field2 is removed

    ObjectNode newLevel2 = JsonNodeFactory.instance.objectNode();
    newLevel2.put("nestedField1", "updatedNestedValue1");
    // nestedField2 is removed
    newLevel1.set("nested", newLevel2);

    newDoc.set("topLevel", newLevel1);
    // simpleField is removed

    ObjectNode result = SearchDocumentTransformer.handleRemoveFields(newDoc, previousDoc);

    // Verify structure
    assertEquals(result.get("topLevel").get("field1").asText(), "updatedValue1");
    assertTrue(result.get("topLevel").has("field2"));
    assertTrue(result.get("topLevel").get("field2").isNull());

    assertEquals(
        result.get("topLevel").get("nested").get("nestedField1").asText(), "updatedNestedValue1");
    assertTrue(result.get("topLevel").get("nested").has("nestedField2"));
    assertTrue(result.get("topLevel").get("nested").get("nestedField2").isNull());

    assertTrue(result.has("simpleField"));
    assertTrue(result.get("simpleField").isNull());
  }

  @Test
  public void testImprovedHandleRemoveFieldsWithDotNotation() {
    // Test that dot notation fields are handled correctly
    ObjectNode previousDoc = JsonNodeFactory.instance.objectNode();
    previousDoc.put("structuredProperties.prop1", "value1");
    previousDoc.put("structuredProperties.prop2", "value2");
    previousDoc.put("structuredProperties.prop3.nested", "value3");

    // Also add actual nested structure
    ObjectNode nested = JsonNodeFactory.instance.objectNode();
    nested.put("actualNested", "actualValue");
    previousDoc.set("normalNested", nested);

    ObjectNode newDoc = JsonNodeFactory.instance.objectNode();
    newDoc.put("structuredProperties.prop1", "updated1");
    // prop2 and prop3.nested removed

    // normalNested.actualNested is removed
    newDoc.set("normalNested", JsonNodeFactory.instance.objectNode());

    ObjectNode result = SearchDocumentTransformer.handleRemoveFields(newDoc, previousDoc);

    // Verify dot notation fields
    assertEquals(result.get("structuredProperties.prop1").asText(), "updated1");
    assertTrue(result.has("structuredProperties.prop2"));
    assertTrue(result.get("structuredProperties.prop2").isNull());
    assertTrue(result.has("structuredProperties.prop3.nested"));
    assertTrue(result.get("structuredProperties.prop3.nested").isNull());

    // Verify actual nested structure
    assertTrue(result.has("normalNested"));
    assertTrue(result.get("normalNested").has("actualNested"));
    assertTrue(result.get("normalNested").get("actualNested").isNull());
  }

  @Test
  public void testImprovedHandleRemoveFieldsTypeChange() {
    // Test when a field changes from object to primitive or vice versa
    ObjectNode previousDoc = JsonNodeFactory.instance.objectNode();

    ObjectNode wasObject = JsonNodeFactory.instance.objectNode();
    wasObject.put("inner1", "value1");
    wasObject.put("inner2", "value2");
    previousDoc.set("changesToString", wasObject);

    previousDoc.put("changesToObject", "wasString");

    ObjectNode newDoc = JsonNodeFactory.instance.objectNode();
    newDoc.put("changesToString", "nowString");

    ObjectNode nowObject = JsonNodeFactory.instance.objectNode();
    nowObject.put("newInner", "newValue");
    newDoc.set("changesToObject", nowObject);

    ObjectNode result = SearchDocumentTransformer.handleRemoveFields(newDoc, previousDoc);

    // When type changes from object to string, the nested fields should not be nulled
    // because the entire structure was replaced
    assertEquals(result.get("changesToString").asText(), "nowString");
    assertFalse(result.get("changesToString").isObject());

    // When type changes from string to object, it's a complete replacement
    assertTrue(result.get("changesToObject").isObject());
    assertEquals(result.get("changesToObject").get("newInner").asText(), "newValue");
  }

  @Test
  public void testImprovedHandleRemoveFieldsDeepNesting() {
    // Test with very deep nesting
    ObjectNode previousDoc = JsonNodeFactory.instance.objectNode();
    ObjectNode current = previousDoc;

    // Create deep structure: a.b.c.d.e = "deep"
    for (String level : new String[] {"a", "b", "c", "d"}) {
      ObjectNode next = JsonNodeFactory.instance.objectNode();
      current.set(level, next);
      current = next;
    }
    current.put("e", "deepValue");
    current.put("f", "anotherDeepValue");

    // New doc removes 'f' at the deepest level
    ObjectNode newDoc = JsonNodeFactory.instance.objectNode();
    current = newDoc;
    for (String level : new String[] {"a", "b", "c", "d"}) {
      ObjectNode next = JsonNodeFactory.instance.objectNode();
      current.set(level, next);
      current = next;
    }
    current.put("e", "updatedDeepValue");

    ObjectNode result = SearchDocumentTransformer.handleRemoveFields(newDoc, previousDoc);

    // Navigate to deep level and check
    JsonNode deepNode = result.get("a").get("b").get("c").get("d");
    assertEquals(deepNode.get("e").asText(), "updatedDeepValue");
    assertTrue(deepNode.has("f"));
    assertTrue(deepNode.get("f").isNull());
  }
}
