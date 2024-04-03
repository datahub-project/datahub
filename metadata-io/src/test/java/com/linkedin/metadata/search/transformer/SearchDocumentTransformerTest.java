package com.linkedin.metadata.search.transformer;

import static com.linkedin.metadata.Constants.*;
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
import com.linkedin.metadata.TestEntitySpecBuilder;
import com.linkedin.metadata.TestEntityUtil;
import com.linkedin.metadata.models.EntitySpec;
import java.io.IOException;
import java.util.Optional;
import org.testng.annotations.Test;

public class SearchDocumentTransformerTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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
    assertEquals(parsedJson.get("esObjectField"), JsonNodeFactory.instance.arrayNode().add("123"));

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
            .add("0123456789"));
  }
}
