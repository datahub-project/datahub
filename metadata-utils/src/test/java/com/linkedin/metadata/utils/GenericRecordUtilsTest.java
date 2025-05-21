package com.linkedin.metadata.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.data.ByteString;
import com.linkedin.data.DataList;
import com.linkedin.data.DataMap;
import com.linkedin.data.schema.ArrayDataSchema;
import com.linkedin.data.schema.BooleanDataSchema;
import com.linkedin.data.schema.FloatDataSchema;
import com.linkedin.data.schema.IntegerDataSchema;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.StringDataSchema;
import com.linkedin.data.template.DynamicRecordTemplate;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.mxe.GenericAspect;
import jakarta.json.Json;
import jakarta.json.JsonPatch;
import jakarta.json.JsonPatchBuilder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.testng.annotations.Test;

public class GenericRecordUtilsTest {
  private final ObjectMapper objectMapper =
      new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);

  private final String expectedPathStr =
      "[{\"op\":\"add\",\"path\":\"/name\",\"value\":\"John Doe\"},{\"op\":\"replace\",\"path\":\"/age\",\"value\":30},{\"op\":\"remove\",\"path\":\"/address\"}]";

  @Test
  public void testSerializePatchWithJsonPatch() throws Exception {
    // Create a JsonPatch instance
    JsonPatchBuilder patchBuilder = Json.createPatchBuilder();
    patchBuilder.add("/name", "John Doe");
    patchBuilder.replace("/age", 30);
    patchBuilder.remove("/address");
    JsonPatch jsonPatch = patchBuilder.build();

    assertEquals(jsonPatch.toString(), expectedPathStr);

    // Call the method under test
    GenericAspect result = GenericRecordUtils.serializePatch(jsonPatch);

    // Verify the result
    assertNotNull(result);
    assertEquals(result.getContentType(), GenericRecordUtils.JSON_PATCH);

    // Verify the serialized content
    String patchContent = result.getValue().asString(StandardCharsets.UTF_8);
    assertNotNull(patchContent);

    // Verify the patch content contains expected operations
    // (exact format may vary based on JsonPatch implementation)
    String expectedContent = jsonPatch.toString();
    assertEquals(expectedContent, patchContent);
  }

  @Test
  public void testSerializePatchWithJsonNode() throws Exception {
    // Create a JsonNode representing a patch
    String patchJson =
        "["
            + "{\"op\":\"add\",\"path\":\"/name\",\"value\":\"John Doe\"},"
            + "{\"op\":\"replace\",\"path\":\"/age\",\"value\":30},"
            + "{\"op\":\"remove\",\"path\":\"/address\"}"
            + "]";
    JsonNode jsonNode = objectMapper.readTree(patchJson);
    assertEquals(jsonNode.toString(), expectedPathStr);

    // Call the method under test
    GenericAspect result = GenericRecordUtils.serializePatch(jsonNode);

    // Verify the result
    assertNotNull(result);
    assertEquals(result.getContentType(), GenericRecordUtils.JSON_PATCH);

    // Verify the serialized content
    String patchContent = result.getValue().asString(StandardCharsets.UTF_8);
    assertNotNull(patchContent);

    // Verify the patch content matches the expected JSON
    assertEquals(jsonNode.toString(), patchContent);
  }

  @Test
  public void testSerializePatchWithGenericJsonPatch() throws Exception {
    GenericJsonPatch.PatchOp addOp = new GenericJsonPatch.PatchOp();
    addOp.setOp("add");
    addOp.setPath("/name");
    addOp.setValue("John Doe");

    GenericJsonPatch.PatchOp replaceOp = new GenericJsonPatch.PatchOp();
    replaceOp.setOp("replace");
    replaceOp.setPath("/age");
    replaceOp.setValue(30);

    GenericJsonPatch.PatchOp removeOp = new GenericJsonPatch.PatchOp();
    removeOp.setOp("remove");
    removeOp.setPath("/address");

    GenericJsonPatch genericJsonPatch =
        GenericJsonPatch.builder().patch(List.of(addOp, replaceOp, removeOp)).build();

    // Call the method under test
    GenericAspect result = GenericRecordUtils.serializePatch(genericJsonPatch, objectMapper);

    // Verify the result
    assertNotNull(result);
    assertEquals(result.getContentType(), GenericRecordUtils.JSON_PATCH);

    // Verify the serialized content
    String patchContent = result.getValue().asString(StandardCharsets.UTF_8);
    assertNotNull(patchContent);

    // Verify the patch content matches
    assertEquals(
        patchContent,
        String.format(
            "{\"arrayPrimaryKeys\":{},\"patch\":%s,\"forceGenericPatch\":false}", expectedPathStr));
  }

  @Test
  public void testSerializePatchContentTypeIsCorrect() throws Exception {
    // This test specifically verifies that the content type is set correctly
    JsonPatchBuilder patchBuilder = Json.createPatchBuilder();
    patchBuilder.add("/test", "value");
    JsonPatch jsonPatch = patchBuilder.build();

    GenericAspect result = GenericRecordUtils.serializePatch(jsonPatch);

    assertEquals(result.getContentType(), GenericRecordUtils.JSON_PATCH);
    assertEquals(result.getContentType(), "application/json-patch+json");
  }

  @Test
  public void testSerializePatchWithEmptyPatch() throws Exception {
    // Test with an empty patch
    JsonPatch emptyPatch = Json.createPatchBuilder().build();

    GenericAspect result = GenericRecordUtils.serializePatch(emptyPatch);

    assertNotNull(result);
    assertEquals(result.getContentType(), GenericRecordUtils.JSON_PATCH);

    String patchContent = result.getValue().asString(StandardCharsets.UTF_8);
    assertEquals(emptyPatch.toString(), patchContent);
  }

  @Test
  public void testSerializePatchByteStringEncoding() throws Exception {
    // Test that the ByteString is correctly encoded
    String patchJson = "[{\"op\":\"add\",\"path\":\"/test\",\"value\":\"value\"}]";
    JsonNode jsonNode = objectMapper.readTree(patchJson);

    GenericAspect result = GenericRecordUtils.serializePatch(jsonNode);

    ByteString byteString = result.getValue();
    byte[] bytes = patchJson.getBytes(StandardCharsets.UTF_8);

    // Compare the byte representation of the patch
    String resultString = new String(byteString.copyBytes(), StandardCharsets.UTF_8);
    assertEquals(jsonNode.toString(), resultString);
  }

  @Test
  public void testSerializePatchWithSpecialCharacters() throws Exception {
    // Test with JSON containing special characters
    String specialJson =
        "[{\"op\":\"add\",\"path\":\"/message\",\"value\":\"Special characters: áéíóú ñ €\"}]";
    JsonNode jsonNode = objectMapper.readTree(specialJson);

    GenericAspect result = GenericRecordUtils.serializePatch(jsonNode);

    String resultString = result.getValue().asString(StandardCharsets.UTF_8);
    assertEquals(jsonNode.toString(), resultString);
  }

  @Test
  public void testFromJsonWithSimpleTypes() throws Exception {
    // Create a JSON object with various simple types
    ObjectNode rootNode = objectMapper.createObjectNode();
    rootNode.put("stringField", "test string");
    rootNode.put("intField", 42);
    rootNode.put("floatField", 3.14);
    rootNode.put("booleanField", true);
    rootNode.putNull("nullField");

    // Convert to DynamicRecordTemplate
    DynamicRecordTemplate result = GenericRecordUtils.fromJson(rootNode, "SimpleTypeSchema");

    // Verify the schema
    RecordDataSchema schema = result.schema();
    assertEquals(schema.getName(), "SimpleTypeSchema");
    assertEquals(schema.getFields().size(), 5);

    // Verify field types
    assertTrue(schema.getField("stringField").getType() instanceof StringDataSchema);
    assertTrue(schema.getField("intField").getType() instanceof IntegerDataSchema);
    assertTrue(schema.getField("floatField").getType() instanceof FloatDataSchema);
    assertTrue(schema.getField("booleanField").getType() instanceof BooleanDataSchema);
    assertTrue(schema.getField("nullField").getType() instanceof StringDataSchema);
    assertTrue(schema.getField("nullField").getOptional());

    // Verify data values
    DataMap dataMap = result.data();
    assertEquals(dataMap.getString("stringField"), "test string");
    assertEquals(dataMap.getInteger("intField"), Integer.valueOf(42));
    assertEquals(dataMap.getDouble("floatField"), Double.valueOf(3.14));
    assertEquals(dataMap.getBoolean("booleanField"), Boolean.TRUE);
    assertNull(dataMap.get("nullField"));
  }

  @Test
  public void testFromJsonWithNestedObject() throws Exception {
    // Create a JSON object with a nested object
    ObjectNode addressNode = objectMapper.createObjectNode();
    addressNode.put("street", "123 Main St");
    addressNode.put("city", "San Francisco");
    addressNode.put("zipCode", 94105);

    ObjectNode rootNode = objectMapper.createObjectNode();
    rootNode.put("name", "John Doe");
    rootNode.set("address", addressNode);

    // Convert to DynamicRecordTemplate
    DynamicRecordTemplate result = GenericRecordUtils.fromJson(rootNode, "PersonSchema");

    // Verify the schema
    RecordDataSchema schema = result.schema();
    assertEquals(schema.getName(), "PersonSchema");
    assertEquals(schema.getFields().size(), 2);

    // Verify the nested object field
    RecordDataSchema.Field addressField = schema.getField("address");
    assertTrue(addressField.getType() instanceof RecordDataSchema);

    RecordDataSchema addressSchema = (RecordDataSchema) addressField.getType();
    assertEquals(addressSchema.getName(), "addressRecord");
    assertEquals(addressSchema.getFields().size(), 3);

    // Verify data
    DataMap dataMap = result.data();
    assertEquals(dataMap.getString("name"), "John Doe");

    DataMap addressData = dataMap.getDataMap("address");
    assertEquals(addressData.getString("street"), "123 Main St");
    assertEquals(addressData.getString("city"), "San Francisco");
    assertEquals(addressData.getInteger("zipCode"), Integer.valueOf(94105));
  }

  @Test
  public void testFromJsonWithArrays() throws Exception {
    // Create a JSON object with arrays
    ArrayNode phoneNumbers = objectMapper.createArrayNode();
    phoneNumbers.add("555-1234");
    phoneNumbers.add("555-5678");

    ArrayNode scores = objectMapper.createArrayNode();
    scores.add(85);
    scores.add(92);
    scores.add(78);

    ObjectNode rootNode = objectMapper.createObjectNode();
    rootNode.put("name", "Jane Smith");
    rootNode.set("phoneNumbers", phoneNumbers);
    rootNode.set("scores", scores);

    // Convert to DynamicRecordTemplate
    DynamicRecordTemplate result = GenericRecordUtils.fromJson(rootNode, "ContactSchema");

    // Verify the schema
    RecordDataSchema schema = result.schema();
    assertEquals(schema.getName(), "ContactSchema");
    assertEquals(schema.getFields().size(), 3);

    // Verify array fields
    RecordDataSchema.Field phonesField = schema.getField("phoneNumbers");
    assertTrue(phonesField.getType() instanceof ArrayDataSchema);
    assertTrue(((ArrayDataSchema) phonesField.getType()).getItems() instanceof StringDataSchema);

    RecordDataSchema.Field scoresField = schema.getField("scores");
    assertTrue(scoresField.getType() instanceof ArrayDataSchema);
    assertTrue(((ArrayDataSchema) scoresField.getType()).getItems() instanceof IntegerDataSchema);

    // Verify data
    DataMap dataMap = result.data();
    assertEquals(dataMap.getString("name"), "Jane Smith");

    DataList phonesList = dataMap.getDataList("phoneNumbers");
    assertEquals(phonesList.size(), 2);
    assertEquals(phonesList.get(0), "555-1234");
    assertEquals(phonesList.get(1), "555-5678");

    DataList scoresList = dataMap.getDataList("scores");
    assertEquals(scoresList.size(), 3);
    assertEquals(scoresList.get(0), Integer.valueOf(85));
    assertEquals(scoresList.get(1), Integer.valueOf(92));
    assertEquals(scoresList.get(2), Integer.valueOf(78));
  }

  @Test
  public void testFromJsonWithEmptyArray() throws Exception {
    // Create a JSON object with an empty array
    ObjectNode rootNode = objectMapper.createObjectNode();
    rootNode.set("tags", objectMapper.createArrayNode());

    // Convert to DynamicRecordTemplate
    DynamicRecordTemplate result = GenericRecordUtils.fromJson(rootNode, "EmptyArraySchema");

    // Verify the schema
    RecordDataSchema schema = result.schema();
    assertEquals(schema.getName(), "EmptyArraySchema");
    assertEquals(schema.getFields().size(), 1);

    // Verify empty array field - should default to string items
    RecordDataSchema.Field tagsField = schema.getField("tags");
    assertTrue(tagsField.getType() instanceof ArrayDataSchema);
    assertTrue(((ArrayDataSchema) tagsField.getType()).getItems() instanceof StringDataSchema);

    // Verify data
    DataMap dataMap = result.data();
    DataList tagsList = dataMap.getDataList("tags");
    assertEquals(tagsList.size(), 0);
  }

  @Test
  public void testFromJsonWithComplexNestedStructure() throws Exception {
    // Create a complex nested JSON structure
    ObjectNode addressNode = objectMapper.createObjectNode();
    addressNode.put("street", "456 Market St");
    addressNode.put("city", "San Francisco");

    ArrayNode phoneNumbers = objectMapper.createArrayNode();

    ObjectNode homePhone = objectMapper.createObjectNode();
    homePhone.put("type", "home");
    homePhone.put("number", "555-1234");

    ObjectNode workPhone = objectMapper.createObjectNode();
    workPhone.put("type", "work");
    workPhone.put("number", "555-5678");

    phoneNumbers.add(homePhone);
    phoneNumbers.add(workPhone);

    ObjectNode rootNode = objectMapper.createObjectNode();
    rootNode.put("id", 1001);
    rootNode.put("name", "Alice Johnson");
    rootNode.put("active", true);
    rootNode.set("address", addressNode);
    rootNode.set("phoneNumbers", phoneNumbers);

    // Convert to DynamicRecordTemplate
    DynamicRecordTemplate result = GenericRecordUtils.fromJson(rootNode, "ComplexSchema");

    // Verify the schema
    RecordDataSchema schema = result.schema();
    assertEquals(schema.getName(), "ComplexSchema");
    assertEquals(schema.getFields().size(), 5);

    // Verify the nested structures in schema
    assertTrue(schema.getField("address").getType() instanceof RecordDataSchema);
    assertTrue(schema.getField("phoneNumbers").getType() instanceof ArrayDataSchema);

    ArrayDataSchema phoneArraySchema = (ArrayDataSchema) schema.getField("phoneNumbers").getType();
    assertTrue(phoneArraySchema.getItems() instanceof RecordDataSchema);

    // Verify data
    DataMap dataMap = result.data();
    assertEquals(dataMap.getInteger("id"), Integer.valueOf(1001));
    assertEquals(dataMap.getString("name"), "Alice Johnson");
    assertEquals(dataMap.getBoolean("active"), Boolean.TRUE);

    DataMap addressData = dataMap.getDataMap("address");
    assertEquals(addressData.getString("street"), "456 Market St");
    assertEquals(addressData.getString("city"), "San Francisco");

    DataList phoneList = dataMap.getDataList("phoneNumbers");
    assertEquals(phoneList.size(), 2);

    DataMap homePhoneData = (DataMap) phoneList.get(0);
    assertEquals(homePhoneData.getString("type"), "home");
    assertEquals(homePhoneData.getString("number"), "555-1234");

    DataMap workPhoneData = (DataMap) phoneList.get(1);
    assertEquals(workPhoneData.getString("type"), "work");
    assertEquals(workPhoneData.getString("number"), "555-5678");
  }

  @Test
  public void testFromJsonWithEmptyObject() throws Exception {
    // Create an empty JSON object
    ObjectNode rootNode = objectMapper.createObjectNode();

    // Convert to DynamicRecordTemplate
    DynamicRecordTemplate result = GenericRecordUtils.fromJson(rootNode, "EmptySchema");

    // Verify the schema has no fields
    RecordDataSchema schema = result.schema();
    assertEquals(schema.getName(), "EmptySchema");
    assertEquals(schema.getFields().size(), 0);

    // Verify data is empty
    DataMap dataMap = result.data();
    assertTrue(dataMap.isEmpty());
  }

  @Test
  public void testFromJsonWithSpecialCharacters() throws Exception {
    // Create a JSON with special characters in field names
    ObjectNode rootNode = objectMapper.createObjectNode();
    rootNode.put("normal_field", "normal value");
    rootNode.put("field-with-dashes", "dashed value");
    rootNode.put("field.with.dots", "dotted value");

    // Convert to DynamicRecordTemplate
    DynamicRecordTemplate result = GenericRecordUtils.fromJson(rootNode, "SpecialCharsSchema");

    // Verify schema and data
    // Note: Some special characters might be transformed or cause warnings during schema creation
    RecordDataSchema schema = result.schema();
    DataMap dataMap = result.data();

    // Fields that should definitely exist
    assertTrue(dataMap.containsKey("normal_field"));
    assertEquals(dataMap.getString("normal_field"), "normal value");

    // Check if other fields exist - they may have been transformed or skipped
    if (dataMap.containsKey("field-with-dashes")) {
      assertEquals(dataMap.getString("field-with-dashes"), "dashed value");
    }

    if (dataMap.containsKey("field.with.dots")) {
      assertEquals(dataMap.getString("field.with.dots"), "dotted value");
    }
  }
}
