package com.linkedin.metadata.test.query.schemafield;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.test.query.TestQuery;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.Test;

public class SchemaFieldUtilsTest {

  @Test
  public void testIsSchemaFieldsQueryTrueForSchemaFields() {
    TestQuery query = mock(TestQuery.class);
    when(query.getQueryParts())
        .thenReturn(Collections.singletonList(TestsSchemaFieldUtils.SCHEMA_FIELDS_PROPERTY));
    when(query.getQuery()).thenReturn(TestsSchemaFieldUtils.SCHEMA_FIELDS_PROPERTY);

    assertTrue(TestsSchemaFieldUtils.isSchemaFieldsQuery(query));
  }

  @Test
  public void testIsSchemaFieldsQueryTrueForSchemaFieldsLength() {
    TestQuery query = mock(TestQuery.class);
    when(query.getQueryParts())
        .thenReturn(Collections.singletonList(TestsSchemaFieldUtils.SCHEMA_FIELDS_LENGTH_PROPERTY));
    when(query.getQuery()).thenReturn(TestsSchemaFieldUtils.SCHEMA_FIELDS_LENGTH_PROPERTY);

    assertTrue(TestsSchemaFieldUtils.isSchemaFieldsQuery(query));
  }

  @Test
  public void testIsSchemaFieldsQueryFalseForOtherQuery() {
    TestQuery query = mock(TestQuery.class);
    when(query.getQueryParts()).thenReturn(Collections.singletonList("otherQuery"));
    when(query.getQuery()).thenReturn("otherQuery");

    assertFalse(TestsSchemaFieldUtils.isSchemaFieldsQuery(query));
  }

  @Test
  public void testIsSchemaFieldsQueryFalseWhenEmptyQueryParts() {
    TestQuery query = mock(TestQuery.class);
    when(query.getQueryParts()).thenReturn(Collections.emptyList());

    assertFalse(TestsSchemaFieldUtils.isSchemaFieldsQuery(query));
  }

  @Test
  public void testSerializeSchemaField() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    SchemaField schemaField = new SchemaField("path", "desc", "editableDesc", null);
    String expectedJson = mapper.writeValueAsString(schemaField);

    assertEquals(TestsSchemaFieldUtils.serializeSchemaField(schemaField), expectedJson);
  }

  @Test
  public void testDeserializeSchemaField() throws Exception {
    String json =
        "{\"path\":\"path\",\"description\":\"desc\",\"editableDescription\":\"editableDesc\"}";
    SchemaField expectedField = new SchemaField("path", "desc", "editableDesc", null);

    SchemaField resultField = TestsSchemaFieldUtils.deserializeSchemaField(json);
    assertEquals(resultField.getPath(), expectedField.getPath());
    assertEquals(resultField.getDescription(), expectedField.getDescription());
    assertEquals(resultField.getEditableDescription(), expectedField.getEditableDescription());
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testDeserializeSchemaFieldException() {
    TestsSchemaFieldUtils.deserializeSchemaField("invalid json");
  }

  @Test
  public void testSerializeSchemaFieldWithDocumentation() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    List<String> documentation =
        Arrays.asList("This is AI-generated documentation", "Another doc string");
    SchemaField schemaField = new SchemaField("path", "desc", "editableDesc", documentation);
    String expectedJson = mapper.writeValueAsString(schemaField);

    assertEquals(TestsSchemaFieldUtils.serializeSchemaField(schemaField), expectedJson);
  }

  @Test
  public void testDeserializeSchemaFieldWithDocumentation() throws Exception {
    List<String> documentation = Arrays.asList("This is propagated documentation");
    SchemaField expectedField = new SchemaField("path", "desc", "editableDesc", documentation);
    String json = TestsSchemaFieldUtils.serializeSchemaField(expectedField);

    SchemaField resultField = TestsSchemaFieldUtils.deserializeSchemaField(json);
    assertEquals(resultField.getPath(), expectedField.getPath());
    assertEquals(resultField.getDescription(), expectedField.getDescription());
    assertEquals(resultField.getEditableDescription(), expectedField.getEditableDescription());
    assertNotNull(resultField.getDocumentation());
    assertEquals(resultField.getDocumentation().size(), 1);
    assertEquals(resultField.getDocumentation().get(0), "This is propagated documentation");
  }

  @Test
  public void testSerializeSchemaFieldWithEmptyDocumentation() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    List<String> documentation = Arrays.asList(""); // Empty documentation
    SchemaField schemaField = new SchemaField("path", "desc", "editableDesc", documentation);
    String expectedJson = mapper.writeValueAsString(schemaField);

    assertEquals(TestsSchemaFieldUtils.serializeSchemaField(schemaField), expectedJson);
  }

  @Test
  public void testSerializeSchemaFieldWithNullDocumentation() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    SchemaField schemaField = new SchemaField("path", "desc", "editableDesc", null);
    String expectedJson = mapper.writeValueAsString(schemaField);

    assertEquals(TestsSchemaFieldUtils.serializeSchemaField(schemaField), expectedJson);
  }

  @Test
  public void testDeserializeSchemaFieldWithNullDocumentation() throws Exception {
    String json =
        "{\"path\":\"path\",\"description\":\"desc\",\"editableDescription\":\"editableDesc\",\"documentation\":null}";
    SchemaField expectedField = new SchemaField("path", "desc", "editableDesc", null);

    SchemaField resultField = TestsSchemaFieldUtils.deserializeSchemaField(json);
    assertEquals(resultField.getPath(), expectedField.getPath());
    assertEquals(resultField.getDescription(), expectedField.getDescription());
    assertEquals(resultField.getEditableDescription(), expectedField.getEditableDescription());
    assertNull(resultField.getDocumentation());
  }
}
