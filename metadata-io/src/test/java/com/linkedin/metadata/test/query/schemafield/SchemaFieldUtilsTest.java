package com.linkedin.metadata.test.query.schemafield;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.test.query.TestQuery;
import java.util.Collections;
import org.testng.annotations.Test;

public class SchemaFieldUtilsTest {

  @Test
  public void testIsSchemaFieldsQueryTrueForSchemaFields() {
    TestQuery query = mock(TestQuery.class);
    when(query.getQueryParts())
        .thenReturn(Collections.singletonList(SchemaFieldUtils.SCHEMA_FIELDS_PROPERTY));
    when(query.getQuery()).thenReturn(SchemaFieldUtils.SCHEMA_FIELDS_PROPERTY);

    assertTrue(SchemaFieldUtils.isSchemaFieldsQuery(query));
  }

  @Test
  public void testIsSchemaFieldsQueryTrueForSchemaFieldsLength() {
    TestQuery query = mock(TestQuery.class);
    when(query.getQueryParts())
        .thenReturn(Collections.singletonList(SchemaFieldUtils.SCHEMA_FIELDS_LENGTH_PROPERTY));
    when(query.getQuery()).thenReturn(SchemaFieldUtils.SCHEMA_FIELDS_LENGTH_PROPERTY);

    assertTrue(SchemaFieldUtils.isSchemaFieldsQuery(query));
  }

  @Test
  public void testIsSchemaFieldsQueryFalseForOtherQuery() {
    TestQuery query = mock(TestQuery.class);
    when(query.getQueryParts()).thenReturn(Collections.singletonList("otherQuery"));
    when(query.getQuery()).thenReturn("otherQuery");

    assertFalse(SchemaFieldUtils.isSchemaFieldsQuery(query));
  }

  @Test
  public void testIsSchemaFieldsQueryFalseWhenEmptyQueryParts() {
    TestQuery query = mock(TestQuery.class);
    when(query.getQueryParts()).thenReturn(Collections.emptyList());

    assertFalse(SchemaFieldUtils.isSchemaFieldsQuery(query));
  }

  @Test
  public void testSerializeSchemaField() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    SchemaField schemaField = new SchemaField("path", "desc", "editableDesc");
    String expectedJson = mapper.writeValueAsString(schemaField);

    assertEquals(SchemaFieldUtils.serializeSchemaField(schemaField), expectedJson);
  }

  @Test
  public void testDeserializeSchemaField() throws Exception {
    String json =
        "{\"path\":\"path\",\"description\":\"desc\",\"editableDescription\":\"editableDesc\"}";
    SchemaField expectedField = new SchemaField("path", "desc", "editableDesc");

    SchemaField resultField = SchemaFieldUtils.deserializeSchemaField(json);
    assertEquals(resultField.getPath(), expectedField.getPath());
    assertEquals(resultField.getDescription(), expectedField.getDescription());
    assertEquals(resultField.getEditableDescription(), expectedField.getEditableDescription());
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testDeserializeSchemaFieldException() {
    SchemaFieldUtils.deserializeSchemaField("invalid json");
  }
}
