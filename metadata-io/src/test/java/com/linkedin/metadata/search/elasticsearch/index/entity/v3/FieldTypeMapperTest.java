package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.search.utils.ESUtils.KEYWORD_IGNORE_ABOVE;
import static org.testng.Assert.*;

import com.linkedin.metadata.models.annotation.SearchableAnnotation.FieldType;
import java.util.Map;
import org.testng.annotations.Test;

public class FieldTypeMapperTest {

  @Test
  public void testTextFieldUsesIgnoreAbove() {
    Map<String, Object> mapping = FieldTypeMapper.getMappingsForFieldType(FieldType.TEXT);
    assertEquals(mapping.get("type"), "keyword");
    assertEquals(mapping.get("ignore_above"), KEYWORD_IGNORE_ABOVE);
  }

  @Test
  public void testTextPartialFieldUsesIgnoreAbove() {
    Map<String, Object> mapping = FieldTypeMapper.getMappingsForFieldType(FieldType.TEXT_PARTIAL);
    assertEquals(mapping.get("type"), "keyword");
    assertEquals(mapping.get("ignore_above"), KEYWORD_IGNORE_ABOVE);
  }

  @Test
  public void testPlainKeywordFieldDoesNotHaveIgnoreAbove() {
    Map<String, Object> mapping = FieldTypeMapper.getMappingsForFieldType(FieldType.KEYWORD);
    assertEquals(mapping.get("type"), "keyword");
    assertFalse(mapping.containsKey("ignore_above"));
  }

  @Test
  public void testGetMappingsForKeywordWithIgnoreAbove() {
    Map<String, Object> mapping = FieldTypeMapper.getMappingsForKeywordWithIgnoreAbove();
    assertEquals(mapping.get("type"), "keyword");
    assertEquals(mapping.get("ignore_above"), KEYWORD_IGNORE_ABOVE);
  }

  @Test
  public void testGetMappingsForKeywordWithConfiguredIgnoreAbove() {
    Map<String, Object> mapping = FieldTypeMapper.getMappingsForKeywordWithIgnoreAbove(1024);
    assertEquals(mapping.get("type"), "keyword");
    // Configured value is UTF-8 bytes; ignore_above is character-based and byte-safe (/ 4).
    assertEquals(mapping.get("ignore_above"), 256);
  }

  @Test
  public void testStringLogicalValueTypeHonorsConfiguredKeywordMaxLength() {
    Map<String, Object> mapping =
        FieldTypeMapper.getMappingsForLogicalValueType(
            com.linkedin.metadata.models.LogicalValueType.STRING, 2048);
    assertEquals(mapping.get("type"), "keyword");
    assertEquals(mapping.get("ignore_above"), 512);
  }

  @Test
  public void testGetMappingsForUrn() {
    Map<String, Object> mapping = FieldTypeMapper.getMappingsForUrn();
    assertEquals(mapping.get("type"), "keyword");
    assertEquals(mapping.get("ignore_above"), 255);
  }

  @Test
  public void testStringLogicalValueTypeUsesIgnoreAbove() {
    Map<String, Object> mapping =
        FieldTypeMapper.getMappingsForLogicalValueType(
            com.linkedin.metadata.models.LogicalValueType.STRING);
    assertEquals(mapping.get("type"), "keyword");
    assertEquals(mapping.get("ignore_above"), KEYWORD_IGNORE_ABOVE);
  }

  @Test
  public void testRichTextLogicalValueTypeUsesIgnoreAbove() {
    Map<String, Object> mapping =
        FieldTypeMapper.getMappingsForLogicalValueType(
            com.linkedin.metadata.models.LogicalValueType.RICH_TEXT);
    assertEquals(mapping.get("type"), "keyword");
    assertEquals(mapping.get("ignore_above"), KEYWORD_IGNORE_ABOVE);
  }

  @Test
  public void testBooleanFieldType() {
    Map<String, Object> mapping = FieldTypeMapper.getMappingsForFieldType(FieldType.BOOLEAN);
    assertEquals(mapping.get("type"), "boolean");
  }

  @Test
  public void testCountFieldType() {
    Map<String, Object> mapping = FieldTypeMapper.getMappingsForFieldType(FieldType.COUNT);
    assertEquals(mapping.get("type"), "long");
  }

  @Test
  public void testDatetimeFieldType() {
    Map<String, Object> mapping = FieldTypeMapper.getMappingsForFieldType(FieldType.DATETIME);
    assertEquals(mapping.get("type"), "date");
  }

  @Test
  public void testDoubleFieldType() {
    Map<String, Object> mapping = FieldTypeMapper.getMappingsForFieldType(FieldType.DOUBLE);
    assertEquals(mapping.get("type"), "double");
  }
}
