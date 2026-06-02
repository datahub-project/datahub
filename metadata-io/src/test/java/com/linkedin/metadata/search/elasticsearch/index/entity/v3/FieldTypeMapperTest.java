package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.search.utils.ESUtils.KEYWORD_MAXLENGTH;
import static org.testng.Assert.*;

import com.linkedin.metadata.models.annotation.SearchableAnnotation.FieldType;
import java.util.Map;
import org.testng.annotations.Test;

public class FieldTypeMapperTest {

  @Test
  public void testTextFieldUsesIgnoreAbove() {
    Map<String, Object> mapping = FieldTypeMapper.getMappingsForFieldType(FieldType.TEXT);
    assertEquals(mapping.get("type"), "keyword");
    assertEquals(mapping.get("ignore_above"), KEYWORD_MAXLENGTH);
  }

  @Test
  public void testTextPartialFieldUsesIgnoreAbove() {
    Map<String, Object> mapping = FieldTypeMapper.getMappingsForFieldType(FieldType.TEXT_PARTIAL);
    assertEquals(mapping.get("type"), "keyword");
    assertEquals(mapping.get("ignore_above"), KEYWORD_MAXLENGTH);
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
    assertEquals(mapping.get("ignore_above"), KEYWORD_MAXLENGTH);
  }

  @Test
  public void testGetMappingsForUrn() {
    Map<String, Object> mapping = FieldTypeMapper.getMappingsForUrn();
    assertEquals(mapping.get("type"), "keyword");
    assertEquals(mapping.get("ignore_above"), 255);
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
