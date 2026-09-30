package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.search.utils.ESUtils.KEYWORD_MAXLENGTH;
import static com.linkedin.metadata.search.utils.ESUtils.keywordIgnoreAboveForMaxBytes;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.linkedin.data.schema.PathSpec;
import com.linkedin.metadata.models.LogicalValueType;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.models.annotation.SearchableAnnotation.FieldType;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

public class FieldTypeMapperTest {

  @Test
  public void testTextFieldUsesIgnoreAbove() {
    Map<String, Object> mapping = FieldTypeMapper.getMappingsForFieldType(FieldType.TEXT);
    assertEquals(mapping.get("type"), "keyword");
    assertEquals(mapping.get("ignore_above"), KEYWORD_MAXLENGTH);
    Map<String, Object> fields = getFields(mapping);
    assertTrue(fields.containsKey("delimited"));
    assertTrue(fields.containsKey("keyword"));
  }

  @Test
  public void testTextPartialFieldUsesIgnoreAbove() {
    Map<String, Object> mapping = FieldTypeMapper.getMappingsForFieldType(FieldType.TEXT_PARTIAL);
    assertEquals(mapping.get("type"), "keyword");
    assertEquals(mapping.get("ignore_above"), KEYWORD_MAXLENGTH);
    Map<String, Object> fields = getFields(mapping);
    assertTrue(fields.containsKey("delimited"));
    assertTrue(fields.containsKey("keyword"));
    assertEquals(((Map<String, Object>) fields.get("ngram")).get("analyzer"), "partial");
  }

  @Test
  public void testWordGramFieldUsesV2CompatibleSubfields() {
    Map<String, Object> mapping = FieldTypeMapper.getMappingsForFieldType(FieldType.WORD_GRAM);
    Map<String, Object> fields = getFields(mapping);

    assertTrue(fields.containsKey("delimited"));
    assertTrue(fields.containsKey("ngram"));
    assertEquals(((Map<String, Object>) fields.get("wordGrams2")).get("analyzer"), "word_gram_2");
    assertEquals(((Map<String, Object>) fields.get("wordGrams3")).get("analyzer"), "word_gram_3");
    assertEquals(((Map<String, Object>) fields.get("wordGrams4")).get("analyzer"), "word_gram_4");
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
    assertEquals(mapping.get("ignore_above"), keywordIgnoreAboveForMaxBytes(KEYWORD_MAXLENGTH));
    // Parent and .keyword sub-field must both be byte-safe so an oversized value is skipped rather
    // than failing the whole document write on Lucene's 32,766-byte term limit.
    Map<String, Object> fields = getFields(mapping);
    assertTrue(fields.containsKey("keyword"), "must keep the .keyword sub-field");
    assertEquals(
        ((Map<String, Object>) fields.get("keyword")).get("ignore_above"),
        keywordIgnoreAboveForMaxBytes(KEYWORD_MAXLENGTH));
  }

  @Test
  public void testStringLogicalValueTypeIsByteSafe() {
    // Structured-property STRING values route here; they must not fail the whole document write on
    // Lucene's 32,766-byte term limit (regression guard — see getMappingsForLogicalValueType).
    assertKeywordByteSafe(FieldTypeMapper.getMappingsForLogicalValueType(LogicalValueType.STRING));
  }

  @Test
  public void testRichTextLogicalValueTypeIsByteSafe() {
    assertKeywordByteSafe(
        FieldTypeMapper.getMappingsForLogicalValueType(LogicalValueType.RICH_TEXT));
  }

  private void assertKeywordByteSafe(Map<String, Object> mapping) {
    // Full fix contract: exact-match filtering is preserved (keyword type + normalizer) AND the
    // parent and its .keyword sub-field are byte-safe so an oversized value is skipped rather than
    // failing the whole document write. A naive swap to a stripped keyword helper would pass the
    // ignore_above checks but silently drop the normalizer — hence the normalizer assertion.
    assertEquals(mapping.get("type"), "keyword");
    assertEquals(mapping.get("normalizer"), "keyword_normalizer", "normalizer must be preserved");
    assertEquals(mapping.get("ignore_above"), keywordIgnoreAboveForMaxBytes(KEYWORD_MAXLENGTH));
    Map<String, Object> fields = getFields(mapping);
    assertEquals(
        ((Map<String, Object>) fields.get("keyword")).get("ignore_above"),
        keywordIgnoreAboveForMaxBytes(KEYWORD_MAXLENGTH),
        "the .keyword sub-field must also be byte-safe");
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
    assertFalse(
        mapping.containsKey("fields"),
        "URN mapping is parent keyword only; query time skips .keyword for URN SPs");
  }

  @Test
  public void testUrnLogicalValueTypeUsesUrnMapping() {
    Map<String, Object> mapping =
        FieldTypeMapper.getMappingsForLogicalValueType(
            com.linkedin.metadata.models.LogicalValueType.URN);
    assertEquals(mapping.get("type"), "keyword");
    assertEquals(mapping.get("ignore_above"), 255);
    assertFalse(
        mapping.containsKey("fields"),
        "URN logical value type must use parent keyword mapping without .keyword subfield");
  }

  @Test
  public void testSearchableUrnFieldUsesV2UrnMapping() {
    Map<String, Object> mapping = FieldTypeMapper.getMappingsForFieldType(FieldType.URN);
    // As on V2: the shared query builder runs analyzed URN search on the field itself
    assertEquals(mapping.get("type"), "text");
    assertEquals(mapping.get("analyzer"), "urn_component");
    assertEquals(mapping.get("search_analyzer"), "query_urn_component");
    assertEquals(getFields(mapping).keySet(), Set.of("keyword"));
  }

  @Test
  public void testSearchableUrnPartialFieldUsesNgramSubfield() {
    Map<String, Object> mapping = FieldTypeMapper.getMappingsForFieldType(FieldType.URN_PARTIAL);
    Map<String, Object> fields = getFields(mapping);

    assertTrue(fields.containsKey("keyword"));
    assertEquals(
        ((Map<String, Object>) fields.get("ngram")).get("analyzer"), "partial_urn_component");
  }

  @Test
  public void testStringLogicalValueTypeUsesIgnoreAbove() {
    Map<String, Object> mapping =
        FieldTypeMapper.getMappingsForLogicalValueType(
            com.linkedin.metadata.models.LogicalValueType.STRING);
    assertEquals(mapping.get("type"), "keyword");
    assertEquals(mapping.get("ignore_above"), keywordIgnoreAboveForMaxBytes(KEYWORD_MAXLENGTH));
  }

  @Test
  public void testRichTextLogicalValueTypeUsesIgnoreAbove() {
    Map<String, Object> mapping =
        FieldTypeMapper.getMappingsForLogicalValueType(
            com.linkedin.metadata.models.LogicalValueType.RICH_TEXT);
    assertEquals(mapping.get("type"), "keyword");
    assertEquals(mapping.get("ignore_above"), keywordIgnoreAboveForMaxBytes(KEYWORD_MAXLENGTH));
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

  @Test
  public void testRichnessTieBreaksDeterministicallyAcrossInputOrder() {
    // TEXT and URN share richness 40 - the representative must not depend on list order.
    SearchableFieldSpec textSpec = fieldSpec(FieldType.TEXT, "aspectA/field");
    SearchableFieldSpec urnSpec = fieldSpec(FieldType.URN, "aspectB/field");

    Map<String, Object> forward =
        FieldTypeMapper.getRichestCompatibleMapping(List.of(textSpec, urnSpec), Map.of());
    Map<String, Object> reversed =
        FieldTypeMapper.getRichestCompatibleMapping(List.of(urnSpec, textSpec), Map.of());
    assertEquals(forward, reversed);
  }

  private static SearchableFieldSpec fieldSpec(FieldType fieldType, String path) {
    SearchableFieldSpec spec = mock(SearchableFieldSpec.class);
    SearchableAnnotation annotation = mock(SearchableAnnotation.class);
    when(annotation.getFieldType()).thenReturn(fieldType);
    when(spec.getSearchableAnnotation()).thenReturn(annotation);
    when(spec.getPath()).thenReturn(new PathSpec(path.split("/")));
    return spec;
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> getFields(Map<String, Object> mapping) {
    return (Map<String, Object>) mapping.get("fields");
  }
}
