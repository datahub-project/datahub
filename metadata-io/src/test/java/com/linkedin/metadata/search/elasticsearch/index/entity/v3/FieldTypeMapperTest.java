package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.search.utils.ESUtils.KEYWORD_MAXLENGTH;
import static com.linkedin.metadata.search.utils.ESUtils.keywordIgnoreAboveForMaxBytes;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.linkedin.data.schema.PathSpec;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.models.LogicalValueType;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.models.annotation.SearchableAnnotation.FieldType;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2MappingsBuilder;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

public class FieldTypeMapperTest {

  @Test
  public void testStringFieldsMapToNormalizedKeywords() {
    // Root fields are not analyzed: full text lives in the shared _search fields
    for (FieldType fieldType :
        List.of(
            FieldType.TEXT,
            FieldType.TEXT_PARTIAL,
            FieldType.WORD_GRAM,
            FieldType.URN,
            FieldType.URN_PARTIAL)) {
      Map<String, Object> mapping = FieldTypeMapper.getMappingsForFieldType(fieldType);
      assertEquals(mapping.get("type"), "keyword", fieldType.name());
      assertEquals(mapping.get("normalizer"), "keyword_normalizer", fieldType.name());
      assertEquals(mapping.get("ignore_above"), KEYWORD_MAXLENGTH, fieldType.name());
      assertEquals(getFields(mapping).keySet(), Set.of("keyword"), fieldType.name());
      assertKeywordSubfieldPresent(mapping);
      // .keyword keeps the stored casing for case-sensitive exact match and facets
      assertFalse(
          ((Map<String, Object>) getFields(mapping).get("keyword")).containsKey("normalizer"),
          fieldType.name());
    }
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
    assertKeywordSubfieldPresent(mapping);
  }

  @Test
  public void testStringLogicalValueTypeHonorsConfiguredKeywordMaxLength() {
    Map<String, Object> mapping =
        FieldTypeMapper.getMappingsForLogicalValueType(
            com.linkedin.metadata.models.LogicalValueType.STRING, 2048);
    assertEquals(mapping.get("type"), "keyword");
    assertEquals(mapping.get("ignore_above"), 512);
    assertKeywordSubfieldPresent(mapping);
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
  public void testStringLogicalValueTypeUsesIgnoreAbove() {
    Map<String, Object> mapping =
        FieldTypeMapper.getMappingsForLogicalValueType(
            com.linkedin.metadata.models.LogicalValueType.STRING);
    assertEquals(mapping.get("type"), "keyword");
    assertEquals(mapping.get("ignore_above"), keywordIgnoreAboveForMaxBytes(KEYWORD_MAXLENGTH));
    assertKeywordSubfieldPresent(mapping);
  }

  @Test
  public void testRichTextLogicalValueTypeUsesIgnoreAbove() {
    Map<String, Object> mapping =
        FieldTypeMapper.getMappingsForLogicalValueType(
            com.linkedin.metadata.models.LogicalValueType.RICH_TEXT);
    assertEquals(mapping.get("type"), "keyword");
    assertEquals(mapping.get("ignore_above"), keywordIgnoreAboveForMaxBytes(KEYWORD_MAXLENGTH));
    assertKeywordSubfieldPresent(mapping);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBrowsePathFieldUsesV2Mapping() {
    // Legacy browse aggregates on the path prefixes and filters on the path depth
    EntityRegistry registry = TestOperationContexts.defaultEntityRegistry();
    Map<String, Object> v2Properties =
        (Map<String, Object>)
            new V2MappingsBuilder(
                    EntityIndexConfiguration.builder().build(),
                    FieldTypeMapper.DEFAULT_PARTIAL_NGRAM_CONFIG)
                .getIndexMappings(registry, registry.getEntitySpec("dataset"))
                .get("properties");

    assertEquals(
        FieldTypeMapper.getMappingsForFieldType(FieldType.BROWSE_PATH),
        v2Properties.get("browsePaths"));
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
  public void testRichestMappingForUrnKeywordCollisionKeepsSortableBaseEitherOrder() {
    SearchableFieldSpec urnSpec = fieldSpec(FieldType.URN, "aspectA/field");
    SearchableFieldSpec keywordSpec = fieldSpec(FieldType.KEYWORD, "aspectB/field");

    for (List<SearchableFieldSpec> order :
        List.of(List.of(urnSpec, keywordSpec), List.of(keywordSpec, urnSpec))) {
      Map<String, Object> mapping = FieldTypeMapper.getRichestCompatibleMapping(order);
      assertEquals(
          mapping.get("type"),
          "keyword",
          "URN+KEYWORD collision must emit an exact-match-safe base regardless of spec order");
      // Filters, facets and sorts append .keyword to root fields
      assertEquals(
          ((Map<String, Object>) getFields(mapping).get("keyword")).get("type"), "keyword");
    }
  }

  @Test
  public void testRichnessTieBreaksDeterministicallyAcrossInputOrder() {
    // TEXT and URN share richness 40 - the representative must not depend on list order.
    SearchableFieldSpec textSpec = fieldSpec(FieldType.TEXT, "aspectA/field");
    SearchableFieldSpec urnSpec = fieldSpec(FieldType.URN, "aspectB/field");

    Map<String, Object> forward =
        FieldTypeMapper.getRichestCompatibleMapping(List.of(textSpec, urnSpec));
    Map<String, Object> reversed =
        FieldTypeMapper.getRichestCompatibleMapping(List.of(urnSpec, textSpec));
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
  private static void assertKeywordSubfieldPresent(Map<String, Object> mapping) {
    assertTrue(mapping.containsKey("fields"), "STRING/RICH_TEXT/TEXT mappings need .keyword");
    Map<String, Object> fields = (Map<String, Object>) mapping.get("fields");
    assertTrue(fields.containsKey("keyword"), "Must expose .keyword multi-field for exact match");
    Map<String, Object> keyword = (Map<String, Object>) fields.get("keyword");
    assertEquals(keyword.get("type"), "keyword");
    assertEquals(
        keyword.get("ignore_above"),
        mapping.get("ignore_above"),
        ".keyword subfield must mirror parent ignore_above");
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> getFields(Map<String, Object> mapping) {
    return (Map<String, Object>) mapping.get("fields");
  }
}
