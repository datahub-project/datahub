package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.search.utils.ESUtils.KEYWORD_MAXLENGTH;
import static com.linkedin.metadata.search.utils.ESUtils.keywordIgnoreAboveForMaxBytes;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.linkedin.data.DataList;
import com.linkedin.data.DataMap;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.IntegerDataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.models.LogicalValueType;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.models.annotation.SearchableAnnotation.FieldType;
import com.linkedin.metadata.models.annotation.v3.FieldMapping;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2MappingsBuilder;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

public class FieldTypeMapperTest {

  @Test
  public void testStringFieldsMapToRawKeywords() {
    // The root is the exact field. Full text lives on the shared labels.
    for (FieldType fieldType :
        List.of(
            FieldType.TEXT,
            FieldType.TEXT_PARTIAL,
            FieldType.WORD_GRAM,
            FieldType.URN,
            FieldType.URN_PARTIAL)) {
      Map<String, Object> mapping = FieldTypeMapper.getMappingsForFieldType(fieldType);
      assertEquals(mapping.get("type"), "keyword", fieldType.name());
      assertFalse(mapping.containsKey("normalizer"), fieldType.name());
      assertEquals(mapping.get("ignore_above"), KEYWORD_MAXLENGTH, fieldType.name());
      assertFalse(mapping.containsKey("fields"), fieldType.name());
    }
  }

  @Test
  public void testPlainKeywordFieldIsARawKeyword() {
    Map<String, Object> mapping = FieldTypeMapper.getMappingsForFieldType(FieldType.KEYWORD);
    assertEquals(mapping.get("type"), "keyword");
    assertEquals(mapping.get("ignore_above"), keywordIgnoreAboveForMaxBytes(KEYWORD_MAXLENGTH));
    assertFalse(mapping.containsKey("normalizer"));
    assertFalse(mapping.containsKey("fields"));
  }

  @Test
  public void testGetMappingsForKeywordWithIgnoreAbove() {
    Map<String, Object> mapping = FieldTypeMapper.getMappingsForKeywordWithIgnoreAbove();
    assertEquals(mapping.get("type"), "keyword");
    assertEquals(mapping.get("ignore_above"), keywordIgnoreAboveForMaxBytes(KEYWORD_MAXLENGTH));
    assertFalse(mapping.containsKey("normalizer"), mapping.toString());
    assertFalse(mapping.containsKey("fields"), mapping.toString());
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
    // The root is the exact keyword. ignore_above skips an oversized value instead of failing the
    // document write on Lucene's term limit.
    assertEquals(mapping.get("type"), "keyword");
    assertFalse(mapping.containsKey("normalizer"), mapping.toString());
    assertFalse(mapping.containsKey("fields"), mapping.toString());
    assertEquals(mapping.get("ignore_above"), keywordIgnoreAboveForMaxBytes(KEYWORD_MAXLENGTH));
  }

  @Test
  public void testGetMappingsForKeywordWithConfiguredIgnoreAbove() {
    Map<String, Object> mapping = FieldTypeMapper.getMappingsForKeywordWithIgnoreAbove(1024);
    assertEquals(mapping.get("type"), "keyword");
    // Configured value is UTF-8 bytes; ignore_above is character-based and byte-safe (/ 4).
    assertEquals(mapping.get("ignore_above"), 256);
    assertRawKeyword(mapping);
  }

  @Test
  public void testStringLogicalValueTypeHonorsConfiguredKeywordMaxLength() {
    Map<String, Object> mapping =
        FieldTypeMapper.getMappingsForLogicalValueType(
            com.linkedin.metadata.models.LogicalValueType.STRING, 2048);
    assertEquals(mapping.get("type"), "keyword");
    assertEquals(mapping.get("ignore_above"), 512);
    assertRawKeyword(mapping);
  }

  @Test
  public void testGetMappingsForUrn() {
    Map<String, Object> mapping = FieldTypeMapper.getMappingsForUrn();
    assertEquals(mapping.get("type"), "keyword");
    assertFalse(mapping.containsKey("ignore_above"));
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
    assertFalse(mapping.containsKey("ignore_above"));
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
    assertRawKeyword(mapping);
  }

  @Test
  public void testRichTextLogicalValueTypeUsesIgnoreAbove() {
    Map<String, Object> mapping =
        FieldTypeMapper.getMappingsForLogicalValueType(
            com.linkedin.metadata.models.LogicalValueType.RICH_TEXT);
    assertEquals(mapping.get("type"), "keyword");
    assertEquals(mapping.get("ignore_above"), keywordIgnoreAboveForMaxBytes(KEYWORD_MAXLENGTH));
    assertRawKeyword(mapping);
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
      assertFalse(mapping.containsKey("fields"), mapping.toString());
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

  @Test
  @SuppressWarnings("unchecked")
  public void testAspectStringsFollowExactRootWithoutAnalyzers() {
    Map<String, Object> aspect =
        FieldTypeMapper.getAspectMappingsForFieldType(
            FieldType.TEXT, mock(SearchableFieldSpec.class));
    assertEquals(aspect.get("type"), "keyword");
    assertEquals(aspect.get("ignore_above"), keywordIgnoreAboveForMaxBytes(KEYWORD_MAXLENGTH));
    assertFalse(aspect.containsKey("fields"), aspect.toString());
    assertFalse(aspect.containsKey("normalizer"), aspect.toString());

    Map<String, Object> unindexed =
        FieldTypeMapper.mappingForRoot(
            FieldMapping.root(FieldMapping.Type.KEYWORD, false, false), null);
    assertEquals(unindexed.get("index"), false);
    assertFalse(unindexed.containsKey("fields"));
    assertFalse(String.valueOf(unindexed).contains("text"));

    Map<String, Object> unindexedAspect =
        FieldTypeMapper.mappingForAspectRoot(
            FieldMapping.root(FieldMapping.Type.KEYWORD, false, false),
            annotationSpec(unindexedKeywordAnnotation(), mock(DataSchema.class)));
    assertEquals(unindexedAspect, unindexed);
  }

  @Test
  public void testLabeledAspectSourceIsUnindexedUntilReenabled() {
    Map<String, Object> labeled =
        FieldTypeMapper.mappingForAspectRoot(
            FieldMapping.root(FieldMapping.Type.KEYWORD),
            annotationSpec(labeledAnnotation("KEYWORD", "name"), mock(DataSchema.class)));
    assertEquals(labeled.get("type"), "keyword");
    assertEquals(labeled.get("index"), false);
    assertEquals(labeled.get("doc_values"), false);
    assertFalse(labeled.containsKey("fields"));

    DataMap indexOnly = labeledAnnotation("KEYWORD", "name");
    DataMap indexOnlySource = new DataMap();
    indexOnlySource.put("index", true);
    ((DataMap) indexOnly.get("v3")).put("source", indexOnlySource);
    Map<String, Object> indexed =
        FieldTypeMapper.mappingForAspectRoot(
            FieldMapping.root(FieldMapping.Type.KEYWORD),
            annotationSpec(indexOnly, mock(DataSchema.class)));
    assertFalse(indexed.containsKey("normalizer"), indexed.toString());
    assertEquals(indexed.get("ignore_above"), keywordIgnoreAboveForMaxBytes(KEYWORD_MAXLENGTH));
    assertFalse(indexed.containsKey("index"));
    assertEquals(indexed.get("doc_values"), false);

    DataMap both = labeledAnnotation("KEYWORD", "name");
    DataMap bothSource = new DataMap();
    bothSource.put("index", true);
    bothSource.put("docValues", true);
    ((DataMap) both.get("v3")).put("source", bothSource);
    Map<String, Object> fullyIndexed =
        FieldTypeMapper.mappingForAspectRoot(
            FieldMapping.root(FieldMapping.Type.KEYWORD),
            annotationSpec(both, mock(DataSchema.class)));
    assertFalse(fullyIndexed.containsKey("normalizer"), fullyIndexed.toString());
    assertEquals(
        fullyIndexed.get("ignore_above"), keywordIgnoreAboveForMaxBytes(KEYWORD_MAXLENGTH));
    assertFalse(fullyIndexed.containsKey("index"));
    assertFalse(fullyIndexed.containsKey("doc_values"));

    Map<String, Object> count =
        FieldTypeMapper.mappingForAspectRoot(
            FieldMapping.root(FieldMapping.Type.COUNT),
            annotationSpec(
                labeledAnnotation("COUNT", "rowCount"),
                new IntegerDataSchema(),
                DataSchema.Type.INT));
    assertEquals(count.get("type"), "integer");
    assertEquals(count.get("index"), false);
    assertEquals(count.get("doc_values"), false);

    DataMap plain = new DataMap();
    plain.put("fieldType", "BOOLEAN");
    Map<String, Object> unlabeled =
        FieldTypeMapper.mappingForAspectRoot(
            FieldMapping.root(FieldMapping.Type.BOOLEAN),
            annotationSpec(plain, mock(DataSchema.class), DataSchema.Type.BOOLEAN));
    assertEquals(unlabeled.get("type"), "boolean");
    assertEquals(unlabeled.get("index"), false);
    assertEquals(unlabeled.get("doc_values"), false);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testLabelSubfieldsAreAlternatesOfTheKeywordRoot() {
    FieldMapping.Resolved resolved =
        FieldMapping.union(
            "name",
            List.of(
                FieldMapping.fallback(FieldType.WORD_GRAM, true),
                List.of(
                    FieldMapping.root(FieldMapping.Type.KEYWORD),
                    FieldMapping.subfield("text", FieldMapping.Type.TEXT),
                    FieldMapping.subfield("stemmed", FieldMapping.Type.STEMMED),
                    FieldMapping.subfield("ngram", FieldMapping.Type.TEXT_PARTIAL))));
    Map<String, Object> label =
        FieldTypeMapper.composeLabelMapping(resolved, null, Map.of("type", "search_as_you_type"));
    assertEquals(label.get("type"), "keyword");
    assertEquals(label.get("ignore_above"), keywordIgnoreAboveForMaxBytes(KEYWORD_MAXLENGTH));
    Map<String, Object> fields = (Map<String, Object>) label.get("fields");
    assertEquals(fields.keySet(), Set.of("text", "stemmed", "ngram"));
    assertEquals(((Map<String, Object>) fields.get("text")).get("type"), "text");
    assertFalse(
        fields.get("text") instanceof Map
            && ((Map<?, ?>) fields.get("text")).containsKey("doc_values"));
  }

  @Test
  public void testUnlabeledTextFallbackHasNoAnalyzerSubfields() {
    List<FieldMapping> mappings = FieldMapping.fallback(FieldType.TEXT, false);
    assertEquals(mappings.size(), 1);
    assertTrue(mappings.get(0).isRoot());
    assertEquals(mappings.get(0).getType(), FieldMapping.Type.KEYWORD);
    assertTrue(mappings.get(0).isIndex());
  }

  private static DataMap labeledAnnotation(String fieldType, String searchLabel) {
    DataMap v3 = new DataMap();
    v3.put("searchLabel", searchLabel);
    DataMap annotation = new DataMap();
    annotation.put("fieldType", fieldType);
    annotation.put("v3", v3);
    return annotation;
  }

  private static DataMap unindexedKeywordAnnotation() {
    DataMap root = new DataMap();
    root.put("type", "KEYWORD");
    root.put("index", false);
    root.put("docValues", false);
    DataList mappings = new DataList();
    mappings.add(root);
    DataMap v3 = new DataMap();
    v3.put("mappings", mappings);
    DataMap annotation = new DataMap();
    annotation.put("fieldType", "KEYWORD");
    annotation.put("v3", v3);
    return annotation;
  }

  private static SearchableFieldSpec annotationSpec(DataMap annotation, DataSchema schema) {
    return annotationSpec(annotation, schema, DataSchema.Type.STRING);
  }

  private static SearchableFieldSpec annotationSpec(
      DataMap annotation, DataSchema schema, DataSchema.Type schemaType) {
    SearchableAnnotation parsed =
        SearchableAnnotation.fromPegasusAnnotationObject(annotation, "field", schemaType, "test");
    return new SearchableFieldSpec(new PathSpec("field"), parsed, schema);
  }

  private static SearchableFieldSpec fieldSpec(FieldType fieldType, String path) {
    SearchableFieldSpec spec = mock(SearchableFieldSpec.class);
    SearchableAnnotation annotation = mock(SearchableAnnotation.class);
    when(annotation.getFieldType()).thenReturn(fieldType);
    when(spec.getSearchableAnnotation()).thenReturn(annotation);
    when(spec.getPath()).thenReturn(new PathSpec(path.split("/")));
    return spec;
  }

  private static void assertRawKeyword(Map<String, Object> mapping) {
    assertFalse(mapping.containsKey("normalizer"), mapping.toString());
    assertFalse(mapping.containsKey("fields"), mapping.toString());
  }
}
