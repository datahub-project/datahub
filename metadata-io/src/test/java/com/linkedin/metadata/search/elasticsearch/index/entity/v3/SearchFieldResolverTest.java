package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.models.annotation.v3.FieldMapping;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.SearchFieldResolver.Intent;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchFieldConfig;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.utils.CriterionUtils;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.testng.annotations.Test;

public class SearchFieldResolverTest {

  @Test
  public void testSearchOnlyLabelRejectsExactIntents() {
    SearchFieldResolver resolver =
        resolver(
            field(
                "description",
                "description",
                List.of(
                    FieldMapping.root(FieldMapping.Type.KEYWORD, false, false),
                    FieldMapping.subfield("text", FieldMapping.Type.TEXT),
                    FieldMapping.subfield("stemmed", FieldMapping.Type.STEMMED))),
            field(
                "editedDescription",
                "description",
                List.of(
                    FieldMapping.root(FieldMapping.Type.KEYWORD, false, false),
                    FieldMapping.subfield("text", FieldMapping.Type.TEXT),
                    FieldMapping.subfield("stemmed", FieldMapping.Type.STEMMED))));

    assertTrue(resolver.resolve("description", Intent.FULL_TEXT).supports(Intent.FULL_TEXT));
    assertEquals(
        resolver.resolve("description", Intent.FULL_TEXT).getSourceFields(),
        java.util.Set.of("description", "editedDescription"));
    assertThrows(
        IllegalArgumentException.class, () -> resolver.exactPath("description", Intent.FILTER));
    assertThrows(
        IllegalArgumentException.class, () -> resolver.exactPath("editedDescription", Intent.SORT));
    assertEquals(resolver.hasValuesFieldName("description"), Optional.empty());
  }

  @Test
  public void testUnindexedLabelExposesSharedHasValuesField() {
    List<FieldMapping> description =
        List.of(
            FieldMapping.root(FieldMapping.Type.KEYWORD, false, false),
            FieldMapping.subfield("text", FieldMapping.Type.TEXT),
            FieldMapping.subfield("stemmed", FieldMapping.Type.STEMMED));
    SearchFieldResolver resolver =
        resolver(
            field("description", "description", description, "hasDescription"),
            field("editedDescription", "description", description, "hasDescription"),
            field("fieldDescriptions", "description", description, "hasDescription"));

    assertEquals(resolver.hasValuesFieldName("description"), Optional.of("hasDescription"));
    assertEquals(resolver.hasValuesFieldName("editedDescription"), Optional.of("hasDescription"));
    assertEquals(resolver.hasValuesFieldName("fieldDescriptions"), Optional.of("hasDescription"));
    assertEquals(resolver.hasValuesFieldName("hasDescription"), Optional.empty());
    assertTrue(resolver.supports("hasDescription", Intent.FILTER));
    assertEquals(resolver.exactPath("hasDescription", Intent.FILTER), "hasDescription");
  }

  @Test
  public void testFieldWithoutSearchLabelDropsHasValues() {
    SearchFieldResolver resolver =
        resolver(
            field(
                "rowCount",
                null,
                List.of(FieldMapping.root(FieldMapping.Type.COUNT)),
                "hasRowCount"),
            field(
                "description",
                "description",
                List.of(FieldMapping.root(FieldMapping.Type.KEYWORD, false, false)),
                "hasDescription"));

    assertFalse(resolver.contains("hasRowCount"));
    assertEquals(resolver.unlabeledHasValuesSource("hasRowCount"), Optional.of("rowCount"));
    assertEquals(resolver.hasValuesFieldName("rowCount"), Optional.empty());
    assertTrue(resolver.supports("rowCount", Intent.FILTER));
    assertTrue(resolver.contains("hasDescription"));
    assertEquals(resolver.unlabeledHasValuesSource("hasDescription"), Optional.empty());
    assertTrue(resolver.falseIsMissing("hasDescription"));
  }

  @Test
  public void testExactPathsFollowResolvedRootType() {
    SearchFieldResolver resolver =
        resolver(
            field(
                "name",
                "name",
                List.of(
                    FieldMapping.root(FieldMapping.Type.KEYWORD),
                    FieldMapping.subfield("text", FieldMapping.Type.TEXT))),
            field("owners", "owners", List.of(FieldMapping.root(FieldMapping.Type.URN))),
            field("createdAt", null, List.of(FieldMapping.root(FieldMapping.Type.DATETIME))));

    assertEquals(resolver.exactPath("name", Intent.FILTER), "name");
    assertEquals(resolver.exactPath("name.keyword", Intent.FILTER), "name");
    assertEquals(resolver.exactPath("name", Intent.SORT), "name");
    assertEquals(resolver.exactPath("owners", Intent.FACET), "owners");
    assertFalse(resolver.supports("owners", Intent.FULL_TEXT));
    assertEquals(resolver.exactPath("createdAt", Intent.RANGE), "createdAt");
  }

  @Test
  public void testSourceRootSelectsOneSharedLabelAcrossEntities() {
    SearchFieldResolver resolver =
        resolver(
            List.of(
                List.of(
                    field(
                        "createdAt", null, List.of(FieldMapping.root(FieldMapping.Type.DATETIME)))),
                List.of(
                    field(
                        "createdAt",
                        "sourceCreatedAt",
                        List.of(FieldMapping.root(FieldMapping.Type.DATETIME))))));

    assertEquals(resolver.exactPath("createdAt", Intent.FILTER), "sourceCreatedAt");
    assertEquals(resolver.exactPath("createdAt", Intent.RANGE), "sourceCreatedAt");
    assertEquals(resolver.exactPath("createdAt", Intent.SORT), "sourceCreatedAt");
    assertEquals(resolver.exactPath("createdAt", Intent.EXACT), "createdAt");
  }

  @Test
  public void testAliasesSharedLabelsNativeRootsAndAutocompleteAreDeterministic() {
    SearchableFieldSpec primaryName =
        field(
            "displayName",
            "name",
            List.of(
                FieldMapping.root(FieldMapping.Type.KEYWORD),
                FieldMapping.subfield("text", FieldMapping.Type.TEXT)),
            List.of("_entityName"),
            true);
    SearchableFieldSpec editedName =
        field(
            "editedName",
            "name",
            List.of(
                FieldMapping.root(FieldMapping.Type.KEYWORD),
                FieldMapping.subfield("stemmed", FieldMapping.Type.STEMMED)));
    SearchFieldResolver resolver =
        resolver(
            List.of(
                List.of(
                    primaryName,
                    field("owners", "owners", List.of(FieldMapping.root(FieldMapping.Type.URN))),
                    field(
                        "createdAt", null, List.of(FieldMapping.root(FieldMapping.Type.DATETIME)))),
                List.of(
                    editedName,
                    field("score", null, List.of(FieldMapping.root(FieldMapping.Type.DOUBLE))),
                    field("count", null, List.of(FieldMapping.root(FieldMapping.Type.COUNT))))));

    assertEquals(resolver.exactPath("_entityName", Intent.EXACT), "name");
    assertEquals(resolver.exactPath("editedName", Intent.FILTER), "name");
    assertEquals(
        resolver.resolve("name", Intent.FULL_TEXT).getSourceFields(),
        java.util.Set.of("displayName", "editedName"));
    assertEquals(resolver.exactPath("owners", Intent.FACET), "owners");
    assertEquals(resolver.exactPath("createdAt", Intent.RANGE), "createdAt");
    assertEquals(resolver.exactPath("score", Intent.SORT), "score");
    assertEquals(resolver.exactPath("count", Intent.FILTER), "count");
    assertThrows(IllegalArgumentException.class, () -> resolver.resolve("unknown", Intent.FILTER));
  }

  @Test
  public void testFullUrnAndParsedNameUseDifferentPaths() {
    SearchableFieldSpec platform = urnField("platform", "platform");
    SearchFieldResolver resolver = new SearchFieldResolver(List.of(entity("datasetKey", platform)));

    assertEquals(resolver.exactPath("platform", Intent.FACET), "platform_urn");
    assertEquals(resolver.exactPath("platform", Intent.FILTER), "platform_urn");
    assertEquals(resolver.exactPath("platform", Intent.SORT), "platform_urn");
    assertEquals(resolver.exactPath("platform", Intent.EXACT), "platform");
    assertEquals(resolver.parsedKeywordPaths("platform"), List.of("platform"));

    SearchableFieldSpec modelPlatform = urnField("platform", "platform");
    SearchFieldResolver shared =
        new SearchFieldResolver(
            List.of(entity("datasetKey", platform), entity("mlModelKey", modelPlatform)));
    assertEquals(shared.exactPath("platform", Intent.FACET), "platform_urn");
    assertEquals(shared.exactPath("platform", Intent.EXACT), "platform");
    assertEquals(shared.parsedKeywordPaths("platform"), List.of("platform"));

    SearchableFieldSpec parent = urnField("parent", null);
    SearchFieldResolver parentResolver =
        new SearchFieldResolver(List.of(entity("schemaFieldKey", parent)));
    assertEquals(parentResolver.exactPath("parent", Intent.FILTER), "parent_urn");
    assertEquals(parentResolver.exactPath("parent", Intent.EXACT), "parent_urn");
    assertEquals(parentResolver.parsedKeywordPaths("parent"), List.of("parent"));
    assertFalse(parentResolver.supports("parent", Intent.FULL_TEXT));

    SearchableFieldSpec owners =
        field("owners", "owners", List.of(FieldMapping.root(FieldMapping.Type.URN)));
    when(owners.getSearchableAnnotation().getFieldType())
        .thenReturn(SearchableAnnotation.FieldType.URN);
    when(owners.getSearchableAnnotation().isQueryByDefault()).thenReturn(false);
    SearchFieldResolver ownerResolver =
        new SearchFieldResolver(List.of(entity("ownership", owners)));
    assertEquals(ownerResolver.exactPath("owners", Intent.FACET), "owners_urn");
    assertEquals(ownerResolver.exactPath("owners", Intent.FILTER), "owners_urn");
    assertEquals(ownerResolver.exactPath("owners", Intent.EXACT), "owners");
    assertEquals(ownerResolver.parsedKeywordPaths("owners"), List.of("owners"));
    assertEquals(ownerResolver.lowercaseFullUrnPath("owners"), "owners_urn.lowercase");
    assertEquals(ownerResolver.exactPath("urn", Intent.FILTER), "urn");
    assertEquals(ownerResolver.lowercaseFullUrnPath("urn"), "urn.lowercase");
    assertEquals(ownerResolver.exactPath("urn_name", Intent.FILTER), "urn_name");
    assertEquals(ownerResolver.lowercaseFullUrnPath("urn_name"), "urn_name.lowercase");
    assertFalse(ownerResolver.supports("owners", SearchFieldResolver.Intent.FULL_TEXT));
  }

  @Test
  public void testChildValueMatchesItsDegreeAndNotTheEntityLabel() {
    EntityRegistry registry = TestOperationContexts.defaultEntityRegistry();
    EntitySpec dataset = registry.getEntitySpec("dataset");
    SearchFieldResolver resolver = new SearchFieldResolver(List.of(dataset));

    assertEquals(resolver.resolve("description", Intent.FULL_TEXT).getPath(), "description");
    assertEquals(
        resolver.resolve("fieldDescriptions", Intent.FULL_TEXT).getPath(), "description_1");
    assertEquals(
        resolver.resolve("editedFieldDescriptions", Intent.FULL_TEXT).getPath(), "description_1");
    assertFalse(
        resolver
            .resolve("description", Intent.FULL_TEXT)
            .getSourceFields()
            .contains("fieldDescriptions"));
    assertEquals(resolver.resolve("hasDescription", Intent.FILTER).getPath(), "hasDescription");
    assertEquals(resolver.resolve("hasDescription_1", Intent.FILTER).getPath(), "hasDescription_1");
    assertEquals(resolver.hasValuesFieldName("description"), Optional.of("hasDescription"));
    assertEquals(resolver.hasValuesFieldName("description_1"), Optional.of("hasDescription_1"));
    assertEquals(resolver.hasValuesFieldName("fieldDescriptions"), Optional.of("hasDescription_1"));
    assertTrue(resolver.falseIsMissing("hasDescription"));
    assertTrue(resolver.falseIsMissing("hasDescription_1"));
    assertEquals(resolver.resolve("hasTags", Intent.FILTER).getPath(), "hasTags");
    assertEquals(resolver.resolve("hasTags_1", Intent.FILTER).getPath(), "hasTags_1");
    assertEquals(resolver.hasValuesFieldName("tags"), Optional.of("hasTags"));
    assertEquals(resolver.hasValuesFieldName("tags_1"), Optional.of("hasTags_1"));
    assertEquals(resolver.hasValuesFieldName("fieldTags"), Optional.of("hasTags_1"));

    assertEquals(resolver.resolve("tags", Intent.FILTER).getPath(), "tags");
    assertEquals(resolver.resolve("fieldTags", Intent.FILTER).getPath(), "tags_1");
    assertEquals(resolver.resolve("editedFieldTags", Intent.FILTER).getPath(), "tags_1");
    assertEquals(resolver.resolve("tags_1", Intent.FILTER).getPath(), "tags_1");
    assertFalse(resolver.resolve("tags", Intent.FILTER).getSourceFields().contains("fieldTags"));
    assertEquals(
        resolver.resolve("tags_1", Intent.FILTER).getSourceFields(),
        Set.of("fieldTags", "editedFieldTags"));

    assertEquals(resolver.resolve("glossaryTerms", Intent.FILTER).getPath(), "glossaryTerms");
    assertEquals(
        resolver.resolve("fieldGlossaryTerms", Intent.FILTER).getPath(), "glossaryTerms_1");
    assertEquals(
        resolver.resolve("editedFieldGlossaryTerms", Intent.FILTER).getPath(), "glossaryTerms_1");
    assertFalse(
        resolver
            .resolve("glossaryTerms", Intent.FILTER)
            .getSourceFields()
            .contains("fieldGlossaryTerms"));

    SearchableFieldSpec fieldTags =
        dataset.getAspectSpec("schemaMetadata").getSearchableFieldSpecs().stream()
            .filter(spec -> "fieldTags".equals(spec.getSearchableAnnotation().getFieldName()))
            .findFirst()
            .orElseThrow();
    assertEquals(SearchFieldConfig.detectSubFieldType(fieldTags).boost(), 0.5f);

    EntitySpec businessAttribute = registry.getEntitySpec("businessAttribute");
    SearchFieldResolver attributeResolver = new SearchFieldResolver(List.of(businessAttribute));
    assertEquals(
        attributeResolver.resolve("editedFieldDescriptions", Intent.FULL_TEXT).getPath(),
        "description");
    assertFalse(attributeResolver.contains("description_1"));
    assertEquals(
        attributeResolver.resolve("hasDescription", Intent.FILTER).getPath(), "hasDescription");
    assertEquals(
        attributeResolver.hasValuesFieldName("editedFieldDescriptions"),
        Optional.of("hasDescription"));
    assertFalse(attributeResolver.contains("hasDescription_1"));
    assertEquals(attributeResolver.resolve("editedFieldTags", Intent.FILTER).getPath(), "tags");
    assertFalse(attributeResolver.contains("tags_1"));

    SearchFieldResolver sharedRecord = new SearchFieldResolver(List.of(dataset, businessAttribute));
    assertEquals(
        sharedRecord.resolve("editedFieldTags", Intent.FILTER).getPath(), "editedFieldTags");
    assertTrue(sharedRecord.supports("editedFieldDescriptions", Intent.CONTAIN));
    assertEquals(
        Set.copyOf(sharedRecord.analyzedPaths("editedFieldDescriptions")),
        Set.of(
            "description.text",
            "description.stemmed",
            "description_1.text",
            "description_1.stemmed"));
    assertEquals(
        Set.copyOf(sharedRecord.hasValuesFieldNames("editedFieldDescriptions")),
        Set.of("hasDescription", "hasDescription_1"));
    assertEquals(sharedRecord.hasValuesFieldName("editedFieldDescriptions"), Optional.empty());
    assertEquals(sharedRecord.resolve("tags", Intent.FILTER).getPath(), "tags");
    assertEquals(sharedRecord.resolve("tags_1", Intent.FILTER).getPath(), "tags_1");
    assertEquals(sharedRecord.hasValuesFieldName("tags"), Optional.of("hasTags"));
    assertEquals(sharedRecord.hasValuesFieldName("tags_1"), Optional.of("hasTags_1"));
    assertTrue(
        sharedRecord.resolve("tags", Intent.FILTER).getSourceFields().contains("editedFieldTags"));
    assertTrue(
        sharedRecord
            .resolve("tags_1", Intent.FILTER)
            .getSourceFields()
            .contains("editedFieldTags"));
    assertEquals(resolver.exactPath("tags", Intent.FILTER), "tags_urn");
    assertEquals(resolver.exactPath("tags_1", Intent.FILTER), "tags_1_urn");
    assertEquals(resolver.exactPath("fieldTags", Intent.FILTER), "tags_1_urn");
    assertEquals(sharedRecord.exactPath("tags", Intent.FILTER), "tags_urn");
    assertEquals(sharedRecord.exactPath("tags_1", Intent.FILTER), "tags_1_urn");
    assertEquals(sharedRecord.exactPath("editedFieldTags", Intent.FILTER), "editedFieldTags");
  }

  @Test
  public void testLowercaseSubfieldIsOptIn() {
    SearchableFieldSpec plain =
        field("platform", null, List.of(FieldMapping.root(FieldMapping.Type.KEYWORD)));
    SearchFieldResolver stored = resolver(plain);
    assertFalse(stored.hasLowercaseSubfield("platform"));

    SearchableFieldSpec folded =
        field("name", null, List.of(FieldMapping.root(FieldMapping.Type.KEYWORD)));
    when(folded.getSearchableAnnotation().isV3Lowercase()).thenReturn(true);
    SearchFieldResolver resolver = resolver(folded);
    assertTrue(resolver.hasLowercaseSubfield("name"));

    String query =
        ESUtils.getQueryBuilderFromCriterion(
                CriterionUtils.buildCriterion("name", Condition.CONTAIN, "José"),
                false,
                Map.of(),
                TestOperationContexts.systemContextNoSearchAuthorization(),
                QueryFilterRewriteChain.EMPTY,
                resolver)
            .toString();
    assertTrue(query.contains("name.lowercase"), query);
    assertTrue(query.contains("*jose*"), query);
    assertFalse(query.contains("*José*"), query);
    assertFalse(query.contains("case_insensitive"), query);
  }

  private static SearchFieldResolver resolver(SearchableFieldSpec... fields) {
    return resolver(List.of(List.of(fields)));
  }

  private static SearchFieldResolver resolver(List<List<SearchableFieldSpec>> entityFields) {
    List<EntitySpec> entitySpecs =
        entityFields.stream()
            .map(
                fields -> {
                  EntitySpec entitySpec = mock(EntitySpec.class);
                  when(entitySpec.getSearchableFieldSpecs()).thenReturn(fields);
                  return entitySpec;
                })
            .toList();
    return new SearchFieldResolver(entitySpecs);
  }

  private static SearchableFieldSpec field(
      String fieldName, String searchLabel, List<FieldMapping> mappings) {
    return field(fieldName, searchLabel, mappings, List.of(), false, null);
  }

  private static SearchableFieldSpec field(
      String fieldName,
      String searchLabel,
      List<FieldMapping> mappings,
      String hasValuesFieldName) {
    return field(fieldName, searchLabel, mappings, List.of(), false, hasValuesFieldName);
  }

  private static SearchableFieldSpec field(
      String fieldName,
      String searchLabel,
      List<FieldMapping> mappings,
      List<String> aliases,
      boolean enableAutocomplete) {
    return field(fieldName, searchLabel, mappings, aliases, enableAutocomplete, null);
  }

  private static SearchableFieldSpec field(
      String fieldName,
      String searchLabel,
      List<FieldMapping> mappings,
      List<String> aliases,
      boolean enableAutocomplete,
      String hasValuesFieldName) {
    SearchableAnnotation annotation = mock(SearchableAnnotation.class);
    when(annotation.isV3Searchable()).thenReturn(true);
    when(annotation.resolvedForV3()).thenReturn(annotation);
    when(annotation.getFieldName()).thenReturn(fieldName);
    when(annotation.getFieldNameAliases()).thenReturn(aliases);
    when(annotation.getSearchLabel()).thenReturn(Optional.ofNullable(searchLabel));
    when(annotation.indexedFieldName())
        .thenReturn(
            searchLabel == null || searchLabel.isEmpty()
                ? Optional.empty()
                : Optional.of(searchLabel));
    when(annotation.getEntityFieldName()).thenReturn(Optional.empty());
    when(annotation.getHasValuesFieldName()).thenReturn(Optional.ofNullable(hasValuesFieldName));
    when(annotation.indexedHasValuesFieldName())
        .thenReturn(Optional.ofNullable(hasValuesFieldName));
    when(annotation.getNumValuesFieldName()).thenReturn(Optional.empty());
    when(annotation.getV3Mappings()).thenReturn(mappings);
    when(annotation.isEnableAutocomplete()).thenReturn(enableAutocomplete);
    when(annotation.getFieldType()).thenReturn(SearchableAnnotation.FieldType.KEYWORD);
    when(annotation.isQueryByDefault()).thenReturn(true);

    SearchableFieldSpec field = mock(SearchableFieldSpec.class);
    when(field.getSearchableAnnotation()).thenReturn(annotation);
    return field;
  }

  private static SearchableFieldSpec urnField(String fieldName, String searchLabel) {
    SearchableFieldSpec field =
        field(fieldName, searchLabel, List.of(FieldMapping.root(FieldMapping.Type.KEYWORD)));
    SearchableAnnotation annotation = field.getSearchableAnnotation();
    when(annotation.getFieldType()).thenReturn(SearchableAnnotation.FieldType.URN);
    when(annotation.isQueryByDefault()).thenReturn(true);
    return field;
  }

  private static EntitySpec entity(String aspectName, SearchableFieldSpec... fields) {
    AspectSpec aspect = mock(AspectSpec.class);
    when(aspect.getName()).thenReturn(aspectName);
    when(aspect.getSearchableFieldSpecs()).thenReturn(List.of(fields));
    EntitySpec entity = mock(EntitySpec.class);
    when(entity.getSearchableFieldSpecs()).thenReturn(List.of(fields));
    when(entity.getAspectSpecs()).thenReturn(List.of(aspect));
    return entity;
  }
}
