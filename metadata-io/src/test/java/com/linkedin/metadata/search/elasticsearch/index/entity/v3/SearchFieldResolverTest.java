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
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.SearchFieldResolver.Intent;
import java.util.List;
import java.util.Optional;
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

    assertEquals(resolver.exactPath("name", Intent.FILTER), "name.keyword");
    assertEquals(resolver.exactPath("name", Intent.SORT), "name");
    assertEquals(resolver.exactPath("owners", Intent.FACET), "owners");
    assertFalse(resolver.supports("owners", Intent.FULL_TEXT));
    assertEquals(resolver.exactPath("createdAt", Intent.RANGE), "createdAt");
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

    assertEquals(resolver.exactPath("_entityName", Intent.EXACT), "name.keyword");
    assertEquals(resolver.exactPath("editedName", Intent.FILTER), "name.keyword");
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
    assertEquals(parentResolver.exactPath("parent", Intent.EXACT), "parent");
    assertEquals(parentResolver.parsedKeywordPaths("parent"), List.of("parent"));
    assertTrue(parentResolver.supports("parent", Intent.FULL_TEXT));

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
    when(annotation.getEntityFieldName()).thenReturn(Optional.empty());
    when(annotation.getHasValuesFieldName()).thenReturn(Optional.ofNullable(hasValuesFieldName));
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
