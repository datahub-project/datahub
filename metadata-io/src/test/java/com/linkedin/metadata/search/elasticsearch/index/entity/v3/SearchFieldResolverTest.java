package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

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
    return field(fieldName, searchLabel, mappings, List.of(), false);
  }

  private static SearchableFieldSpec field(
      String fieldName,
      String searchLabel,
      List<FieldMapping> mappings,
      List<String> aliases,
      boolean enableAutocomplete) {
    SearchableAnnotation annotation = mock(SearchableAnnotation.class);
    when(annotation.isV3Searchable()).thenReturn(true);
    when(annotation.resolvedForV3()).thenReturn(annotation);
    when(annotation.getFieldName()).thenReturn(fieldName);
    when(annotation.getFieldNameAliases()).thenReturn(aliases);
    when(annotation.getSearchLabel()).thenReturn(Optional.ofNullable(searchLabel));
    when(annotation.getEntityFieldName()).thenReturn(Optional.empty());
    when(annotation.getHasValuesFieldName()).thenReturn(Optional.empty());
    when(annotation.getNumValuesFieldName()).thenReturn(Optional.empty());
    when(annotation.getV3Mappings()).thenReturn(mappings);
    when(annotation.isEnableAutocomplete()).thenReturn(enableAutocomplete);

    SearchableFieldSpec field = mock(SearchableFieldSpec.class);
    when(field.getSearchableAnnotation()).thenReturn(annotation);
    return field;
  }
}
