package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.SearchFieldResolver.Intent;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.testng.annotations.Test;

public class V3SearchFieldModelTest {

  private static final EntityRegistry REGISTRY = TestOperationContexts.defaultEntityRegistry();

  @Test
  public void testModelMatchesTheFieldsItCaches() {
    List<EntitySpec> every = List.copyOf(REGISTRY.getEntitySpecs().values());
    for (List<EntitySpec> specs : List.of(List.of(REGISTRY.getEntitySpec("dataset")), every)) {
      V3SearchFieldModel model = V3SearchFieldModel.of(specs);

      assertEquals(model.storedParsedNameFields(), V3SearchFields.storedParsedNameFields(specs));
      assertEquals(model.fullTextFields(), V3SearchFields.fullTextFields(specs));
      assertEquals(model.analyzerSubfields(), V3SearchFields.analyzerSubfields(specs));
      List<String> configured = List.of("name", "_search.description", "tags", "missing");
      assertEquals(
          model.copyDestinations(configured), V3SearchFields.copyDestinations(specs, configured));
      model
          .storedParsedNameFields()
          .forEach(
              (label, paths) ->
                  paths.forEach(
                      path -> assertEquals(model.storedParsedNameLabels().get(path), label)));
    }
  }

  @Test
  public void testSameSpecsShareOneModel() {
    List<EntitySpec> every = List.copyOf(REGISTRY.getEntitySpecs().values());

    V3SearchFieldModel model = V3SearchFieldModel.of(every);

    assertSame(V3SearchFieldModel.of(new ArrayList<>(every)), model);
    assertSame(V3SearchFieldModel.of(every).resolver(), model.resolver());
    assertSame(
        V3SearchFieldModel.forIndexGroup(REGISTRY, List.of(REGISTRY.getEntitySpec("dataset"))),
        V3SearchFieldModel.of(
            V3SearchFields.indexGroupSpecs(REGISTRY, List.of(REGISTRY.getEntitySpec("dataset")))));
  }

  @Test
  public void testSearchReadsTheSharedMappingFieldModel() {
    V3SearchFieldModel shared = V3SearchFieldModel.forSearchableEntities(REGISTRY);
    SearchFieldResolver document = shared.resolver();
    SearchFieldResolver dataset = V3SearchFieldModel.forSearchableEntities(REGISTRY).resolver();

    SearchFieldResolver.Field description = document.resolve("description", Intent.FULL_TEXT);

    assertSame(document, dataset);
    assertEquals(description.getPath(), "description");
    assertTrue(description.getSubfields().containsAll(Set.of("text", "stemmed")));
    assertFalse(
        V3SearchFieldModel.forIndexGroup(REGISTRY, List.of(REGISTRY.getEntitySpec("document")))
            .resolver()
            .supports("description", Intent.FULL_TEXT));
    assertEquals(document.exactPath("id", Intent.EXACT), "id");
    assertEquals(document.exactPath("domains.keyword", Intent.FILTER), "domains_urn");
    assertEquals(document.exactPath("id", Intent.FILTER), "name");
    assertEquals(document.exactPath("title", Intent.FILTER), "name");
    assertEquals(document.exactPath("createdAt", Intent.RANGE), "sourceCreatedAt");
    assertEquals(document.exactPath("createdAt", Intent.SORT), "sourceCreatedAt");
    assertEquals(
        V3SearchFieldModel.of(List.of(REGISTRY.getEntitySpec("mlModel")))
            .resolver()
            .exactPath("id", Intent.EXACT),
        "name");
    assertEquals(
        V3SearchFieldModel.of(List.of(REGISTRY.getEntitySpec("dataset")))
            .resolver()
            .exactPath("id", Intent.EXACT),
        "name");
  }
}
