package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.ArrayList;
import java.util.List;
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
}
