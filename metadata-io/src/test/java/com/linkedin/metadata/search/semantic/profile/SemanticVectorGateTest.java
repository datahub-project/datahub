package com.linkedin.metadata.search.semantic.profile;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.search.ModelEmbeddingConfig;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.semantic.profile.SemanticVectorGate.Rejection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;
import org.testng.annotations.Test;

public class SemanticVectorGateTest {

  private static EntitySpec entity(String name, String searchGroup) {
    EntitySpec spec = mock(EntitySpec.class);
    when(spec.getName()).thenReturn(name);
    when(spec.getSearchGroup()).thenReturn(searchGroup);
    return spec;
  }

  private static SemanticVectorGate gate() {
    return new SemanticVectorGate(Set.of("chart"), Map.of("model_a", 2));
  }

  @Test
  public void testPrimaryGroupAndEnabledEntitiesAreEligible() {
    assertTrue(gate().isEntityEligible(entity("dataset", "primary")));
    assertTrue(gate().isEntityEligible(entity("chart", "default")));
    assertFalse(gate().isEntityEligible(entity("dashboard", "default")));
  }

  @Test
  public void testRejectionOrderIsEntityThenModelThenDimension() {
    EntitySpec disabled = entity("dashboard", "default");
    EntitySpec primary = entity("dataset", "primary");

    assertEquals(
        gate().reject(disabled, "unknown", IntStream.of(9)),
        Optional.of(Rejection.ENTITY_NOT_ENABLED));
    assertEquals(
        gate().reject(primary, "unknown", IntStream.of(9)),
        Optional.of(Rejection.MODEL_NOT_CONFIGURED));
    assertEquals(
        gate().reject(primary, "model_a", IntStream.of(2, 3)),
        Optional.of(Rejection.DIMENSION_MISMATCH));
    assertEquals(gate().reject(primary, "model_a", IntStream.of(2, 2)), Optional.empty());
    assertEquals(gate().reject(primary, "model_a", IntStream.empty()), Optional.empty());
  }

  @Test
  public void testAChunkWithoutAVectorIsADimensionMismatch() {
    assertEquals(
        gate().reject(entity("dataset", "primary"), "model_a", IntStream.of(0)),
        Optional.of(Rejection.DIMENSION_MISMATCH));
  }

  @Test
  public void testFromConfigurationUsesEnabledEntitiesOnlyWhenSemanticSearchIsOn() {
    ModelEmbeddingConfig model = new ModelEmbeddingConfig();
    model.setVectorDimension(4);
    SemanticSearchConfiguration configuration = new SemanticSearchConfiguration();
    configuration.setEnabled(true);
    configuration.setEnabledEntities(Set.of("chart"));
    configuration.setModels(Map.of("model_a", model));

    SemanticVectorGate on = SemanticVectorGate.from(configuration);
    assertTrue(on.isEntityEligible(entity("chart", "default")));
    assertEquals(on.modelDimensions(), Map.of("model_a", 4));

    configuration.setEnabled(false);
    assertFalse(
        SemanticVectorGate.from(configuration).isEntityEligible(entity("chart", "default")));
  }

  @Test
  public void testMissingConfigurationAcceptsNoModels() {
    SemanticVectorGate gate = SemanticVectorGate.from(null);

    assertTrue(gate.modelDimensions().isEmpty());
    assertFalse(gate.isEntityEligible(entity("chart", "default")));
  }
}
