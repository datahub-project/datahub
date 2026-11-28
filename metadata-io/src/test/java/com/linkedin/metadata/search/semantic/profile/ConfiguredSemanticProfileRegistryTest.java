package com.linkedin.metadata.search.semantic.profile;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.metadata.config.search.ModelEmbeddingConfig;
import com.linkedin.metadata.config.search.SemanticProfilesConfiguration;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import java.util.LinkedHashMap;
import java.util.Map;
import org.testng.annotations.Test;

public class ConfiguredSemanticProfileRegistryTest {

  private static SemanticSearchConfiguration configuration(
      boolean generationEnabled, Map<String, String> active) {
    ModelEmbeddingConfig service = new ModelEmbeddingConfig();
    service.setServiceOwned(true);
    SemanticProfilesConfiguration profiles = new SemanticProfilesConfiguration();
    profiles.setGenerationEnabled(generationEnabled);
    profiles.setActive(new LinkedHashMap<>(active));
    SemanticSearchConfiguration configuration = new SemanticSearchConfiguration();
    configuration.setModels(Map.of("service_model", service));
    configuration.setProfiles(profiles);
    return configuration;
  }

  @Test
  public void testGenerationWithoutActiveFailsStartup() {
    expectThrows(
        IllegalArgumentException.class,
        () -> new ConfiguredSemanticProfileRegistry(configuration(true, Map.of())));
  }

  @Test
  public void testGenerationWithActiveServiceProfileStarts() {
    ConfiguredSemanticProfileRegistry registry =
        new ConfiguredSemanticProfileRegistry(
            configuration(true, Map.of("document", "service_model")));

    assertTrue(registry.generationEnabled());
  }

  @Test
  public void testEmptyActiveIsAcceptedWhileGenerationIsOff() {
    new ConfiguredSemanticProfileRegistry(configuration(false, Map.of()));
  }
}
