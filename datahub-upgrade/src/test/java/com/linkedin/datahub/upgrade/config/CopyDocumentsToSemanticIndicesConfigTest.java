package com.linkedin.datahub.upgrade.config;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.upgrade.system.BlockingSystemUpgrade;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.SearchV3ReadHold;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Map;
import java.util.Set;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.testng.annotations.Test;

public class CopyDocumentsToSemanticIndicesConfigTest {

  @Test
  public void testHoldDoesNotStartTheCopyWhenV2WasConfiguredOff() {
    BlockingSystemUpgrade upgrade = copy(false, true);

    assertTrue(upgrade.steps().isEmpty());
  }

  @Test
  public void testConfiguredV2StillCopies() {
    BlockingSystemUpgrade upgrade = copy(true, true);

    assertEquals(upgrade.steps().size(), 1);
    assertEquals(upgrade.steps().get(0).id(), "CopyDocumentsToSemanticIndex_dataset");
  }

  private static BlockingSystemUpgrade copy(boolean configuredV2, boolean holdOverlay) {
    StandardEnvironment environment = new StandardEnvironment();
    environment
        .getPropertySources()
        .addFirst(
            new MapPropertySource(
                "configured", Map.of(SearchV3ReadHold.V2_ENABLED, Boolean.toString(configuredV2))));
    if (holdOverlay) {
      environment
          .getPropertySources()
          .addFirst(
              new MapPropertySource(
                  SearchV3ReadHold.PROPERTY_SOURCE, Map.of(SearchV3ReadHold.V2_ENABLED, "true")));
    }
    SemanticSearchConfiguration semantic = new SemanticSearchConfiguration();
    semantic.setEnabledEntities(Set.of("dataset"));
    ConfigurationProvider provider = mock(ConfigurationProvider.class);
    when(provider.getElasticSearch())
        .thenReturn(
            ElasticSearchConfiguration.builder()
                .entityIndex(EntityIndexConfiguration.builder().semanticSearch(semantic).build())
                .build());

    return new CopyDocumentsToSemanticIndicesConfig()
        .copyDocumentsToSemanticIndices(
            mock(OperationContext.class),
            mock(EntityService.class),
            provider,
            mock(IndexConvention.class),
            true,
            environment);
  }
}
