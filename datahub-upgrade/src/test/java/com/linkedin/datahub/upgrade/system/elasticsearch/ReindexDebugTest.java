package com.linkedin.datahub.upgrade.system.elasticsearch;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.config.StructuredPropertiesConfiguration;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import java.util.Set;
import java.util.stream.Stream;
import org.testng.annotations.Test;

public class ReindexDebugTest {

  @Test
  public void testConstructionReadsNoStructuredPropertyDefinitions() {
    // Spring builds this upgrade for every general upgrade run, so building it must not read
    // definitions; only a ReindexDebug run does.
    ConfigurationProvider configurationProvider = mock(ConfigurationProvider.class);
    when(configurationProvider.getStructuredProperties())
        .thenReturn(StructuredPropertiesConfiguration.builder().enabled(true).build());
    AspectDao aspectDao = mock(AspectDao.class);

    ReindexDebug reindexDebug =
        new ReindexDebug(
            mock(SystemMetadataService.class),
            mock(TimeseriesAspectService.class),
            mock(EntitySearchService.class),
            mock(GraphService.class),
            mock(BaseElasticSearchComponentsFactory.BaseElasticSearchComponents.class),
            configurationProvider,
            aspectDao);

    assertEquals(reindexDebug.steps().size(), 3);
    verifyNoInteractions(aspectDao);
  }

  @Test
  public void testStructuredPropertiesLoadOnceOnFirstUse() {
    ConfigurationProvider configurationProvider = mock(ConfigurationProvider.class);
    when(configurationProvider.getStructuredProperties())
        .thenReturn(StructuredPropertiesConfiguration.builder().enabled(true).build());
    AspectDao aspectDao = mock(AspectDao.class);
    when(aspectDao.streamAspects(anyString(), anyString()))
        .thenReturn(Stream.empty())
        .thenReturn(Stream.empty());

    Set<Pair<Urn, StructuredPropertyDefinition>> structuredProperties =
        ReindexDebug.lazyStructuredProperties(configurationProvider, aspectDao);
    verifyNoInteractions(aspectDao);

    assertTrue(structuredProperties.isEmpty());
    assertTrue(structuredProperties.isEmpty());
    // One load: the status stream and the definition stream.
    verify(aspectDao, times(2)).streamAspects(anyString(), anyString());
  }
}
