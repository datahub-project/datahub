package com.linkedin.datahub.upgrade.system.elasticsearch;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertThrows;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.config.StructuredPropertiesConfiguration;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.V3DocumentIdScheme;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.version.GitVersion;
import io.datahubproject.metadata.context.OperationContext;
import org.testng.annotations.Test;

public class BuildIndicesTest {

  @Test
  public void testConstructionFailsWhenTheDefinitionsCannotBeRead() {
    // The scale-down vote counts a failure as "no reindex due", so the read must fail the run
    // before that vote rather than inside it.
    ConfigurationProvider configurationProvider = mock(ConfigurationProvider.class);
    when(configurationProvider.getStructuredProperties())
        .thenReturn(StructuredPropertiesConfiguration.builder().enabled(true).build());
    AspectDao aspectDao = mock(AspectDao.class);
    when(aspectDao.streamAspects(anyString(), anyString()))
        .thenThrow(new IllegalStateException("database unavailable"));

    assertThrows(
        IllegalStateException.class,
        () ->
            new BuildIndices(
                mock(SystemMetadataService.class),
                mock(TimeseriesAspectService.class),
                mock(EntitySearchService.class),
                mock(GraphService.class),
                mock(BaseElasticSearchComponentsFactory.BaseElasticSearchComponents.class),
                configurationProvider,
                aspectDao,
                mock(OperationContext.class),
                mock(EntityService.class),
                mock(GitVersion.class),
                "",
                mock(UpdateIndicesService.class),
                mock(V3DocumentIdScheme.class),
                false,
                null));
  }
}
