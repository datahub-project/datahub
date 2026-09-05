package com.linkedin.gms.factory.timeseries;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.expectThrows;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.TimeseriesAspectServiceConfig;
import com.linkedin.metadata.config.TimeseriesAspectServiceImplementation;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.timeseries.elastic.ElasticSearchTimeseriesAspectService;
import org.springframework.beans.factory.ObjectProvider;
import org.testng.annotations.Test;

public class TimeseriesAspectServiceFactoryTest {

  @Test
  public void rejectsPgTimeseriesEnabledWithElasticsearchImplementation() {
    ConfigurationProvider configurationProvider = mock(ConfigurationProvider.class);
    TimeseriesAspectServiceConfig config = mock(TimeseriesAspectServiceConfig.class);
    when(config.getImplementation())
        .thenReturn(TimeseriesAspectServiceImplementation.elasticsearch);
    when(configurationProvider.getTimeseriesAspectService()).thenReturn(config);

    PostgresSqlSetupProperties properties = new PostgresSqlSetupProperties();
    properties.getPgTimeseries().setEnabled(true);

    TimeseriesAspectServiceFactory factory =
        new TimeseriesAspectServiceFactory(
            mock(ObjectProvider.class),
            configurationProvider,
            properties,
            mock(ObjectProvider.class));

    expectThrows(IllegalStateException.class, factory::timeseriesAspectService);
  }

  @Test
  public void usesElasticsearchWhenPgTimeseriesIsDisabled() {
    ConfigurationProvider configurationProvider = mock(ConfigurationProvider.class);
    TimeseriesAspectServiceConfig config = mock(TimeseriesAspectServiceConfig.class);
    when(config.getImplementation())
        .thenReturn(TimeseriesAspectServiceImplementation.elasticsearch);
    when(configurationProvider.getTimeseriesAspectService()).thenReturn(config);

    PostgresSqlSetupProperties properties = new PostgresSqlSetupProperties();
    ObjectProvider<ElasticSearchTimeseriesAspectService> elasticsearchProvider =
        mock(ObjectProvider.class);
    ElasticSearchTimeseriesAspectService elasticsearch =
        mock(ElasticSearchTimeseriesAspectService.class);
    when(elasticsearchProvider.getObject()).thenReturn(elasticsearch);

    TimeseriesAspectServiceFactory factory =
        new TimeseriesAspectServiceFactory(
            elasticsearchProvider, configurationProvider, properties, mock(ObjectProvider.class));

    assertSame(factory.timeseriesAspectService(), elasticsearch);
  }
}
