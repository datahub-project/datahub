package com.linkedin.gms.factory.common;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.expectThrows;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.SystemMetadataServiceConfig;
import com.linkedin.metadata.config.SystemMetadataServiceImplementation;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.systemmetadata.ElasticSearchSystemMetadataService;
import com.linkedin.metadata.systemmetadata.PostgresSystemMetadataService;
import org.springframework.beans.factory.ObjectProvider;
import org.testng.annotations.Test;

public class SystemMetadataServiceFactoryTest {

  @Test
  public void rejectsPgEnabledWithElasticsearchImplementation() {
    SystemMetadataServiceFactory factory =
        factory(
            SystemMetadataServiceImplementation.elasticsearch,
            true,
            mock(ObjectProvider.class),
            mock(ObjectProvider.class));
    expectThrows(IllegalStateException.class, factory::createInstance);
  }

  @Test
  public void usesElasticsearchWhenPostgresDisabled() {
    ObjectProvider<ElasticSearchSystemMetadataService> esProvider = mock(ObjectProvider.class);
    ElasticSearchSystemMetadataService es = mock(ElasticSearchSystemMetadataService.class);
    when(esProvider.getObject()).thenReturn(es);

    SystemMetadataServiceFactory factory =
        factory(
            SystemMetadataServiceImplementation.elasticsearch,
            false,
            esProvider,
            mock(ObjectProvider.class));
    assertSame(factory.createInstance(), es);
  }

  @Test
  public void usesPostgresWhenExclusiveSoT() {
    ObjectProvider<PostgresSystemMetadataService> pgProvider = mock(ObjectProvider.class);
    PostgresSystemMetadataService pg = mock(PostgresSystemMetadataService.class);
    when(pgProvider.getIfAvailable()).thenReturn(pg);

    SystemMetadataServiceFactory factory =
        factory(
            SystemMetadataServiceImplementation.postgres,
            true,
            mock(ObjectProvider.class),
            pgProvider);
    assertSame(factory.createInstance(), pg);
  }

  @Test
  public void rejectsPostgresImplementationWhenBeanMissing() {
    ObjectProvider<PostgresSystemMetadataService> pgProvider = mock(ObjectProvider.class);
    when(pgProvider.getIfAvailable()).thenReturn(null);

    SystemMetadataServiceFactory factory =
        factory(
            SystemMetadataServiceImplementation.postgres,
            true,
            mock(ObjectProvider.class),
            pgProvider);
    expectThrows(IllegalStateException.class, factory::createInstance);
  }

  private static SystemMetadataServiceFactory factory(
      SystemMetadataServiceImplementation impl,
      boolean pgEnabled,
      ObjectProvider<ElasticSearchSystemMetadataService> esProvider,
      ObjectProvider<PostgresSystemMetadataService> pgProvider) {
    ConfigurationProvider configurationProvider = mock(ConfigurationProvider.class);
    SystemMetadataServiceConfig config = new SystemMetadataServiceConfig();
    config.setImplementation(impl);
    when(configurationProvider.getSystemMetadataService()).thenReturn(config);

    PostgresSqlSetupProperties properties = PostgresSqlSetupProperties.disabled();
    properties.getPgSystemMetadata().setEnabled(pgEnabled);
    return new SystemMetadataServiceFactory(
        esProvider, pgProvider, configurationProvider, properties);
  }
}
