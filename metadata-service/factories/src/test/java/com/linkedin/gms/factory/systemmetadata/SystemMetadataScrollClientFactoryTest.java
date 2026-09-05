package com.linkedin.gms.factory.systemmetadata;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.expectThrows;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.SystemMetadataServiceConfig;
import com.linkedin.metadata.config.SystemMetadataServiceImplementation;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.systemmetadata.scroll.ESSystemMetadataScrollClient;
import com.linkedin.metadata.systemmetadata.scroll.PostgresSystemMetadataScrollClient;
import org.springframework.beans.factory.ObjectProvider;
import org.testng.annotations.Test;

public class SystemMetadataScrollClientFactoryTest {

  @Test
  public void rejectsPgEnabledWithElasticsearchImplementation() {
    SystemMetadataScrollClientFactory factory =
        factory(
            SystemMetadataServiceImplementation.elasticsearch,
            true,
            mock(ObjectProvider.class),
            mock(ObjectProvider.class));
    expectThrows(IllegalStateException.class, factory::systemMetadataScrollClient);
  }

  @Test
  public void usesElasticsearchWhenPostgresDisabled() {
    ObjectProvider<ESSystemMetadataScrollClient> esProvider = mock(ObjectProvider.class);
    ESSystemMetadataScrollClient es = mock(ESSystemMetadataScrollClient.class);
    when(esProvider.getObject()).thenReturn(es);

    SystemMetadataScrollClientFactory factory =
        factory(
            SystemMetadataServiceImplementation.elasticsearch,
            false,
            esProvider,
            mock(ObjectProvider.class));
    assertSame(factory.systemMetadataScrollClient(), es);
  }

  @Test
  public void usesPostgresWhenExclusiveSoT() {
    ObjectProvider<PostgresSystemMetadataScrollClient> pgProvider = mock(ObjectProvider.class);
    PostgresSystemMetadataScrollClient pg = mock(PostgresSystemMetadataScrollClient.class);
    when(pgProvider.getIfAvailable()).thenReturn(pg);

    SystemMetadataScrollClientFactory factory =
        factory(
            SystemMetadataServiceImplementation.postgres,
            true,
            mock(ObjectProvider.class),
            pgProvider);
    assertSame(factory.systemMetadataScrollClient(), pg);
  }

  @Test
  public void rejectsPostgresImplementationWhenBeanMissing() {
    ObjectProvider<PostgresSystemMetadataScrollClient> pgProvider = mock(ObjectProvider.class);
    when(pgProvider.getIfAvailable()).thenReturn(null);

    SystemMetadataScrollClientFactory factory =
        factory(
            SystemMetadataServiceImplementation.postgres,
            true,
            mock(ObjectProvider.class),
            pgProvider);
    expectThrows(IllegalStateException.class, factory::systemMetadataScrollClient);
  }

  private static SystemMetadataScrollClientFactory factory(
      SystemMetadataServiceImplementation impl,
      boolean pgEnabled,
      ObjectProvider<ESSystemMetadataScrollClient> esProvider,
      ObjectProvider<PostgresSystemMetadataScrollClient> pgProvider) {
    ConfigurationProvider configurationProvider = mock(ConfigurationProvider.class);
    SystemMetadataServiceConfig config = new SystemMetadataServiceConfig();
    config.setImplementation(impl);
    when(configurationProvider.getSystemMetadataService()).thenReturn(config);

    PostgresSqlSetupProperties properties = PostgresSqlSetupProperties.disabled();
    properties.getPgSystemMetadata().setEnabled(pgEnabled);
    return new SystemMetadataScrollClientFactory(
        esProvider, pgProvider, configurationProvider, properties);
  }
}
