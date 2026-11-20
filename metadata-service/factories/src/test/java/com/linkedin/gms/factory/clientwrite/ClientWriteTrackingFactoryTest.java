package com.linkedin.gms.factory.clientwrite;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.ClientWriteTrackingConfiguration;
import com.linkedin.metadata.config.DataHubConfiguration;
import com.linkedin.metadata.config.GMSConfiguration;
import com.linkedin.metadata.config.IngestionConfiguration;
import io.datahubproject.test.wiring.WiringCoverage;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.testng.annotations.Test;

public class ClientWriteTrackingFactoryTest {

  /**
   * ConfigurationProvider is itself a configuration class whose gmsConfiguration bean needs gms.
   */
  private static ConfigurationProvider provider(IngestionConfiguration ingestion) {
    DataHubConfiguration datahub = new DataHubConfiguration();
    datahub.setGms(new GMSConfiguration());
    ConfigurationProvider provider = new ConfigurationProvider();
    provider.setDatahub(datahub);
    provider.setIngestion(ingestion);
    return provider;
  }

  private static ApplicationContextRunner runner(ConfigurationProvider configurationProvider) {
    return new ApplicationContextRunner()
        .withInitializer(WiringCoverage.initializer())
        .withUserConfiguration(ClientWriteTrackingFactory.class)
        .withBean(ConfigurationProvider.class, () -> configurationProvider)
        .withBean(MeterRegistry.class, SimpleMeterRegistry::new);
  }

  @Test
  public void testUsesConfiguredSettings() {
    ClientWriteTrackingConfiguration configured = new ClientWriteTrackingConfiguration();
    configured.setEnabled(false);
    IngestionConfiguration ingestion = new IngestionConfiguration();
    ingestion.setClientWriteTracking(configured);
    runner(provider(ingestion))
        .run(
            context -> {
              assertThat(context).hasSingleBean(ClientWriteRecorder.class);
              assertThat(context.getBean(ClientWriteTrackingConfiguration.class).isEnabled())
                  .isFalse();
            });
  }

  @Test
  public void testDefaultsToEnabledWhenTheBlockIsMissing() {
    runner(provider(null))
        .run(
            context -> {
              assertThat(context).hasSingleBean(ClientWriteRecorder.class);
              assertThat(context.getBean(ClientWriteTrackingConfiguration.class).isEnabled())
                  .isTrue();
            });
  }
}
