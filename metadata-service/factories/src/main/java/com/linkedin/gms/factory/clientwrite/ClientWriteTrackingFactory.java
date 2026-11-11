package com.linkedin.gms.factory.clientwrite;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.ClientWriteTrackingConfiguration;
import io.micrometer.core.instrument.MeterRegistry;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Beans for client write tracking. The servlet filter itself is registered in the GMS servlet
 * configuration, not here, so contexts that scan this package without serving GMS writes (the
 * consumer jobs) never pick it up.
 */
@Configuration
public class ClientWriteTrackingFactory {

  @Bean
  @Nonnull
  public ClientWriteTrackingConfiguration clientWriteTrackingConfiguration(
      ConfigurationProvider configurationProvider) {
    ClientWriteTrackingConfiguration configured =
        configurationProvider.getIngestion() != null
            ? configurationProvider.getIngestion().getClientWriteTracking()
            : null;
    return configured != null ? configured : new ClientWriteTrackingConfiguration();
  }

  @Bean
  @Nonnull
  public ClientWriteRecorder clientWriteRecorder(
      ClientWriteTrackingConfiguration clientWriteTrackingConfiguration,
      @Autowired(required = false) MeterRegistry meterRegistry) {
    return new ClientWriteRecorder(
        meterRegistry,
        clientWriteTrackingConfiguration.getSummaryIntervalSeconds(),
        clientWriteTrackingConfiguration.getMaxTrackedKeys());
  }
}
