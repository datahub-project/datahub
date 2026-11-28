package com.linkedin.gms.factory.clientwrite;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.ClientWriteTrackingConfiguration;
import io.micrometer.core.instrument.MeterRegistry;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Beans for client write tracking. The servlet filter itself is registered in the GMS servlet
 * configuration, not here, so contexts that scan this package without serving GMS writes ({@code
 * datahub-upgrade}, through {@code GeneralUpgradeConfiguration}) never pick it up.
 */
@Slf4j
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
    if (configured != null) {
      return configured;
    }
    // Fail closed: a missing block means the configuration did not bind as expected, and a
    // feature the operator may have disabled must not turn itself on.
    log.warn(
        "ingestion.clientWriteTracking is not bound; client write tracking is disabled. Check the"
            + " application configuration.");
    ClientWriteTrackingConfiguration disabled = new ClientWriteTrackingConfiguration();
    disabled.setEnabled(false);
    return disabled;
  }

  /** Closed on shutdown, which stops the summary thread and logs the final partial window. */
  @Bean(destroyMethod = "close")
  @Nonnull
  public ClientWriteRecorder clientWriteRecorder(
      ClientWriteTrackingConfiguration clientWriteTrackingConfiguration,
      @Autowired(required = false) MeterRegistry meterRegistry) {
    return new ClientWriteRecorder(meterRegistry, clientWriteTrackingConfiguration);
  }
}
