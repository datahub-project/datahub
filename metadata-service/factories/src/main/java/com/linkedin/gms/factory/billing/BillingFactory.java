package com.linkedin.gms.factory.billing;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.billing.BillingHandler;
import com.linkedin.metadata.billing.BillingProvider;
import com.linkedin.metadata.billing.metronome.MetronomeClient;
import com.linkedin.metadata.config.BillingConfiguration;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

/**
 * Factory for creating billing-related beans.
 *
 * <p>This factory conditionally creates beans based on the billing.enabled configuration property.
 * When billing is enabled, it creates a BillingHandler with the configured provider
 * (Metronome/Stripe/etc.)
 *
 * @see BillingHandler
 * @see BillingProvider
 * @see MetronomeClient
 */
@Slf4j
@Configuration
public class BillingFactory {

  @Value("${baseUrl}")
  private String baseUrl;

  /**
   * Create BillingHandler when billing is enabled.
   *
   * @return Configured BillingHandler with the active billing provider
   */
  @Bean(name = "billingHandler")
  @Scope("singleton")
  @ConditionalOnProperty(name = "datahub.billing.enabled", havingValue = "true")
  @Nonnull
  protected BillingHandler createBillingHandler(ConfigurationProvider configProvider) {
    BillingConfiguration billingConfig = configProvider.getDatahub().getBilling();

    BillingProvider provider = createProvider(billingConfig);
    String customerName = deriveCustomerName(baseUrl);
    BillingHandler handler = new BillingHandler(billingConfig, provider, customerName);

    log.info(
        "Successfully created BillingHandler for provider: {} with customer: {}",
        billingConfig.getProvider(),
        customerName);

    return handler;
  }

  /**
   * Create a billing provider based on configuration.
   *
   * @param config Billing configuration
   * @return Configured billing provider
   * @throws IllegalArgumentException if provider is unknown
   */
  private BillingProvider createProvider(BillingConfiguration config) {
    String provider = config.getProvider();

    if ("metronome".equalsIgnoreCase(provider)) {
      log.info("Creating Metronome billing provider");
      CloseableHttpClient httpClient = HttpClients.createDefault();
      return new MetronomeClient(httpClient, config.getMetronome());
    }

    throw new IllegalArgumentException("Unknown billing provider: " + provider);
  }

  /**
   * Derive customer name from base URL.
   *
   * <p>Extracts the customer name from URL "https://<customer_name>.trials.acryl.io" ->
   * "<customer_name>"
   *
   * @param baseUrl DataHub base URL
   * @return Customer name for billing provider
   */
  private String deriveCustomerName(String baseUrl) {
    // Remove protocol and extract hostname
    String cleaned = baseUrl.replaceFirst("^https?://", "");
    String hostname = cleaned.split("/")[0]; // Remove path if present

    // Extract customer name from hostname
    int dotIndex = hostname.indexOf(".");
    return dotIndex > 0 ? hostname.substring(0, dotIndex) : hostname;
  }
}
