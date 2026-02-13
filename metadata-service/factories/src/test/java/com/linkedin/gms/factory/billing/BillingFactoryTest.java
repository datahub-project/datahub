package com.linkedin.gms.factory.billing;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.billing.BillingHandler;
import com.linkedin.metadata.config.BillingConfiguration;
import com.linkedin.metadata.config.DataHubConfiguration;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

/** Unit tests for BillingFactory. */
@TestPropertySource(
    properties = {
      "baseUrl=https://test-customer.trials.acryl.io",
      "datahub.billing.enabled=true",
      "datahub.billing.provider=metronome",
      "datahub.billing.metronome.apiKey=test-api-key",
      "datahub.billing.metronome.baseUrl=https://api.metronome.com",
      "datahub.billing.metronome.packageAlias=standard"
    })
@ContextConfiguration(classes = {BillingFactoryTest.BillingFactoryTestConfig.class})
public class BillingFactoryTest extends AbstractTestNGSpringContextTests {

  @Test
  public void testBillingHandlerCreationWhenEnabled() {
    BillingHandler billingHandler =
        applicationContext.getBean("billingHandler", BillingHandler.class);

    assertNotNull(billingHandler, "BillingHandler should be created when billing is enabled");
    assertTrue(billingHandler.isEnabled(), "Billing should be enabled");
  }

  @Test
  public void testBillingHandlerNotCreatedWhenDisabled() {
    org.springframework.context.annotation.AnnotationConfigApplicationContext disabledContext =
        new org.springframework.context.annotation.AnnotationConfigApplicationContext();
    disabledContext.register(BillingDisabledTestConfig.class);
    disabledContext.refresh();

    boolean beanExists = disabledContext.containsBean("billingHandler");
    assertFalse(beanExists, "BillingHandler should not be created when billing is disabled");

    disabledContext.close();
  }

  @Test
  public void testBillingHandlerUsesCorrectProvider() {
    BillingHandler billingHandler =
        applicationContext.getBean("billingHandler", BillingHandler.class);

    assertNotNull(billingHandler);
    assertTrue(billingHandler.isEnabled());
  }

  @Test
  public void testDeriveCustomerNameWithHttpsTrialUrl() {
    BillingFactory factory = new BillingFactory();
    String customerName = factory.deriveCustomerName("https://customer-name.trials.acryl.io");
    assertEquals(
        customerName, "customer-name", "Should extract customer name from HTTPS trial URL");
  }

  @Test
  public void testDeriveCustomerNameWithHttpTrialUrl() {
    BillingFactory factory = new BillingFactory();
    String customerName = factory.deriveCustomerName("http://customer-name.trials.acryl.io");
    assertEquals(customerName, "customer-name", "Should extract customer name from HTTP trial URL");
  }

  @Test
  public void testDeriveCustomerNameWithoutProtocol() {
    BillingFactory factory = new BillingFactory();
    String customerName = factory.deriveCustomerName("customer-name.trials.acryl.io");
    assertEquals(customerName, "customer-name", "Should extract customer name without protocol");
  }

  @Test
  public void testDeriveCustomerNameWithPath() {
    BillingFactory factory = new BillingFactory();
    String customerName =
        factory.deriveCustomerName("https://customer-name.trials.acryl.io/some/path");
    assertEquals(customerName, "customer-name", "Should extract customer name and ignore path");
  }

  @Test
  public void testDeriveCustomerNameWithUnderscores() {
    BillingFactory factory = new BillingFactory();
    String customerName = factory.deriveCustomerName("https://my_customer.trials.acryl.io");
    assertEquals(customerName, "my_customer", "Should handle customer names with underscores");
  }

  @Test
  public void testDeriveCustomerNameWithNoDot() {
    BillingFactory factory = new BillingFactory();
    String customerName = factory.deriveCustomerName("https://localhost");
    assertEquals(customerName, "localhost", "Should return full hostname when no dot is present");
  }

  @TestConfiguration
  @ComponentScan(basePackages = "com.linkedin.gms.factory.billing")
  static class BillingFactoryTestConfig {

    @Bean
    @Primary
    public ConfigurationProvider configurationProvider() {
      ConfigurationProvider mockProvider = mock(ConfigurationProvider.class);
      when(mockProvider.getDatahub()).thenReturn(dataHubConfiguration());
      return mockProvider;
    }

    @Bean
    @Primary
    public DataHubConfiguration dataHubConfiguration() {
      DataHubConfiguration config = new DataHubConfiguration();
      config.setBilling(billingConfiguration());
      return config;
    }

    @Bean
    @Primary
    public BillingConfiguration billingConfiguration() {
      BillingConfiguration config = new BillingConfiguration();
      config.setEnabled(true);
      config.setProvider("metronome");

      BillingConfiguration.MetronomeConfiguration metronomeConfig =
          new BillingConfiguration.MetronomeConfiguration();
      metronomeConfig.setApiKey("test-api-key");
      metronomeConfig.setBaseUrl("https://api.metronome.com");
      metronomeConfig.setPackageAlias("standard");

      // Set up product ID mappings
      java.util.Map<String, String> products = new java.util.HashMap<>();
      products.put("askDataHubProductId", "test-product");
      metronomeConfig.setProducts(products);

      config.setMetronome(metronomeConfig);

      return config;
    }

    @Bean
    @Qualifier("entityService")
    public EntityService<?> entityService() {
      return mock(EntityService.class);
    }

    @Bean
    @Qualifier("systemOperationContext")
    public OperationContext systemOperationContext() {
      return mock(OperationContext.class);
    }
  }

  @TestConfiguration
  @ComponentScan(basePackages = "com.linkedin.gms.factory.billing")
  static class BillingDisabledTestConfig {

    @Bean
    @Primary
    public ConfigurationProvider configurationProvider() {
      ConfigurationProvider mockProvider = mock(ConfigurationProvider.class);
      when(mockProvider.getDatahub()).thenReturn(dataHubConfiguration());
      return mockProvider;
    }

    @Bean
    @Primary
    public DataHubConfiguration dataHubConfiguration() {
      DataHubConfiguration config = new DataHubConfiguration();
      config.setBilling(billingConfiguration());
      return config;
    }

    @Bean
    @Primary
    public BillingConfiguration billingConfiguration() {
      BillingConfiguration config = new BillingConfiguration();
      config.setEnabled(false);
      config.setProvider("metronome");
      return config;
    }

    @Bean
    @Qualifier("entityService")
    public EntityService<?> entityService() {
      return mock(EntityService.class);
    }

    @Bean
    @Qualifier("systemOperationContext")
    public OperationContext systemOperationContext() {
      return mock(OperationContext.class);
    }
  }
}
