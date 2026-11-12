package com.linkedin.datahub.upgrade.system.kafka;

import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.config.KafkaSetupConfig;
import com.linkedin.datahub.upgrade.config.NonBlockingConfigs;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.kafka.DataHubMskIamClientCallbackHandler;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.kafka.ProducerConfiguration;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.MapPropertySource;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Under MSK IAM, building a Kafka admin client needs the shared AWS credentials that {@code
 * AwsClientFactory} installs. Each case starts a minimal upgrade-style context holding only one
 * configuration (no component scan), so nothing else loads {@code AwsClientFactory}, and builds the
 * admin client of every step that configuration registers.
 */
public class KafkaSetupMskIamWiringTest {

  /**
   * The shared credentials are process-wide; cached Spring test contexts elsewhere in this JVM may
   * have installed them, which would make both cases pass without exercising the wiring.
   */
  @BeforeMethod
  @AfterMethod
  public void resetSharedCredentials() {
    DataHubMskIamClientCallbackHandler.reset();
  }

  @DataProvider(name = "upgradeConfigs")
  public Object[][] upgradeConfigs() {
    return new Object[][] {
      {KafkaSetupConfig.class, "kafkaSetup", 2},
      {NonBlockingConfigs.class, "kafkaSetupNonBlocking", 1}
    };
  }

  @Test(dataProvider = "upgradeConfigs")
  public void upgradeConfigLoadsAwsCredentialsForItsAdminClients(
      Class<?> config, String beanName, int adminClientSteps) {
    List<Map<String, Object>> adminConfigs = new ArrayList<>();
    try (var adminStatic = mockStatic(AdminClient.class);
        AnnotationConfigApplicationContext ctx = newContext(config)) {
      adminStatic
          .when(() -> AdminClient.create(anyMap()))
          .thenAnswer(
              invocation -> {
                adminConfigs.add(new HashMap<>(invocation.<Map<String, Object>>getArgument(0)));
                return mock(AdminClient.class);
              });

      for (UpgradeStep step : ctx.getBean(beanName, Upgrade.class).steps()) {
        createAdminClient(step);
      }

      assertEquals(adminConfigs.size(), adminClientSteps);
      for (Map<String, Object> adminConfig : adminConfigs) {
        assertEquals(
            adminConfig.get("sasl.client.callback.handler.class"),
            DataHubMskIamClientCallbackHandler.class.getName());
      }
    }
  }

  @Test
  public void contextWithoutAwsClientFactoryFailsNamingWhatToLoad() {
    try (var ignored = mockStatic(AdminClient.class);
        AnnotationConfigApplicationContext ctx = newContext(BareKafkaSetupConfig.class)) {
      UpgradeStep step = ctx.getBean(KafkaSetup.class).steps().get(0);

      IllegalStateException e =
          expectThrows(IllegalStateException.class, () -> createAdminClient(step));
      assertTrue(e.getMessage().contains("com.linkedin.gms.factory.aws.AwsClientFactory"));
    }
  }

  /** An upgrade configuration that builds KafkaSetup without importing AwsClientFactory. */
  @Configuration
  static class BareKafkaSetupConfig {
    @Bean
    KafkaSetup kafkaSetup(
        OperationContext opContext,
        ConfigurationProvider configurationProvider,
        KafkaProperties properties) {
      return new KafkaSetup(opContext, configurationProvider.getKafka(), properties);
    }
  }

  private static void createAdminClient(UpgradeStep step) {
    if (step instanceof WaitForKafkaReadyStep waitStep) {
      waitStep.createAdminClient();
    } else if (step instanceof CreateKafkaTopicsStep createStep) {
      createStep.createAdminClient();
    } else if (step instanceof ConfluentSchemaRegistryCleanupPolicyStep cleanupStep) {
      cleanupStep.createAdminClient();
    } else {
      throw new AssertionError("Unexpected step " + step.id());
    }
  }

  private static AnnotationConfigApplicationContext newContext(Class<?> config) {
    AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
    // ConfigurationProvider binds KafkaProperties from spring.kafka.*, as in the real upgrade job.
    ctx.getEnvironment()
        .getPropertySources()
        .addFirst(new MapPropertySource("test", mskIamEnvironment()));
    // Lazy, so only the requested bean and its declared dependencies are built.
    ctx.addBeanFactoryPostProcessor(
        beanFactory -> {
          for (String name : beanFactory.getBeanDefinitionNames()) {
            beanFactory.getBeanDefinition(name).setLazyInit(true);
          }
        });
    ctx.registerBean(
        ApplicationArguments.class, () -> new DefaultApplicationArguments("SystemUpdate"));
    ctx.registerBean(OperationContext.class, () -> mock(OperationContext.class));
    ctx.registerBean(
        "configurationProvider",
        ConfigurationProvider.class,
        KafkaSetupMskIamWiringTest::configurationProvider);
    ctx.register(config);
    ctx.refresh();
    return ctx;
  }

  /** The documented MSK IAM setup, with the stock callback handler. */
  private static Map<String, Object> mskIamEnvironment() {
    return Map.of(
        "spring.kafka.properties.security.protocol", "SASL_SSL",
        "spring.kafka.properties.sasl.mechanism", "AWS_MSK_IAM",
        "spring.kafka.properties.sasl.jaas.config",
            "software.amazon.msk.auth.iam.IAMLoginModule required;",
        "spring.kafka.properties.sasl.client.callback.handler.class",
            "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
  }

  private static ConfigurationProvider configurationProvider() {
    KafkaConfiguration kafka = new KafkaConfiguration();
    kafka.setBootstrapServers("b-1.msk.example.amazonaws.com:9098");
    kafka.setProducer(new ProducerConfiguration());
    ConfigurationProvider provider = new ConfigurationProvider();
    provider.setKafka(kafka);
    return provider;
  }
}
