package com.linkedin.gms.factory.kafka;

import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.withSettings;
import static org.testng.Assert.assertEquals;

import com.datahub.metadata.dao.throttle.KafkaThrottleSensor;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.kafka.throttle.KafkaThrottleFactory;
import com.linkedin.gms.factory.kafka.trace.KafkaTraceReaderFactory;
import com.linkedin.metadata.config.MetadataChangeProposalConfig;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.kafka.ProducerConfiguration;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.MapPropertySource;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * The admin-client factories must install the shared AWS credentials before building an admin
 * client under MSK IAM; otherwise {@link KafkaMskIamAuth#configure} fails the bean at startup. Each
 * case starts a context holding only the factory under test, so nothing else (such as the producer
 * factory) installs the credentials first.
 */
public class AdminClientMskIamWiringTest {

  @AfterMethod
  public void resetSharedCredentials() {
    DataHubMskIamClientCallbackHandler.reset();
  }

  @DataProvider(name = "adminClientBeans")
  public Object[][] adminClientBeans() {
    return new Object[][] {
      {KafkaTraceReaderFactory.class, "traceAdminClient"},
      {KafkaThrottleFactory.class, "kafkaThrottle"}
    };
  }

  @Test(dataProvider = "adminClientBeans")
  public void adminClientGetsDataHubMskIamHandler(Class<?> factory, String beanName) {
    List<Map<String, Object>> adminConfigs = new ArrayList<>();
    try (var adminStatic = mockStatic(AdminClient.class);
        var ignored =
            mockConstruction(
                KafkaThrottleSensor.class, withSettings().defaultAnswer(RETURNS_SELF));
        AnnotationConfigApplicationContext ctx = newContext(factory)) {
      adminStatic
          .when(() -> AdminClient.create(anyMap()))
          .thenAnswer(
              invocation -> {
                adminConfigs.add(new HashMap<>(invocation.<Map<String, Object>>getArgument(0)));
                return mock(AdminClient.class);
              });

      ctx.getBean(beanName);

      assertEquals(adminConfigs.size(), 1, "expected exactly one AdminClient creation");
      assertEquals(
          adminConfigs.get(0).get("sasl.client.callback.handler.class"),
          DataHubMskIamClientCallbackHandler.class.getName());
    }
  }

  private static AnnotationConfigApplicationContext newContext(Class<?> factory) {
    AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
    ctx.getEnvironment()
        .getPropertySources()
        .addFirst(new MapPropertySource("test", mskIamEnvironment()));
    // Lazy, so only the requested bean and its dependencies are built.
    ctx.addBeanFactoryPostProcessor(
        beanFactory -> {
          for (String name : beanFactory.getBeanDefinitionNames()) {
            beanFactory.getBeanDefinition(name).setLazyInit(true);
          }
        });
    ctx.registerBean(
        "configurationProvider",
        ConfigurationProvider.class,
        AdminClientMskIamWiringTest::configurationProvider);
    ctx.registerBean(EntityRegistry.class, () -> mock(EntityRegistry.class));
    ctx.register(factory);
    ctx.refresh();
    return ctx;
  }

  /** The documented MSK IAM setup, with the stock callback handler. */
  private static Map<String, Object> mskIamEnvironment() {
    return Map.of(
        "datahub.messaging.transport", "kafka",
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
    MetadataChangeProposalConfig.ThrottlesConfig throttle =
        new MetadataChangeProposalConfig.ThrottlesConfig();
    throttle.setUpdateIntervalMs(60000);
    MetadataChangeProposalConfig mcp = new MetadataChangeProposalConfig();
    mcp.setThrottle(throttle);
    ConfigurationProvider provider = new ConfigurationProvider();
    provider.setKafka(kafka);
    provider.setMetadataChangeProposal(mcp);
    return provider;
  }
}
