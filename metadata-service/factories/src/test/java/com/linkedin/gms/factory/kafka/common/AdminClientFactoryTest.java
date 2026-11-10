package com.linkedin.gms.factory.kafka.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import com.linkedin.gms.factory.ProductionConfigContextRunner;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.mockito.MockedStatic;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.ApplicationContext;
import org.testng.annotations.Test;

/**
 * Builds the admin client from the {@link ConfigurationProvider} and {@link KafkaProperties} that
 * the production {@code application.yaml} binds, and checks which bootstrap servers it is given.
 * Only the Kafka client constructor is mocked, to capture its properties.
 */
public class AdminClientFactoryTest {

  @Test
  public void testProductionDefaultsUseTheSharedBootstrapServers() {
    ProductionConfigContextRunner.create()
        .run(
            context ->
                assertThat(adminProperties(context, "client-a"))
                    .containsEntry(
                        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                        List.of("http://localhost:9092"))
                    .containsEntry(AdminClientConfig.CLIENT_ID_CONFIG, "client-a"));
  }

  @Test
  public void testProducerBootstrapOverridesTheSharedOne() {
    ProductionConfigContextRunner.create()
        .withPropertyValues(
            "kafka.bootstrapServers=shared:9092",
            "kafka.producer.bootstrapServers=producer-a:9092,producer-b:9092")
        .run(
            context ->
                assertThat(adminProperties(context, "client-b"))
                    .containsEntry(
                        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                        List.of("producer-a:9092", "producer-b:9092")));
  }

  @Test
  public void testDataHubBootstrapTakesPrecedenceOverSpringKafka() {
    ProductionConfigContextRunner.create()
        .withPropertyValues(
            "kafka.bootstrapServers=datahub:9092", "spring.kafka.bootstrap-servers=spring:9092")
        .run(
            context ->
                assertThat(adminProperties(context, "client-c"))
                    .containsEntry(
                        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, List.of("datahub:9092")));
  }

  @Test
  public void testBlankDataHubBootstrapFallsBackToSpringKafka() {
    ProductionConfigContextRunner.create()
        .withPropertyValues("kafka.bootstrapServers=", "spring.kafka.bootstrap-servers=spring:9092")
        .run(
            context ->
                assertThat(adminProperties(context, "client-d"))
                    .containsEntry(
                        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, List.of("spring:9092")));
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> adminProperties(ApplicationContext context, String clientId) {
    AtomicReference<Map<String, Object>> captured = new AtomicReference<>();
    try (MockedStatic<AdminClient> adminClient = mockStatic(AdminClient.class)) {
      adminClient
          .when(() -> AdminClient.create(anyMap()))
          .thenAnswer(
              invocation -> {
                captured.set(invocation.getArgument(0));
                return mock(AdminClient.class);
              });
      AdminClientFactory.buildKafkaAdminClient(
          context.getBean(ConfigurationProvider.class).getKafka(),
          context.getBean(KafkaProperties.class),
          clientId);
    }
    assertThat(captured.get()).as("admin client was created").isNotNull();
    return captured.get();
  }
}
