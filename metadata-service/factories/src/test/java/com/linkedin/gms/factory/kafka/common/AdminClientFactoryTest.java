package com.linkedin.gms.factory.kafka.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import com.linkedin.gms.factory.ProductionConfigContextRunner;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.kafka.DataHubMskIamClientCallbackHandler;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.mockito.MockedStatic;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.ApplicationContext;
import org.testng.annotations.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

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

  @Test
  public void testSplitProducerConfigUsesProducerBootstrapAndSecurityProtocol() {
    ProductionConfigContextRunner.create()
        .withPropertyValues(
            "kafka.bootstrapServers=base:9092",
            "kafka.producer.bootstrapServers=producer:9093",
            "kafka.producer.securityProtocol=SSL",
            "spring.kafka.security.protocol=PLAINTEXT")
        .run(
            context ->
                assertThat(adminProperties(context, "client-e"))
                    .containsEntry(
                        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, List.of("producer:9093"))
                    .containsEntry(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SSL"));
  }

  @Test
  public void testProducerSecurityProtocolOverridesSpringAdminProtocol() {
    ProductionConfigContextRunner.create()
        .withPropertyValues(
            "kafka.producer.securityProtocol=SASL_SSL",
            "spring.kafka.admin.security.protocol=SASL_PLAINTEXT")
        .run(
            context ->
                assertThat(adminProperties(context, "client-f"))
                    .containsEntry(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SASL_SSL"));
  }

  @Test
  public void testWithoutProducerSecurityProtocolKeepsTheBaseProtocol() {
    ProductionConfigContextRunner.create()
        .withPropertyValues(
            "kafka.producer.bootstrapServers=producer:9093",
            "spring.kafka.security.protocol=SASL_SSL")
        .run(
            context ->
                assertThat(adminProperties(context, "client-g"))
                    .containsEntry(
                        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, List.of("producer:9093"))
                    .containsEntry(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SASL_SSL"));
  }

  @Test
  public void testMskIamUsesTheDataHubCallbackHandlerAndDisablesDebugCreds() {
    StaticCredentialsProvider credentials =
        StaticCredentialsProvider.create(AwsBasicCredentials.create("id", "secret"));
    DataHubMskIamClientCallbackHandler.installSharedCredentials(credentials);
    try {
      // The documented MSK IAM setup, with the stock callback handler.
      ProductionConfigContextRunner.create()
          .withPropertyValues(
              "spring.kafka.properties.security.protocol=SASL_SSL",
              "spring.kafka.properties.sasl.mechanism=AWS_MSK_IAM",
              "spring.kafka.properties.sasl.jaas.config="
                  + "software.amazon.msk.auth.iam.IAMLoginModule required awsDebugCreds=true;",
              "spring.kafka.properties.sasl.client.callback.handler.class="
                  + "software.amazon.msk.auth.iam.IAMClientCallbackHandler")
          .run(
              context ->
                  assertThat(adminProperties(context, "client-h"))
                      .containsEntry(
                          "sasl.client.callback.handler.class",
                          DataHubMskIamClientCallbackHandler.class.getName())
                      .containsEntry(
                          "sasl.jaas.config",
                          "software.amazon.msk.auth.iam.IAMLoginModule required"
                              + " awsDebugCreds=false;"));
    } finally {
      DataHubMskIamClientCallbackHandler.resetIfInstalled(credentials);
    }
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
