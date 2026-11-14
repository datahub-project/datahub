package com.linkedin.gms.factory.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datahub.authorization.DataHubAuthorizer;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.kafka.TopicsConfiguration;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.datahubproject.test.wiring.WiringCoverage;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.testng.annotations.Test;

/** Verifies when the policy change listener starts. */
public class PolicyChangeListenerFactoryConditionalTest {

  private static DataHubAuthorizer authorizer(boolean sqlSource) {
    DataHubAuthorizer authorizer = mock(DataHubAuthorizer.class);
    when(authorizer.isSqlSourceEnabled()).thenReturn(sqlSource);
    when(authorizer.getSystemOpContext())
        .thenReturn(TestOperationContexts.systemContextNoSearchAuthorization());
    return authorizer;
  }

  private static ConfigurationProvider configWithTopic() {
    TopicsConfiguration.TopicConfiguration topic = new TopicsConfiguration.TopicConfiguration();
    topic.setName("PolicyChange_v1");
    topic.setEnabled(true);
    KafkaConfiguration kafka = new KafkaConfiguration();
    kafka.setTopics(Map.of("policyChange", topic));
    // A mock: a real ConfigurationProvider is a @Configuration class and would bring its own beans.
    ConfigurationProvider config = mock(ConfigurationProvider.class);
    when(config.getKafka()).thenReturn(kafka);
    return config;
  }

  private static ApplicationContextRunner runner() {
    return new ApplicationContextRunner()
        .withInitializer(WiringCoverage.initializer())
        .withUserConfiguration(PolicyChangeListenerFactory.class)
        .withBean("configurationProvider", ConfigurationProvider.class, () -> configWithTopic());
  }

  private static ApplicationContextRunner withAuthorizer(boolean sqlSource) {
    return runner()
        .withBean("dataHubAuthorizer", DataHubAuthorizer.class, () -> authorizer(sqlSource));
  }

  @Test
  public void absentWhenTheBroadcastIsOff() {
    withAuthorizer(true)
        .run(context -> assertThat(context).doesNotHaveBean(PolicyChangeListenerFactory.class));
  }

  @Test
  public void absentWhenTheTransportIsNotKafka() {
    withAuthorizer(true)
        .withPropertyValues(
            "authorization.defaultAuthorizer.changeBroadcast.enabled=true",
            "datahub.messaging.transport=pgqueue")
        .run(context -> assertThat(context).doesNotHaveBean(PolicyChangeListenerFactory.class));
  }

  @Test
  public void notStartedWithoutAnAuthorizer() {
    runner()
        .withPropertyValues("authorization.defaultAuthorizer.changeBroadcast.enabled=true")
        .run(
            context ->
                assertThat(context.getBeanProvider(PolicyChangeListener.class).getIfAvailable())
                    .isNull());
  }

  @Test
  public void notStartedWithoutTheSqlSource() {
    withAuthorizer(false)
        .withPropertyValues("authorization.defaultAuthorizer.changeBroadcast.enabled=true")
        .run(
            context ->
                assertThat(context.getBeanProvider(PolicyChangeListener.class).getIfAvailable())
                    .isNull());
  }

  @Test
  public void startedWithTheBroadcastOnAKafkaTransport() {
    withAuthorizer(true)
        .withBean(
            "kafkaConsumerFactory",
            DefaultKafkaConsumerFactory.class,
            () ->
                new DefaultKafkaConsumerFactory<>(
                    Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1",
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)))
        .withPropertyValues("authorization.defaultAuthorizer.changeBroadcast.enabled=true")
        .run(
            context ->
                assertThat(context.getBeanProvider(PolicyChangeListener.class).getIfAvailable())
                    .isNotNull());
  }
}
