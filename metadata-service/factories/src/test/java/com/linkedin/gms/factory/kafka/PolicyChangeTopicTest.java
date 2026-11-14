package com.linkedin.gms.factory.kafka;

import static org.testng.Assert.assertEquals;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.kafka.TopicsConfiguration;
import java.util.Map;
import java.util.Optional;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.ClassPathResource;
import org.testng.annotations.Test;

public class PolicyChangeTopicTest {

  private static ConfigurationProvider config(Boolean enabled, String name) {
    TopicsConfiguration.TopicConfiguration topic = new TopicsConfiguration.TopicConfiguration();
    topic.setEnabled(enabled);
    topic.setName(name);
    KafkaConfiguration kafka = new KafkaConfiguration();
    kafka.setTopics(Map.of(PolicyChangeTopic.CONFIG_KEY, topic));
    ConfigurationProvider config = new ConfigurationProvider();
    config.setKafka(kafka);
    return config;
  }

  @Test
  public void testEnabledTopicName() {
    assertEquals(
        PolicyChangeTopic.enabledTopicName(config(true, "PolicyChange_v1")),
        Optional.of("PolicyChange_v1"));
    assertEquals(
        PolicyChangeTopic.enabledTopicName(config(false, "PolicyChange_v1")), Optional.empty());
    assertEquals(PolicyChangeTopic.enabledTopicName(config(true, " ")), Optional.empty());
    assertEquals(PolicyChangeTopic.enabledTopicName(new ConfigurationProvider()), Optional.empty());
  }

  /** Setting only the authorizer property, not the env var, must also enable the topic. */
  @Test
  public void testTopicFollowsTheAuthorizerProperty() throws Exception {
    StandardEnvironment env = new StandardEnvironment();
    env.getPropertySources().remove(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
    new YamlPropertySourceLoader()
        .load("application", new ClassPathResource("application.yaml"))
        .forEach(env.getPropertySources()::addLast);
    assertEquals(env.getProperty("kafka.topics.policyChange.enabled"), "false");

    env.getPropertySources()
        .addFirst(
            new MapPropertySource(
                "test", Map.of("authorization.defaultAuthorizer.changeBroadcast.enabled", "true")));
    assertEquals(env.getProperty("kafka.topics.policyChange.enabled"), "true");
  }
}
