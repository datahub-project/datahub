package com.linkedin.datahub.upgrade.cleanup;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.kafka.TopicsConfiguration;
import com.linkedin.upgrade.DataHubUpgradeState;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DeleteKafkaTopicsStepTest {

  private KafkaConfiguration kafkaConfiguration;
  private KafkaProperties kafkaProperties;

  @BeforeMethod
  public void setUp() {
    kafkaConfiguration = new KafkaConfiguration();
    kafkaProperties = new KafkaProperties();
  }

  @Test
  public void testId() {
    DeleteKafkaTopicsStep step = new DeleteKafkaTopicsStep(kafkaConfiguration, kafkaProperties);
    assertEquals(step.id(), "DeleteKafkaTopicsStep");
  }

  @Test
  public void testRetryCount() {
    DeleteKafkaTopicsStep step = new DeleteKafkaTopicsStep(kafkaConfiguration, kafkaProperties);
    assertEquals(step.retryCount(), 2);
  }

  @Test
  public void testSucceedsWhenNoTopicsConfigured() {
    // Topics config is null by default
    DeleteKafkaTopicsStep step = new DeleteKafkaTopicsStep(kafkaConfiguration, kafkaProperties);

    UpgradeContext mockContext = mock(UpgradeContext.class);
    UpgradeStepResult result = step.executable().apply(mockContext);

    assertNotNull(result);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testSucceedsWhenAllTopicsDisabled() {
    Map<String, TopicsConfiguration.TopicConfiguration> topics = new HashMap<>();
    TopicsConfiguration.TopicConfiguration topicConfig =
        new TopicsConfiguration.TopicConfiguration();
    topicConfig.setName("test-topic");
    topicConfig.setEnabled(false);
    topics.put("testTopic", topicConfig);
    kafkaConfiguration.setTopics(topics);

    DeleteKafkaTopicsStep step = new DeleteKafkaTopicsStep(kafkaConfiguration, kafkaProperties);

    UpgradeContext mockContext = mock(UpgradeContext.class);
    UpgradeStepResult result = step.executable().apply(mockContext);

    assertNotNull(result);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testSuccessfulTopicDeletion() throws Exception {
    Map<String, TopicsConfiguration.TopicConfiguration> topics = new HashMap<>();
    TopicsConfiguration.TopicConfiguration topicConfig =
        new TopicsConfiguration.TopicConfiguration();
    topicConfig.setName("test-topic");
    topicConfig.setEnabled(true);
    topics.put("testTopic", topicConfig);
    kafkaConfiguration.setTopics(topics);

    DeleteKafkaTopicsStep step =
        spy(new DeleteKafkaTopicsStep(kafkaConfiguration, kafkaProperties));

    AdminClient mockAdminClient = mock(AdminClient.class);
    DeleteTopicsResult mockDeleteResult = mock(DeleteTopicsResult.class);
    KafkaFuture<Void> mockFuture = mock(KafkaFuture.class);
    when(mockFuture.get()).thenReturn(null);
    when(mockDeleteResult.all()).thenReturn(mockFuture);
    when(mockAdminClient.deleteTopics(anyCollection())).thenReturn(mockDeleteResult);
    doReturn(mockAdminClient).when(step).createAdminClient();

    UpgradeContext mockContext = mock(UpgradeContext.class);
    UpgradeStepResult result = step.executable().apply(mockContext);

    assertNotNull(result);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockAdminClient).deleteTopics(anyCollection());
  }

  @Test
  public void testFailsWhenDeleteThrows() throws Exception {
    Map<String, TopicsConfiguration.TopicConfiguration> topics = new HashMap<>();
    TopicsConfiguration.TopicConfiguration topicConfig =
        new TopicsConfiguration.TopicConfiguration();
    topicConfig.setName("test-topic");
    topicConfig.setEnabled(true);
    topics.put("testTopic", topicConfig);
    kafkaConfiguration.setTopics(topics);

    DeleteKafkaTopicsStep step =
        spy(new DeleteKafkaTopicsStep(kafkaConfiguration, kafkaProperties));

    AdminClient mockAdminClient = mock(AdminClient.class);
    when(mockAdminClient.deleteTopics(anyCollection()))
        .thenThrow(new RuntimeException("Kafka unavailable"));
    doReturn(mockAdminClient).when(step).createAdminClient();

    UpgradeContext mockContext = mock(UpgradeContext.class);
    UpgradeStepResult result = step.executable().apply(mockContext);

    assertNotNull(result);
    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testOnlyDeletesEnabledTopics() throws Exception {
    Map<String, TopicsConfiguration.TopicConfiguration> topics = new HashMap<>();

    TopicsConfiguration.TopicConfiguration enabledTopic =
        new TopicsConfiguration.TopicConfiguration();
    enabledTopic.setName("enabled-topic");
    enabledTopic.setEnabled(true);
    topics.put("enabledTopic", enabledTopic);

    TopicsConfiguration.TopicConfiguration disabledTopic =
        new TopicsConfiguration.TopicConfiguration();
    disabledTopic.setName("disabled-topic");
    disabledTopic.setEnabled(false);
    topics.put("disabledTopic", disabledTopic);

    kafkaConfiguration.setTopics(topics);

    DeleteKafkaTopicsStep step =
        spy(new DeleteKafkaTopicsStep(kafkaConfiguration, kafkaProperties));

    AdminClient mockAdminClient = mock(AdminClient.class);
    DeleteTopicsResult mockDeleteResult = mock(DeleteTopicsResult.class);
    KafkaFuture<Void> mockFuture = mock(KafkaFuture.class);
    when(mockFuture.get()).thenReturn(null);
    when(mockDeleteResult.all()).thenReturn(mockFuture);
    when(mockAdminClient.deleteTopics(anyCollection())).thenReturn(mockDeleteResult);
    doReturn(mockAdminClient).when(step).createAdminClient();

    UpgradeContext mockContext = mock(UpgradeContext.class);
    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    // Verify only enabled topic was passed for deletion
    verify(mockAdminClient)
        .deleteTopics(
            argThat(
                (java.util.Collection<String> c) -> c.size() == 1 && c.contains("enabled-topic")));
  }
}
