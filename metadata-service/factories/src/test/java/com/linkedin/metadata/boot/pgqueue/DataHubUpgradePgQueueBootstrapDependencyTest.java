package com.linkedin.metadata.boot.pgqueue;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.SystemUpdateConfiguration;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.queue.QueueTopicMetadata;
import com.linkedin.metadata.version.GitVersion;
import com.linkedin.mxe.TopicConvention;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DataHubUpgradePgQueueBootstrapDependencyTest {

  private MetadataQueueStore metadataQueueStore;
  private TopicConvention topicConvention;
  private ConfigurationProvider configurationProvider;
  private DataHubUpgradePgQueueBootstrapDependency dependency;

  @BeforeMethod
  public void setUp() {
    metadataQueueStore = mock(MetadataQueueStore.class);
    topicConvention = mock(TopicConvention.class);
    configurationProvider = mock(ConfigurationProvider.class);
    GitVersion gitVersion = mock(GitVersion.class);
    when(gitVersion.getVersion()).thenReturn("1.0.0");
    when(topicConvention.getDataHubUpgradeHistoryTopicName())
        .thenReturn("DataHubUpgradeHistory_v1");

    SystemUpdateConfiguration systemUpdate = new SystemUpdateConfiguration();
    systemUpdate.setWaitForSystemUpdate(false);
    when(configurationProvider.getSystemUpdate()).thenReturn(systemUpdate);

    dependency =
        new DataHubUpgradePgQueueBootstrapDependency(
            metadataQueueStore,
            topicConvention,
            gitVersion,
            configurationProvider,
            mock(Deserializer.class),
            mock(OperationContext.class));
    ReflectionTestUtils.setField(dependency, "revision", "0");
  }

  @Test
  public void waitForBootstrap_skipsWhenSystemUpdateWaitDisabled() {
    assertTrue(dependency.waitForBootstrap());
  }

  @Test
  public void waitForBootstrap_succeedsWhenTopicMissingAndBackoffZero() {
    SystemUpdateConfiguration systemUpdate = new SystemUpdateConfiguration();
    systemUpdate.setWaitForSystemUpdate(true);
    systemUpdate.setMaxBackOffs("1");
    systemUpdate.setInitialBackOffMs("0");
    systemUpdate.setBackOffFactor("1");
    when(configurationProvider.getSystemUpdate()).thenReturn(systemUpdate);
    when(metadataQueueStore.fetchTopic("DataHubUpgradeHistory_v1")).thenReturn(Optional.empty());

    try {
      dependency.waitForBootstrap();
    } catch (IllegalStateException expected) {
      // exhausted backoff without matching upgrade history
    }
  }

  @Test
  public void waitForBootstrap_succeedsWhenPartitionLogEmpty() {
    SystemUpdateConfiguration systemUpdate = new SystemUpdateConfiguration();
    systemUpdate.setWaitForSystemUpdate(true);
    systemUpdate.setMaxBackOffs("1");
    systemUpdate.setInitialBackOffMs("0");
    systemUpdate.setBackOffFactor("1");
    when(configurationProvider.getSystemUpdate()).thenReturn(systemUpdate);
    when(metadataQueueStore.fetchTopic("DataHubUpgradeHistory_v1"))
        .thenReturn(Optional.of(new QueueTopicMetadata(1L, 1, Optional.empty())));
    when(metadataQueueStore.partitionNextExclusiveSeqs(1L, 1)).thenReturn(Map.of(0, 1L));

    try {
      dependency.waitForBootstrap();
    } catch (IllegalStateException expected) {
      // no rows in log yet
    }
  }
}
