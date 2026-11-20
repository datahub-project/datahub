package com.linkedin.gms.factory.entity;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.gms.factory.ProductionConfigContextRunner;
import com.linkedin.gms.factory.buffer.CoalesceBufferFactoryConfig;
import com.linkedin.metadata.buffer.CoalesceBuffer;
import com.linkedin.metadata.entity.RetentionService;
import com.linkedin.metadata.entity.retention.RetentionBatchEntry;
import com.linkedin.metadata.entity.retention.buffer.CoalesceRetentionBuffer;
import com.linkedin.metadata.entity.retention.buffer.RetentionBuffer;
import com.linkedin.metadata.entity.retention.buffer.RetentionDrainer;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Loads {@link RetentionBufferFactory} with the production {@code application.yaml}, the real
 * {@link CoalesceBufferFactoryConfig} and an in-memory Hazelcast member built from the factory's
 * own {@link MapConfig} beans. Checks that the buffer exists only when both {@code
 * featureFlags.retentionBufferEnabled} and {@code featureFlags.postCommitRetentionEnabled} are on,
 * that the map limits come from {@code datahub.retention.buffer}, and that an enqueued retention
 * reaches the retention service through the drainer. The retention service is the only mock.
 */
public class RetentionBufferFactoryTest {

  private static final String BUFFER_ON = "featureFlags.retentionBufferEnabled=true";
  private static final String POST_COMMIT_ON = "featureFlags.postCommitRetentionEnabled=true";
  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,my_db.my_table,PROD)");

  private static final TestHazelcastInstanceFactory HAZELCAST = new TestHazelcastInstanceFactory();

  @AfterMethod
  public void tearDown() {
    HAZELCAST.terminateAll();
  }

  @Test
  public void testProductionDefaultsCreateNoBuffer() {
    retentionRunner().run(RetentionBufferFactoryTest::assertNoBufferBeans);
  }

  @Test
  public void testBufferFlagWithoutPostCommitRetentionCreatesNoBuffer() {
    retentionRunner()
        .withPropertyValues(BUFFER_ON)
        .run(RetentionBufferFactoryTest::assertNoBufferBeans);
  }

  @Test
  public void testPostCommitRetentionWithoutBufferFlagCreatesNoBuffer() {
    retentionRunner()
        .withPropertyValues(POST_COMMIT_ON)
        .run(RetentionBufferFactoryTest::assertNoBufferBeans);
  }

  @Test
  public void testBothFlagsBoundTheMapsFromYaml() {
    retentionRunner()
        .withPropertyValues(BUFFER_ON, POST_COMMIT_ON)
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              MapConfig pending = context.getBean("retentionPendingMapConfig", MapConfig.class);
              assertThat(pending.getName()).isEqualTo("retention-pending");
              assertThat(pending.getEvictionConfig().getSize()).isEqualTo(100000);
              assertThat(context.getBean("retentionDrainLockMapConfig", MapConfig.class).getName())
                  .isEqualTo("retention-drain-lock");
              // Production's CacheConfig hands every MapConfig bean to the Hazelcast member.
              assertThat(
                      context
                          .getBean(HazelcastInstance.class)
                          .getConfig()
                          .getMapConfigOrNull("retention-pending"))
                  .isNotNull();
            });

    retentionRunner()
        .withPropertyValues(
            BUFFER_ON,
            POST_COMMIT_ON,
            "datahub.retention.buffer.mapName=custom-pending",
            "datahub.retention.buffer.lockMapName=custom-lock",
            "datahub.retention.buffer.maxPendingEntries=10")
        .run(
            context -> {
              MapConfig pending = context.getBean("retentionPendingMapConfig", MapConfig.class);
              assertThat(pending.getName()).isEqualTo("custom-pending");
              assertThat(pending.getEvictionConfig().getSize()).isEqualTo(10);
              assertThat(context.getBean("retentionDrainLockMapConfig", MapConfig.class).getName())
                  .isEqualTo("custom-lock");
            });
  }

  private static void assertNoBufferBeans(AssertableApplicationContext context) {
    assertThat(context).hasNotFailed();
    assertThat(context.getBeansOfType(RetentionBuffer.class)).isEmpty();
    assertThat(context.getBeansOfType(RetentionDrainer.class)).isEmpty();
    assertThat(context.getBeansOfType(MapConfig.class)).isEmpty();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testEnqueuedRetentionIsAppliedByTheDrainer() {
    RetentionService<?> retentionService = mock(RetentionService.class);
    when(retentionService.applyRetentionBatchWithPolicyDefaults(any(), anyList()))
        .thenAnswer(
            invocation ->
                invocation.<List<RetentionBatchEntry>>getArgument(1).stream()
                    .map(RetentionBatchEntry::key)
                    .toList());

    retentionRunner(retentionService)
        .withPropertyValues(BUFFER_ON, POST_COMMIT_ON)
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              RetentionBuffer buffer = context.getBean(RetentionBuffer.class);
              assertThat(buffer).isInstanceOf(CoalesceRetentionBuffer.class);
              assertThat(buffer.defersApply()).isTrue();

              buffer.enqueue(
                  TestOperationContexts.systemContextNoSearchAuthorization(),
                  DATASET_URN,
                  "datasetProperties",
                  3L);
              context.getBean(RetentionDrainer.class).tick();

              ArgumentCaptor<List<RetentionBatchEntry>> entries =
                  ArgumentCaptor.forClass(List.class);
              verify(retentionService)
                  .applyRetentionBatchWithPolicyDefaults(any(), entries.capture());
              assertThat(entries.getValue()).hasSize(1);
              RetentionBatchEntry entry = entries.getValue().get(0);
              assertThat(entry.context().getUrn()).isEqualTo(DATASET_URN);
              assertThat(entry.context().getAspectName()).isEqualTo("datasetProperties");
              assertThat(entry.context().getMaxVersion()).contains(3L);

              // Applied keys are cleared, so the next tick has nothing to drain.
              assertThat(context.getBean(CoalesceBuffer.class).drain(10)).isEmpty();
            });
  }

  private static ApplicationContextRunner retentionRunner() {
    return retentionRunner(mock(RetentionService.class));
  }

  private static ApplicationContextRunner retentionRunner(RetentionService<?> retentionService) {
    OperationContext systemOpContext = TestOperationContexts.systemContextNoSearchAuthorization();
    return ProductionConfigContextRunner.create()
        .withUserConfiguration(
            RetentionBufferFactory.class,
            RetentionContextResolverFactory.class,
            CoalesceBufferFactoryConfig.class,
            InMemoryHazelcast.class)
        .withBean("retentionService", RetentionService.class, () -> retentionService)
        .withBean("systemOperationContext", OperationContext.class, () -> systemOpContext);
  }

  /**
   * Production builds {@code hazelcastInstance} in {@code CacheConfig} from every {@link MapConfig}
   * bean; this does the same with an in-memory member.
   */
  @Configuration(proxyBeanMethods = false)
  static class InMemoryHazelcast {
    @Bean("hazelcastInstance")
    HazelcastInstance hazelcastInstance(ObjectProvider<MapConfig> mapConfigs) {
      Config config = new Config();
      mapConfigs.orderedStream().forEach(config::addMapConfig);
      return HAZELCAST.newHazelcastInstance(config);
    }
  }
}
