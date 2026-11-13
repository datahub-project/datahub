package com.linkedin.gms.factory.event;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.ProductionConfigContextRunner;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.queue.QueueTopicDefaults;
import com.linkedin.metadata.queue.inmemory.InMemoryMetadataQueueStore;
import com.linkedin.metadata.queue.postgres.EbeanPostgresMetadataQueueStore;
import io.datahubproject.test.wiring.WiringCoverage;
import io.ebean.Database;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class MetadataQueueStoreFactoryTest {

  private ApplicationContextRunner runner() {
    return ProductionConfigContextRunner.create()
        .withInitializer(WiringCoverage.initializer())
        .withUserConfiguration(
            MetadataQueueStoreFactory.class, InMemoryMetadataQueueStoreFactory.class)
        .withPropertyValues("postgres.pgQueue.enabled=true");
  }

  @DataProvider
  public Object[][] postgresSettings() {
    return new Object[][] {{new String[] {}}, {new String[] {"postgres.pgQueue.store=postgres"}}};
  }

  @Test(dataProvider = "postgresSettings")
  public void selectsPostgresByDefaultAndWhenExplicit(String[] settings) {
    runner()
        .withPropertyValues("datahub.messaging.transport=pgqueue")
        .withPropertyValues(settings)
        // The store factory runs for real; its database dependency needs no live connection.
        .withBean("pgQueueEbeanServer", Database.class, () -> mock(Database.class))
        .withBean(ObjectMapper.class, ObjectMapper::new)
        .run(
            context -> {
              assertThat(context).hasSingleBean(MetadataQueueStore.class);
              assertThat(context.getBean(MetadataQueueStore.class))
                  .isInstanceOf(EbeanPostgresMetadataQueueStore.class);
              assertThat(context).doesNotHaveBean(InMemoryMetadataQueueStoreFactory.class);
            });
  }

  @Test
  public void selectsInMemoryWithoutCreatingPostgresPool() {
    runner()
        .withUserConfiguration(PgQueueEbeanConfigFactory.class)
        .withPropertyValues("DATAHUB_MESSAGING_TRANSPORT=pgqueue", "DATAHUB_PGQUEUE_STORE=inmemory")
        .run(
            context -> {
              assertThat(context).hasSingleBean(MetadataQueueStore.class);
              assertThat(context.getBean(MetadataQueueStore.class))
                  .isInstanceOf(InMemoryMetadataQueueStore.class);
              assertThat(context).doesNotHaveBean(MetadataQueueStoreFactory.class);
              assertThat(context).doesNotHaveBean(PgQueueEbeanConfigFactory.class);
              assertThat(context).doesNotHaveBean("pgQueueDataSourceConfig");
              assertThat(context).doesNotHaveBean("pgQueueEbeanServer");
            });
  }

  @DataProvider
  public Object[][] kafkaSettings() {
    return new Object[][] {
      {new String[] {}},
      {new String[] {"postgres.pgQueue.store=postgres"}},
      {new String[] {"postgres.pgQueue.store=inmemory"}}
    };
  }

  @Test(dataProvider = "kafkaSettings")
  public void defaultKafkaTransportCreatesNeitherStoreNorPool(String[] settings) {
    runner()
        .withUserConfiguration(PgQueueEbeanConfigFactory.class)
        .withPropertyValues(settings)
        .run(
            context -> {
              assertThat(context).doesNotHaveBean(MetadataQueueStore.class);
              assertThat(context).doesNotHaveBean(PgQueueEbeanConfigFactory.class);
            });
  }

  @Test
  public void spoolIsSavedOnContextCloseAndLoadedByNextContext() throws IOException {
    Path directory = Files.createTempDirectory("queue-store-test");
    Path spool = directory.resolve("queue.json");
    ApplicationContextRunner spooledRunner =
        runner()
            .withUserConfiguration(PgQueueEbeanConfigFactory.class)
            .withPropertyValues(
                "DATAHUB_MESSAGING_TRANSPORT=pgqueue",
                "DATAHUB_PGQUEUE_STORE=inmemory",
                "DATAHUB_PGQUEUE_INMEMORY_SPOOL_FILE=" + spool);
    try {
      spooledRunner.run(
          context ->
              context
                  .getBean(MetadataQueueStore.class)
                  .ensureTopic("test-topic", new QueueTopicDefaults(1, 3600, 0, 0, false, null)));
      assertThat(spool).exists();
      spooledRunner.run(
          context ->
              assertThat(context.getBean(MetadataQueueStore.class).fetchTopic("test-topic"))
                  .isPresent());
    } finally {
      Files.deleteIfExists(spool);
      Files.deleteIfExists(directory);
    }
  }
}
