package com.linkedin.metadata.kafka.boot;

import com.linkedin.gms.factory.telemetry.DailyReport;
import com.linkedin.gms.factory.telemetry.ScheduledAnalyticsFactory;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.kafka.MceConsumerApplication;
import com.linkedin.restli.server.RestliHandlerServlet;
import io.datahubproject.test.boot.BootContract;
import io.datahubproject.test.boot.HttpRoutes;
import io.datahubproject.test.boot.MessagingContract;
import io.datahubproject.test.boot.ProcessBoot;
import io.datahubproject.test.boot.ProcessBoot.Messaging;
import java.util.Set;
import org.springframework.boot.WebApplicationType;
import org.springframework.context.ConfigurableApplicationContext;
import org.testng.annotations.Test;

/**
 * Boots the standalone MCE consumer the way its image does ({@code SPRING_PROFILES_ACTIVE=mce},
 * {@code MCE_CONSUMER_ENABLED=true}) under each messaging transport.
 */
public class MceConsumerBootTest {

  /** See {@link MessagingContract}: existing Kafka client factories in a Kafka-disabled MCE. */
  private static final Set<String> KAFKA_DISABLED_BASELINE =
      Set.of(
          "kafkaAdmin",
          "kafkaConsumerFactory",
          "kafkaConsumerPoolConsumerFactory",
          "kafkaEventConsumer",
          "kafkaListenerContainerFactory",
          "kafkaProducerFactory",
          "kafkaTemplate",
          "mclBatchEventConsumer",
          "mclEventConsumer",
          "mcpBatchEventConsumer",
          "mcpEventConsumer",
          "platformEventConsumer",
          "simpleKafkaConsumer");

  @Test
  public void bootsWithKafka() {
    boot(Messaging.KAFKA);
  }

  @Test
  public void bootsWithPgQueue() {
    boot(Messaging.PGQUEUE);
  }

  private static void boot(Messaging messaging) {
    try (ConfigurableApplicationContext ctx =
        ProcessBoot.of(MceConsumerApplication.class)
            .profiles("mce")
            .web(WebApplicationType.SERVLET)
            .messaging(messaging)
            .property("MCE_CONSUMER_ENABLED", "true")
            .start()) {
      contract(messaging).verify(ctx);
    }
  }

  private static BootContract contract(Messaging messaging) {
    BootContract contract =
        BootContract.forProcess("mce-consumer-" + messaging.name().toLowerCase())
            .requiresBean("entityService", EntityServiceImpl.class)
            .requiresBeans(
                "ebeanServer",
                "searchClusterRegistry",
                "authorizerChain",
                "batchMetadataChangeProposalsProcessor",
                "metadataChangeEventsProcessor")
            // The MCE consumer serves the rest.li ingest endpoints.
            .requires(RestliHandlerServlet.class)
            .check(HttpRoutes.mounted("/gms", "GET /gms/aspects?action=getCount"))
            // Not asserted yet: AspectMigrationMutators.allRegistered(...). MceConsumerApplication
            // does not component-scan com.linkedin.metadata.aspect.hooks.migrations, so its
            // AspectMigrationMutatorChain is empty while GMS and system-update have all mutators.
            // Add the check once the consumer is meant to migrate aspects on ingest.
            // GMS-only: telemetry scheduler, GraphQL, ingestion scheduler.
            .forbids(ScheduledAnalyticsFactory.class, DailyReport.class)
            .forbidsBeans("graphQLEngine", "rateLimitEngine", "ingestionScheduler");
    if (messaging == Messaging.KAFKA) {
      contract.requiresBeans("MCPBatchKafkaListenerRegistrar");
    } else {
      contract.requiresBeans("pgQueueBatchMcpSource", "pgQueuePollWorkersBootstrap");
    }
    return MessagingContract.apply(contract, messaging, KAFKA_DISABLED_BASELINE);
  }
}
