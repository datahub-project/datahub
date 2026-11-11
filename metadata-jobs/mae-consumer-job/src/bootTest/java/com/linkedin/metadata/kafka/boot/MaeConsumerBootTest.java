package com.linkedin.metadata.kafka.boot;

import com.datahub.event.PlatformEventProcessor;
import com.datahub.event.hook.BusinessAttributeUpdateHook;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.telemetry.DailyReport;
import com.linkedin.gms.factory.telemetry.ScheduledAnalyticsFactory;
import com.linkedin.metadata.kafka.MaeConsumerApplication;
import com.linkedin.metadata.kafka.MclConsumerConfig;
import com.linkedin.metadata.kafka.hook.UpdateIndicesHook;
import com.linkedin.metadata.kafka.hook.event.PlatformEventGeneratorHook;
import com.linkedin.metadata.kafka.hook.form.FormAssignmentHook;
import com.linkedin.metadata.kafka.hook.incident.IncidentsSummaryHook;
import com.linkedin.metadata.kafka.hook.ingestion.IngestionSchedulerHook;
import com.linkedin.metadata.kafka.hook.siblings.SiblingAssociationHook;
import io.datahubproject.test.boot.BootContract;
import io.datahubproject.test.boot.MessagingContract;
import io.datahubproject.test.boot.ProcessBoot;
import io.datahubproject.test.boot.ProcessBoot.Messaging;
import java.util.Set;
import org.springframework.boot.WebApplicationType;
import org.springframework.context.ConfigurableApplicationContext;
import org.testng.annotations.Test;

/**
 * Boots the standalone MAE consumer the way its image does ({@code SPRING_PROFILES_ACTIVE=mae},
 * {@code MAE_CONSUMER_ENABLED=true}), with the PE consumer on, under each messaging transport.
 */
public class MaeConsumerBootTest {

  /** See {@link MessagingContract}: existing Kafka client factories in a Kafka-disabled MAE. */
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
        ProcessBoot.of(MaeConsumerApplication.class, MclConsumerConfig.class)
            .profiles("mae")
            .web(WebApplicationType.SERVLET)
            .messaging(messaging)
            .property("MAE_CONSUMER_ENABLED", "true")
            .property("PE_CONSUMER_ENABLED", "true")
            .start()) {
      contract(messaging).verify(ctx);
    }
  }

  private static BootContract contract(Messaging messaging) {
    BootContract contract =
        BootContract.forProcess("mae-consumer-" + messaging.name().toLowerCase())
            .requires(
                UpdateIndicesHook.class,
                IngestionSchedulerHook.class,
                PlatformEventGeneratorHook.class,
                SiblingAssociationHook.class,
                FormAssignmentHook.class,
                IncidentsSummaryHook.class,
                BusinessAttributeUpdateHook.class,
                PlatformEventProcessor.class)
            .requiresBeans(
                "searchClusterRegistry",
                "entitySearchService",
                "graphService",
                "timeseriesAspectService",
                "systemMetadataService",
                "dataHubUsageEventsProcessor")
            // MAE writes back through GMS (rest.li), never to the entity store directly.
            .requiresBean("systemEntityClient", SystemEntityClient.class)
            .forbidsBeans("entityService", "ebeanServer")
            // GMS-only: telemetry scheduler, query-serving components.
            .forbids(ScheduledAnalyticsFactory.class, DailyReport.class)
            .forbidsBeans(
                "graphQLEngine",
                "authorizerChain",
                "dataHubAuthorizer",
                "rateLimitEngine",
                "siblingGraphService");
    if (messaging == Messaging.KAFKA) {
      contract.requiresBeans("MCLKafkaListenerRegistrar");
    } else {
      contract.requiresBeans(
          "pgQueueMclHookSources",
          "pgQueuePeSource",
          "pgQueueUsageSource",
          "pgQueuePollWorkersBootstrap");
    }
    return MessagingContract.apply(contract, messaging, KAFKA_DISABLED_BASELINE);
  }
}
