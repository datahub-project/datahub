package com.linkedin.gms.boot;

import com.datahub.auth.authentication.AuthServiceController;
import com.datahub.authorization.AuthorizerChain;
import com.datahub.graphql.GraphQLController;
import com.linkedin.gms.GMSApplication;
import com.linkedin.gms.factory.telemetry.DailyReport;
import com.linkedin.gms.servlet.AuthServletConfig;
import com.linkedin.gms.servlet.GraphQLServletConfig;
import com.linkedin.gms.servlet.IcebergCatalogServletConfig;
import com.linkedin.gms.servlet.OpenAPIServletConfig;
import com.linkedin.gms.servlet.RestliServletConfig;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.search.SearchService;
import io.datahubproject.iceberg.catalog.rest.open.PublicIcebergApiController;
import io.datahubproject.iceberg.catalog.rest.secure.IcebergApiController;
import io.datahubproject.openapi.operations.elastic.ElasticsearchController;
import io.datahubproject.test.boot.AspectMigrationMutators;
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
 * Boots {@link GMSApplication} as the quickstart image runs it (MCE/MAE/PE consumers embedded) on a
 * real Jetty, under each messaging transport.
 */
public class GmsBootTest {

  /** See {@link MessagingContract}: existing Kafka client factories in a Kafka-disabled GMS. */
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
        ProcessBoot.of(GMSApplication.class)
            .web(WebApplicationType.SERVLET)
            .messaging(messaging)
            .property("MAE_CONSUMER_ENABLED", "true")
            .property("MCE_CONSUMER_ENABLED", "true")
            .property("PE_CONSUMER_ENABLED", "true")
            .start()) {
      contract(messaging).verify(ctx);
    }
  }

  private static BootContract contract(Messaging messaging) {
    BootContract contract =
        BootContract.forProcess("gms-" + messaging.name().toLowerCase())
            // Servlet configs imported by GMSApplication via ServletConfig's component scan.
            .requires(
                GraphQLServletConfig.class,
                OpenAPIServletConfig.class,
                RestliServletConfig.class,
                AuthServletConfig.class,
                IcebergCatalogServletConfig.class)
            // GraphQL
            .requires(GraphQLController.class)
            .requiresTypeNamed("com.linkedin.datahub.graphql.GraphQLEngine")
            // OpenAPI (v1/v2/v3 + operations)
            .requires(
                io.datahubproject.openapi.v3.controller.EntityController.class,
                io.datahubproject.openapi.v2.controller.EntityController.class,
                ElasticsearchController.class)
            // Rest.li (the servlet itself is not a bean; the HTTP probe below covers it)
            .requiresBeans("restliServletRegistration", "rapServlet")
            // Auth
            .requires(AuthServiceController.class, AuthorizerChain.class)
            .requiresBeans("authExtractionFilter", "authFilter", "dataHubAuthorizer")
            // Iceberg REST catalog
            .requires(IcebergApiController.class, PublicIcebergApiController.class)
            // Storage and search
            .requiresBean("entityService", EntityServiceImpl.class)
            .requires(SearchService.class)
            .requiresBeans(
                "ebeanServer", "searchClusterRegistry", "graphService", "timeseriesAspectService")
            // Embedded consumers (quickstart shape)
            .requiresTypeNamed(
                "com.linkedin.metadata.kafka.hook.UpdateIndicesHook",
                "com.linkedin.metadata.kafka.batch.BatchMetadataChangeProposalsProcessor",
                "com.datahub.event.PlatformEventProcessor")
            // GMS-only schedulers
            .requires(DailyReport.class)
            .requiresBeans("ingestionScheduler")
            .check(AspectMigrationMutators.allRegistered(false))
            .check(
                HttpRoutes.mounted(
                    "",
                    "GET /health",
                    "GET /config",
                    "POST /api/graphql",
                    "GET /openapi/v3/entity/dataset",
                    "GET /aspects?action=getCount",
                    "GET /iceberg/v1/config",
                    "POST /auth/generateSessionTokenForUser"))
            .check(HttpRoutes.answers("GET /health", 200));
    if (messaging == Messaging.KAFKA) {
      contract.requiresBeans("kafkaController", "MCLKafkaListenerRegistrar");
    } else {
      contract.requiresBeans(
          "pgQueueMessagingOperationsController",
          "pgQueueMclHookSources",
          "pgQueueBatchMcpSource",
          "pgQueuePeSource");
    }
    return MessagingContract.apply(contract, messaging, KAFKA_DISABLED_BASELINE);
  }
}
