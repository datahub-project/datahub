package com.linkedin.datahub.upgrade.boot;

import com.linkedin.datahub.upgrade.UpgradeConfigurationSelector;
import com.linkedin.datahub.upgrade.system.BlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.SystemUpdate;
import com.linkedin.datahub.upgrade.system.SystemUpdateBlocking;
import com.linkedin.datahub.upgrade.system.SystemUpdateNonBlocking;
import com.linkedin.datahub.upgrade.system.elasticsearch.BuildIndices;
import com.linkedin.datahub.upgrade.system.kafka.KafkaSetup;
import com.linkedin.gms.factory.search.SearchClusterRegistry;
import com.linkedin.gms.factory.telemetry.DailyReport;
import com.linkedin.gms.factory.telemetry.ScheduledAnalyticsFactory;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.entity.EntityServiceImpl;
import io.datahubproject.test.boot.AspectMigrationMutators;
import io.datahubproject.test.boot.BootContract;
import io.datahubproject.test.boot.MessagingContract;
import io.datahubproject.test.boot.ProcessBoot;
import io.datahubproject.test.boot.ProcessBoot.Messaging;
import java.util.Set;
import org.springframework.boot.WebApplicationType;
import org.springframework.context.ConfigurableApplicationContext;
import org.testng.annotations.Test;

/**
 * Boots {@code datahub-upgrade -u SystemUpdate} as {@code UpgradeCliApplication.main} does ({@code
 * SPRING_PROFILES_ACTIVE=upgrade}), minus the {@code UpgradeCli} runner itself so no upgrade
 * executes, under each messaging transport.
 */
public class SystemUpdateBootTest {

  /** See {@link MessagingContract}: existing Kafka client factories in a Kafka-disabled upgrade. */
  private static final Set<String> KAFKA_DISABLED_BASELINE =
      Set.of(
          "kafkaAdmin",
          "kafkaConsumerFactory",
          "kafkaListenerContainerFactory",
          "kafkaProducerFactory",
          "kafkaTemplate");

  @Test
  public void bootsWithKafka() {
    boot(Messaging.KAFKA);
  }

  @Test
  public void bootsWithPgQueue() {
    boot(Messaging.PGQUEUE);
  }

  /**
   * A second search cluster with the graph routed to it: the registry must hold both clusters and
   * route the graph to the secondary one, each with its own client.
   */
  @Test
  public void bootsWithSecondarySearchCluster() {
    try (ConfigurableApplicationContext ctx =
        process(Messaging.KAFKA)
            .property("elasticsearch.clusters.secondary.uri", "http://127.0.0.1:1")
            .property("elasticsearch.componentCluster.graph", "secondary")
            .start()) {
      contract(Messaging.KAFKA)
          .check(
              (c, v) -> {
                SearchClusterRegistry registry = c.getBean(SearchClusterRegistry.class);
                if (!registry.clusterNames().equals(Set.of("primary", "secondary"))) {
                  v.add(
                      "search clusters "
                          + registry.clusterNames()
                          + ", expected primary+secondary");
                } else if (!"secondary".equals(registry.clusterNameFor(SearchComponent.GRAPH))) {
                  v.add("graph routed to " + registry.clusterNameFor(SearchComponent.GRAPH));
                } else if (registry.clientFor(SearchComponent.GRAPH)
                    == registry.clientFor(SearchComponent.SEARCH_V2)) {
                  v.add("graph and search share one client despite different clusters");
                }
              })
          .verify(ctx);
    }
  }

  private static void boot(Messaging messaging) {
    try (ConfigurableApplicationContext ctx = process(messaging).start()) {
      contract(messaging).verify(ctx);
    }
  }

  private static ProcessBoot process(Messaging messaging) {
    return ProcessBoot.of(UpgradeConfigurationSelector.class)
        .profiles("upgrade")
        .web(WebApplicationType.NONE)
        .args("-u", "SystemUpdate")
        .messaging(messaging)
        // Chain membership is only populated with the flag on; check the full set.
        .property("featureFlags.aspectMigrationMutatorEnabled", "true");
  }

  private static BootContract contract(Messaging messaging) {
    BootContract contract =
        BootContract.forProcess("system-update-" + messaging.name().toLowerCase())
            .requires(SystemUpdate.class, SystemUpdateBlocking.class, SystemUpdateNonBlocking.class)
            .requires(
                BuildIndices.class, BlockingSystemUpgrade.class, NonBlockingSystemUpgrade.class)
            .requiresBean("entityService", EntityServiceImpl.class)
            .requiresBeans("ebeanServer", "searchClusterRegistry", "systemOperationContext")
            // Every aspect migration mutator must run during the upgrade's aspect migrations.
            .check(AspectMigrationMutators.allRegistered(true))
            // Excluded by GeneralUpgradeConfiguration: serving-only and scheduled components.
            .forbids(ScheduledAnalyticsFactory.class, DailyReport.class)
            .forbidsBeans(
                "graphQLEngine",
                "authorizerChain",
                "dataHubAuthorizer",
                "rateLimitEngine",
                "kafkaConsumerPool",
                "traceService",
                "entityCountMetricsPublisher");
    if (messaging == Messaging.KAFKA) {
      contract.requires(KafkaSetup.class);
    } else {
      contract.forbids(KafkaSetup.class);
    }
    return MessagingContract.apply(contract, messaging, KAFKA_DISABLED_BASELINE);
  }
}
