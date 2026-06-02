package com.linkedin.metadata.boot;

import com.linkedin.gms.factory.kafka.common.KafkaInitializationManager;
import com.linkedin.gms.factory.kafka.common.PgQueueConsumerInitializationManager;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.web.context.WebApplicationContext;

/** Responsible for coordinating starting steps that happen before the application starts up. */
@Slf4j
@Component
public class OnBootApplicationListener {

  @Autowired
  @Qualifier("bootstrapManager")
  private BootstrapManager _bootstrapManager;

  @Autowired
  @Qualifier("systemOperationContext")
  private OperationContext systemOperationContext;

  @Autowired private KafkaInitializationManager kafkaInitializationManager;

  @Autowired(required = false)
  private PgQueueConsumerInitializationManager pgQueueConsumerInitializationManager;

  @EventListener(ContextRefreshedEvent.class)
  public void onApplicationEvent(@Nonnull ContextRefreshedEvent event) {
    String contextId = event.getApplicationContext().getId();
    log.info("Context refreshed for ID: {}", contextId);

    // Spring Boot's separate Jetty port for actuator uses a distinct WebApplicationContext whose id
    // contains ":management". Running bootstrap there duplicates work and races async boot steps on
    // the main Ebean pool (PostgreSQL "Cannot change transaction isolation level in the middle of a
    // transaction"). Detect by id — parent linkage alone is not reliable across Boot versions.
    if (contextId.contains(":management")) {
      log.info(
          "Skipping GMS bootstrap on actuator management context ({}) — bootstrap runs on the main"
              + " application context only",
          contextId);
      return;
    }

    // Also skip any non-root context that may not include ":management" in its id.
    if (event.getApplicationContext().getParent() != null) {
      log.debug(
          "Skipping bootstrap for non-root WebApplicationContext (child of {})",
          event.getApplicationContext().getParent().getId());
      return;
    }

    // For the root application context only
    if (event.getApplicationContext() instanceof WebApplicationContext) {
      log.info("Root WebApplicationContext initialized, starting bootstrap process");

      // Initialize Ebean first
      try {
        Class.forName("io.ebean.XServiceProvider");
      } catch (ClassNotFoundException e) {
        log.error("Failed to initialize io.ebean.XServiceProvider", e);
        throw new RuntimeException(e);
      }

      // Initialize consumers
      kafkaInitializationManager.initialize(this.getClass().getSimpleName());
      if (pgQueueConsumerInitializationManager != null) {
        pgQueueConsumerInitializationManager.initialize(this.getClass().getSimpleName());
      }

      _bootstrapManager.start(systemOperationContext);
    } else {
      log.debug("Ignoring non-web application context refresh");
    }
  }
}
