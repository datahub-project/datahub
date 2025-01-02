package com.linkedin.metadata.boot;

import com.linkedin.gms.factory.kafka.common.KafkaInitializationManager;
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

  @EventListener(ContextRefreshedEvent.class)
  public void onApplicationEvent(@Nonnull ContextRefreshedEvent event) {
    String contextId = event.getApplicationContext().getId();
    log.info("Context refreshed for ID: {}", contextId);

    // For the root application context
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

      _bootstrapManager.start(systemOperationContext);
    } else {
      log.debug("Ignoring non-web application context refresh");
    }
  }
}
