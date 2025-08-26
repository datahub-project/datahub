package com.linkedin.metadata.boot;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.kafka.common.KafkaInitializationManager;
import com.linkedin.metadata.version.GitVersion;
import io.datahubproject.metadata.context.OperationContext;
import io.sentry.Sentry;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
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
  @Qualifier("gitVersion")
  private GitVersion gitVersion;

  @Value("${sentry.enabled}")
  private Boolean sentryEnabled;

  @Value("${sentry.dsn}")
  private String sentryDsn;

  @Value("${sentry.env}")
  private String sentryEnv;

  @Value("${sentry.debug}")
  private Boolean sentryDebug;

  @Autowired
  @Qualifier("configurationProvider")
  private ConfigurationProvider provider;

  @Value("${bootstrap.servlets.waitTimeout}")
  private int _servletsWaitTimeout;

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

      if (sentryEnabled) {
        Sentry.init(
            options -> {
              options.setDsn(sentryDsn);
              options.setRelease(gitVersion.getVersion());
              options.setEnvironment(sentryEnv);
              options.setTracesSampleRate(0.0);
              options.setDebug(sentryDebug);
            });
        if (sentryDebug) {
          try {
            throw new Exception("This is a test.");
          } catch (Exception e) {
            Sentry.captureException(e);
          }
        }
      }

      // Initialize consumers
      kafkaInitializationManager.initialize(this.getClass().getSimpleName());

      _bootstrapManager.start(systemOperationContext);
    } else {
      log.debug("Ignoring non-web application context refresh");
    }
  }
}
