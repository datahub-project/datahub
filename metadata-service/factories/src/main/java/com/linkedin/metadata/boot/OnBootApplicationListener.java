package com.linkedin.metadata.boot;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.kafka.schemaregistry.InternalSchemaRegistryFactory;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.web.context.WebApplicationContext;

/** Responsible for coordinating starting steps that happen before the application starts up. */
@Configuration
@Slf4j
@Component
public class OnBootApplicationListener {
  private static final Set<Integer> ACCEPTED_HTTP_CODES =
      Set.of(
          HttpStatus.SC_OK,
          HttpStatus.SC_MOVED_PERMANENTLY,
          HttpStatus.SC_MOVED_TEMPORARILY,
          HttpStatus.SC_FORBIDDEN,
          HttpStatus.SC_UNAUTHORIZED);

  private static final String ROOT_WEB_APPLICATION_CONTEXT_ID =
      String.format("%s:", WebApplicationContext.class.getName());

  private final CloseableHttpClient httpClient = HttpClients.createDefault();

  private final ExecutorService executorService = Executors.newSingleThreadExecutor();

  @Autowired
  @Qualifier("bootstrapManager")
  private BootstrapManager _bootstrapManager;

  @Autowired
  @Qualifier("configurationProvider")
  private ConfigurationProvider provider;

  @Value("${bootstrap.servlets.waitTimeout}")
  private int _servletsWaitTimeout;

  @EventListener(ContextRefreshedEvent.class)
  public void onApplicationEvent(@Nonnull ContextRefreshedEvent event) {
    log.warn(
        "OnBootApplicationListener context refreshed! {} event: {}",
        ROOT_WEB_APPLICATION_CONTEXT_ID.equals(event.getApplicationContext().getId()),
        event);
    String schemaRegistryType = provider.getKafka().getSchemaRegistry().getType();
    if (ROOT_WEB_APPLICATION_CONTEXT_ID.equals(event.getApplicationContext().getId())) {
      if (InternalSchemaRegistryFactory.TYPE.equals(schemaRegistryType)) {
        executorService.submit(isSchemaRegistryAPIServletReady());
      } else {
        _bootstrapManager.start();
      }
    }
  }

  public Runnable isSchemaRegistryAPIServletReady() {
    return () -> {
      final HttpGet request = new HttpGet(provider.getKafka().getSchemaRegistry().getUrl());
      int timeouts = _servletsWaitTimeout;
      boolean openAPIServeletReady = false;
      while (!openAPIServeletReady && timeouts > 0) {
        try {
          log.info("Sleeping for 1 second");
          Thread.sleep(1000);
          StatusLine statusLine = httpClient.execute(request).getStatusLine();
          if (ACCEPTED_HTTP_CODES.contains(statusLine.getStatusCode())) {
            log.info("Connected! Authentication not tested.");
            openAPIServeletReady = true;
          }
        } catch (IOException | InterruptedException e) {
          log.info("Failed to connect to open servlet: {}", e.getMessage());
        }
        timeouts--;
      }
      if (!openAPIServeletReady) {
        log.error(
            "Failed to bootstrap DataHub, OpenAPI servlet was not ready after {} seconds",
            timeouts);
        System.exit(1);
      } else {
        _bootstrapManager.start();
      }
    };
  }
}
