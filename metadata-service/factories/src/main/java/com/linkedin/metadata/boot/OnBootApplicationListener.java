package com.linkedin.metadata.boot;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HTTP;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.web.context.WebApplicationContext;


/**
 * Responsible for coordinating starting steps that happen before the application starts up.
 */
@Slf4j
@Component
public class OnBootApplicationListener {

  private static final String ROOT_WEB_APPLICATION_CONTEXT_ID = String.format("%s:", WebApplicationContext.class.getName());

  private final CloseableHttpClient httpClient = HttpClients.createDefault();

  private final ExecutorService executorService = Executors.newSingleThreadExecutor();

  @Autowired
  @Qualifier("bootstrapManager")
  private BootstrapManager _bootstrapManager;


  @EventListener(ContextRefreshedEvent.class)
  public void onApplicationEvent(@Nonnull ContextRefreshedEvent event) {
    log.warn("OnBootApplicationListener context refreshed! {} event: {}",
        ROOT_WEB_APPLICATION_CONTEXT_ID.equals(event.getApplicationContext().getId()), event);
    if (ROOT_WEB_APPLICATION_CONTEXT_ID.equals(event.getApplicationContext().getId())) {
      executorService.submit(isOpenAPIServeletReady());
    }
  }

  public Runnable isOpenAPIServeletReady() {
    return () -> {
        final HttpGet request = new HttpGet("http://localhost:8080/openapi/up/");
        int timeouts = 30;
        boolean openAPIServeletReady = false;
        while(!openAPIServeletReady && timeouts > 0){
          try {
            Thread.sleep(1000);
            StatusLine statusLine = httpClient.execute(request).getStatusLine();
            if (statusLine.getStatusCode() == HttpStatus.SC_OK && statusLine.getReasonPhrase().equals("OK")) {
              openAPIServeletReady = true;
            }
          } catch (IOException | InterruptedException e) {
            log.info("Failed to connect to open servlet: ", e);
          }
          //openAPIServeletReady = isOpenAPIServeletReady();
          timeouts--;
        }
        if (!openAPIServeletReady) {
          log.error("Failed to bootstrap DataHub, OpenAPI servlet was not ready after 30 seconds");
        } else {
        _bootstrapManager.start();
        }
    };
  }
}
