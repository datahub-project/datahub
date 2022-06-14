package com.linkedin.metadata.boot;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;
import org.springframework.web.context.WebApplicationContext;


/**
 * Responsible for coordinating steps that run when the application has started up.
 */
@Slf4j
@Component
public class OnReadyApplicationListener implements ApplicationListener<ApplicationReadyEvent> {

  private static final String ROOT_WEB_APPLICATION_CONTEXT_ID = String.format("%s:", WebApplicationContext.class.getName());

  @Autowired
  @Qualifier("bootstrapManager")
  private BootstrapManager _bootstrapManager;

  @Override
  public void onApplicationEvent(ApplicationReadyEvent event) {
    if (ROOT_WEB_APPLICATION_CONTEXT_ID.equals(event.getApplicationContext().getId())) {
      _bootstrapManager.start(BootstrapStep.ExecutionTime.ON_READY);
    }
  }
}