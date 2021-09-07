package com.linkedin.metadata.boot;

import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;


/**
 * Responsible for coordinating boot-time checks.
 */
@Slf4j
@Component
public class BootstrapManagerApplicationListener implements ApplicationListener<ContextRefreshedEvent> {

  @Autowired
  @Qualifier("bootstrapManager")
  private BootstrapManager _bootstrapManager;

  @Override
  public void onApplicationEvent(@Nonnull ContextRefreshedEvent event) {
    _bootstrapManager.start();
  }
}
