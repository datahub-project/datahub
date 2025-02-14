package com.datahub.event.hook;

import com.linkedin.metadata.service.BusinessAttributeUpdateHookService;
import com.linkedin.mxe.PlatformEvent;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class BusinessAttributeUpdateHook implements PlatformEventHook {

  protected final BusinessAttributeUpdateHookService businessAttributeUpdateHookService;
  protected final boolean enabled;

  public BusinessAttributeUpdateHook(
      BusinessAttributeUpdateHookService businessAttributeUpdateHookService,
      @Value("${featureFlags.businessAttributeEntityEnabled}") boolean enabled) {
    this.businessAttributeUpdateHookService = businessAttributeUpdateHookService;
    this.enabled = enabled;
  }

  @Override
  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public void init() {
    log.info("Initialized PlatformEventHook: BusinessAttributeUpdateHook");
  }

  /**
   * Invoke the hook when a PlatformEvent is received
   *
   * @param event
   */
  @Override
  public void invoke(@Nonnull OperationContext opContext, @Nonnull PlatformEvent event) {
    businessAttributeUpdateHookService.handleChangeEvent(opContext, event);
  }
}
