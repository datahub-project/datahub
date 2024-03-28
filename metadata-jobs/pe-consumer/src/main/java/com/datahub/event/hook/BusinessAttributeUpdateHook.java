package com.datahub.event.hook;

import com.linkedin.gms.factory.common.GraphServiceFactory;
import com.linkedin.gms.factory.entity.EntityServiceFactory;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.metadata.service.BusinessAttributeUpdateHookService;
import com.linkedin.mxe.PlatformEvent;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Import({EntityServiceFactory.class, EntityRegistryFactory.class, GraphServiceFactory.class})
public class BusinessAttributeUpdateHook implements PlatformEventHook {

  protected final BusinessAttributeUpdateHookService businessAttributeUpdateHookService;

  public BusinessAttributeUpdateHook(
      BusinessAttributeUpdateHookService businessAttributeUpdateHookService) {
    this.businessAttributeUpdateHookService = businessAttributeUpdateHookService;
  }

  /**
   * Invoke the hook when a PlatformEvent is received
   *
   * @param event
   */
  @Override
  public void invoke(@Nonnull PlatformEvent event) {
    businessAttributeUpdateHookService.handleChangeEvent(event);
  }
}
