package com.linkedin.datahub.upgrade.config;

import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.stereotype.Component;

/**
 * Upgrade jobs scan every entity, so they must see non-primary dataset siblings even when the
 * deployment hides them from search ({@code ELASTICSEARCH_SEARCH_HIDE_NON_PRIMARY_SIBLINGS}). Sets
 * {@code includeNonPrimarySiblings} on the upgrade app's system operation context, which every step
 * either receives or derives its context from. GMS builds its own system context and is unaffected.
 */
@Component
public class IncludeNonPrimarySiblingsPostProcessor implements BeanPostProcessor {

  static final String SYSTEM_OPERATION_CONTEXT = "systemOperationContext";

  @Override
  public Object postProcessAfterInitialization(
      @Nonnull final Object bean, @Nonnull final String beanName) {
    if (SYSTEM_OPERATION_CONTEXT.equals(beanName) && bean instanceof OperationContext) {
      return ((OperationContext) bean)
          .withSearchFlags(flags -> flags.setIncludeNonPrimarySiblings(true));
    }
    return bean;
  }
}
