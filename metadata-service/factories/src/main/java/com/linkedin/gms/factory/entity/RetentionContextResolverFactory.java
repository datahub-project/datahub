package com.linkedin.gms.factory.entity;

import com.linkedin.metadata.entity.retention.RetentionContextResolver;
import com.linkedin.metadata.entity.retention.SimpleRetentionContextResolver;
import javax.annotation.Nonnull;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * OSS default {@link RetentionContextResolver}: a no-op that carries no routing metadata on buffer
 * keys and returns the system {@link io.datahubproject.metadata.context.OperationContext} unchanged
 * — matching a single-database deployment. An extension module that routes to multiple underlying
 * databases may override with a {@code @Primary} bean; this default is gated on
 * {@code @ConditionalOnMissingBean} so it backs off when an override is present.
 */
@Configuration
public class RetentionContextResolverFactory {

  @Bean
  @ConditionalOnMissingBean(RetentionContextResolver.class)
  @Nonnull
  protected RetentionContextResolver retentionContextResolver() {
    return new SimpleRetentionContextResolver();
  }
}
