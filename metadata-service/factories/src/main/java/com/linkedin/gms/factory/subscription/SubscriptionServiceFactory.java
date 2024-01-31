package com.linkedin.gms.factory.subscription;

import com.datahub.subscription.SubscriptionService;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;

@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class SubscriptionServiceFactory {
  @Bean(name = "subscriptionService")
  @Scope("singleton")
  @Nonnull
  protected SubscriptionService getInstance(@Qualifier("entityClient") EntityClient entityClient)
      throws Exception {
    return new SubscriptionService(entityClient);
  }
}
