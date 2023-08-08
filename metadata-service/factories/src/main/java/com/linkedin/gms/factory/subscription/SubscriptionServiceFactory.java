package com.linkedin.gms.factory.subscription;

import com.datahub.subscription.SubscriptionService;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.client.JavaEntityClient;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;


@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class SubscriptionServiceFactory {
  @Autowired
  @Qualifier("javaEntityClient")
  private JavaEntityClient _javaEntityClient;

  @Bean(name = "subscriptionService")
  @Scope("singleton")
  @Nonnull
  protected SubscriptionService getInstance() throws Exception {
    return new SubscriptionService(this._javaEntityClient);
  }
}
