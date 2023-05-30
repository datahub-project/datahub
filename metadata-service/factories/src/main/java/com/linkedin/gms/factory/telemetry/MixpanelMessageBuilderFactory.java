package com.linkedin.gms.factory.telemetry;

import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.mixpanel.mixpanelapi.MessageBuilder;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;


@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)

public class MixpanelMessageBuilderFactory {
  private static final String MIXPANEL_TOKEN = "5ee83d940754d63cacbf7d34daa6f44a";

  @Bean(name = "mixpanelMessageBuilder")
  @ConditionalOnProperty("telemetry.enabledServer")
  @Scope("singleton")
  protected MessageBuilder getInstance() throws Exception {
    return new MessageBuilder(MIXPANEL_TOKEN);
  }
}
