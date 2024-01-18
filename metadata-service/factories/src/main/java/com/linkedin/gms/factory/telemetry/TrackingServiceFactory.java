package com.linkedin.gms.factory.telemetry;

import com.datahub.telemetry.TrackingService;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.secret.SecretService;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.version.GitVersion;
import com.mixpanel.mixpanelapi.MessageBuilder;
import com.mixpanel.mixpanelapi.MixpanelAPI;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;

@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class TrackingServiceFactory {
  @Autowired(required = false)
  @Qualifier("mixpanelApi")
  private MixpanelAPI _mixpanelAPI;

  @Autowired(required = false)
  @Qualifier("mixpanelMessageBuilder")
  private MessageBuilder _mixpanelMessageBuilder;

  @Autowired
  @Qualifier("dataHubSecretService")
  private SecretService _secretService;

  @Autowired
  @Qualifier("entityService")
  private EntityService _entityService;

  @Autowired
  @Qualifier("gitVersion")
  private GitVersion _gitVersion;

  @Bean(name = "trackingService")
  @ConditionalOnProperty("telemetry.enabledServer")
  @Scope("singleton")
  protected TrackingService getInstance() throws Exception {
    return new TrackingService(
        this._mixpanelAPI,
        this._mixpanelMessageBuilder,
        this._secretService,
        this._entityService,
        this._gitVersion);
  }
}
