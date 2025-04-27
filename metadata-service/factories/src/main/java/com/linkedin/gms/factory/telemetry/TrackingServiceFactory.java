package com.linkedin.gms.factory.telemetry;

import com.datahub.telemetry.TrackingService;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.telemetry.MixpanelConfiguration;
import com.linkedin.metadata.config.telemetry.TelemetryConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.version.GitVersion;
import com.mixpanel.mixpanelapi.MessageBuilder;
import com.mixpanel.mixpanelapi.MixpanelAPI;
import io.datahubproject.metadata.services.SecretService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Slf4j
@Configuration
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
  private EntityService<?> _entityService;

  @Autowired
  @Qualifier("gitVersion")
  private GitVersion _gitVersion;

  @Autowired(required = false)
  @Qualifier("dataHubUsageProducer")
  private Producer<String, String> _dataHubUsageProducer;

  @Autowired private ConfigurationProvider _configurationProvider;

  @Bean(name = "trackingService")
  @Scope("singleton")
  protected TrackingService getInstance() throws Exception {

    TelemetryConfiguration _telemetryConfiguration = _configurationProvider.getTelemetry();
    MixpanelConfiguration _mixpanelConfiguration = _telemetryConfiguration.getMixpanel();

    // Check if telemetry is enabled
    boolean telemetryEnabled = _telemetryConfiguration.isEnabledServer();

    log.info(
        "Initializing TrackingService with telemetry enabled: {}, MixpanelAPI: {} and MixpanelMessageBuilder: {}",
        telemetryEnabled,
        _mixpanelAPI != null ? "present" : "null",
        _mixpanelMessageBuilder != null ? "present" : "null");

    log.info(
        "Initializing TrackingService with DataHubUsageProducer: {}",
        _dataHubUsageProducer != null ? "present" : "null");

    // If telemetry is disabled, pass null for Mixpanel components
    MixpanelAPI mixpanelAPI = telemetryEnabled ? _mixpanelAPI : null;
    MessageBuilder mixpanelMessageBuilder = telemetryEnabled ? _mixpanelMessageBuilder : null;
    MixpanelConfiguration mixpanelConfiguration = telemetryEnabled ? _mixpanelConfiguration : null;

    TrackingService trackingService =
        new TrackingService(
            mixpanelConfiguration,
            _secretService,
            mixpanelMessageBuilder,
            mixpanelAPI,
            _entityService,
            _gitVersion,
            _dataHubUsageProducer);

    log.info("TrackingService initialized successfully");
    return trackingService;
  }
}
