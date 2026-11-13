package com.linkedin.gms.factory.telemetry;

import com.datahub.telemetry.MixpanelEventSender;
import com.datahub.telemetry.TrackingService;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.kafka.TopicsConfiguration;
import com.linkedin.metadata.config.telemetry.TelemetryConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.event.UsageEventPublisher;
import com.linkedin.metadata.version.GitVersion;
import com.mixpanel.mixpanelapi.MessageBuilder;
import io.datahubproject.metadata.services.SecretService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Slf4j
@Configuration
public class TrackingServiceFactory {
  @Autowired(required = false)
  @Qualifier("mixpanelEventSender")
  private MixpanelEventSender _mixpanelEventSender;

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
  @Qualifier("dataHubUsageEventProducer")
  private UsageEventPublisher usageEventPublisher;

  @Autowired private ConfigurationProvider _configurationProvider;

  @Bean(name = "trackingService")
  @Scope("singleton")
  protected TrackingService getInstance() throws Exception {

    TelemetryConfiguration _telemetryConfiguration = _configurationProvider.getTelemetry();
    TopicsConfiguration _topicsConfiguration = _configurationProvider.getKafka().getTopics();

    // Check if telemetry is enabled
    boolean telemetryEnabled = _telemetryConfiguration.isEnabledServer();

    log.info(
        "Initializing TrackingService with telemetry enabled: {}, MixpanelEventSender: {} and MixpanelMessageBuilder: {}",
        telemetryEnabled,
        _mixpanelEventSender != null ? "present" : "null",
        _mixpanelMessageBuilder != null ? "present" : "null");

    log.info(
        "Initializing TrackingService with DataHubUsageProducer: {}",
        usageEventPublisher != null ? "present" : "null");

    // If telemetry is disabled, pass null for Mixpanel components
    MixpanelEventSender mixpanelEventSender = telemetryEnabled ? _mixpanelEventSender : null;
    MessageBuilder mixpanelMessageBuilder = telemetryEnabled ? _mixpanelMessageBuilder : null;

    TrackingService trackingService =
        new TrackingService(
            _topicsConfiguration,
            _secretService,
            mixpanelMessageBuilder,
            mixpanelEventSender,
            _entityService,
            _gitVersion,
            usageEventPublisher);

    log.info("TrackingService initialized successfully");
    return trackingService;
  }
}
