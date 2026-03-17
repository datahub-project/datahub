package com.linkedin.gms.factory.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.kafka.TopicsConfiguration;
import io.datahubproject.event.ExternalEventsService;
import io.datahubproject.event.kafka.KafkaConsumerPool;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@ConditionalOnProperty(name = "eventsApi.enabled", havingValue = "true")
@Configuration
public class ExternalEventsServiceFactory {

  @Value("${eventsApi.defaultPollTimeoutSeconds:5}")
  private int pollTimeout;

  @Value("${eventsApi.defaultLimit:100}")
  private int defaultLimit;

  @Autowired private ConfigurationProvider configurationProvider;

  @Autowired private KafkaConsumerPool consumerPool;

  @Autowired private ObjectMapper objectMapper;

  @Bean
  public ExternalEventsService externalEventsService() {
    return new ExternalEventsService(
        buildPollAllowedTopics(),
        consumerPool,
        objectMapper,
        buildTopicNameMappings(),
        pollTimeout,
        defaultLimit);
  }

  private Set<String> buildPollAllowedTopics() {
    return configurationProvider.getKafka().getTopics().getTopics().values().stream()
        .filter(t -> t.getPollEnabled())
        .map(TopicsConfiguration.TopicConfiguration::getName)
        .collect(Collectors.toSet());
  }

  private Map<String, String> buildTopicNameMappings() {
    return configurationProvider.getKafka().getTopics().getTopics().values().stream()
        .filter(t -> t.getPollEnabled())
        .collect(
            Collectors.toMap(
                TopicsConfiguration.TopicConfiguration::getName,
                TopicsConfiguration.TopicConfiguration::getName));
  }
}
