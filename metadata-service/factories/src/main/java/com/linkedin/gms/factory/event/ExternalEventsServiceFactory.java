package com.linkedin.gms.factory.event;

import static io.datahubproject.event.ExternalEventsService.PLATFORM_EVENT_TOPIC_NAME;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.mxe.TopicConvention;
import io.datahubproject.event.ExternalEventsService;
import io.datahubproject.event.kafka.KafkaConsumerPool;
import java.util.HashMap;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@ConditionalOnProperty(name = "eventsApi.enabled", havingValue = "true")
@Configuration
public class ExternalEventsServiceFactory {

  @Value("${eventsApi.defaultPollTimeoutSeconds:10}")
  private int pollTimeout;

  @Value("${eventsApi.defaultLimit:100}")
  private int defaultLimit;

  @Autowired private TopicConvention topicConvention;

  @Autowired private KafkaConsumerPool consumerPool;

  @Autowired private ObjectMapper objectMapper;

  @Bean
  public ExternalEventsService externalEventsService() {
    return new ExternalEventsService(
        consumerPool, objectMapper, buildTopicNameMappings(), pollTimeout, defaultLimit);
  }

  private Map<String, String> buildTopicNameMappings() {
    final Map<String, String> topicNames = new HashMap<>();
    topicNames.put(PLATFORM_EVENT_TOPIC_NAME, topicConvention.getPlatformEventTopicName());
    return topicNames;
  }
}
