package com.linkedin.gms.factory.event;

import static io.datahubproject.event.ExternalEventsService.METADATA_CHANGE_LOG_TIMESERIES_TOPIC_NAME;
import static io.datahubproject.event.ExternalEventsService.METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME;
import static io.datahubproject.event.ExternalEventsService.PLATFORM_EVENT_TOPIC_NAME;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.mxe.TopicConvention;
import io.datahubproject.event.ExternalEventsService;
import io.datahubproject.event.TopicAllowList;
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

  @Value("${eventsApi.defaultPollTimeoutSeconds:5}")
  private int pollTimeout;

  @Value("${eventsApi.defaultLimit:100}")
  private int defaultLimit;

  @Value("${eventsApi.pollAllowedTopics}")
  private String pollAllowedTopics;

  @Value("${kafka.topicPrefixEnabled}")
  private boolean topicPrefixEnabled;

  @Value("${kafka.topicPrefix}")
  private String topicPrefix;

  @Autowired private TopicConvention topicConvention;

  @Autowired private KafkaConsumerPool consumerPool;

  @Autowired private ObjectMapper objectMapper;

  @Bean
  public TopicAllowList topicAllowList() {
    return new TopicAllowList(pollAllowedTopics);
  }

  @Bean
  public ExternalEventsService externalEventsService(TopicAllowList topicAllowList) {
    return new ExternalEventsService(
        topicAllowList,
        consumerPool,
        objectMapper,
        buildTopicNameMappings(),
        topicPrefixEnabled,
        topicPrefix,
        pollTimeout,
        defaultLimit);
  }

  private Map<String, String> buildTopicNameMappings() {
    final Map<String, String> topicNames = new HashMap<>();
    topicNames.put(PLATFORM_EVENT_TOPIC_NAME, topicConvention.getPlatformEventTopicName());
    topicNames.put(
        METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME,
        topicConvention.getMetadataChangeLogVersionedTopicName());
    topicNames.put(
        METADATA_CHANGE_LOG_TIMESERIES_TOPIC_NAME,
        topicConvention.getMetadataChangeLogTimeseriesTopicName());
    return topicNames;
  }
}
