package com.linkedin.metadata.examples.configs;

import com.linkedin.mxe.TopicConvention;
import com.linkedin.mxe.TopicConventionImpl;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


/**
 * Creates a {@link TopicConvention} to generate kafka metadata event topic names.
 *
 * <p>This allows you to easily override Kafka topic names within your organization.
 */
@Configuration
public class TopicConventionFactory {
  public static final String TOPIC_CONVENTION_BEAN = "metadataKafkaTopicConvention";

  @Value("${METADATA_CHANGE_EVENT_NAME:" + TopicConventionImpl.DEFAULT_METADATA_CHANGE_EVENT_NAME + "}")
  private String metadataChangeEventName;

  @Value("${METADATA_AUDIT_EVENT_NAME:" + TopicConventionImpl.DEFAULT_METADATA_AUDIT_EVENT_NAME + "}")
  private String metadataAuditEventName;

  @Value("${FAILED_METADATA_CHANGE_EVENT_NAME:" + TopicConventionImpl.DEFAULT_FAILED_METADATA_CHANGE_EVENT_NAME + "}")
  private String failedMetadataChangeEventName;

  @Bean(name = TOPIC_CONVENTION_BEAN)
  protected TopicConvention createInstance() {
    return new TopicConventionImpl(metadataChangeEventName, metadataAuditEventName, failedMetadataChangeEventName,
        // TODO once we start rolling out v5 add support for changing the new event names.
        TopicConventionImpl.DEFAULT_EVENT_PATTERN);
  }
}
