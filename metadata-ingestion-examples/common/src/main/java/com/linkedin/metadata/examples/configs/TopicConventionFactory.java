package com.linkedin.metadata.examples.configs;

import com.linkedin.mxe.TopicConvention;
import com.linkedin.mxe.TopicConventionImpl;
import com.linkedin.mxe.Topics;
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

  @Value("${METADATA_CHANGE_EVENT_NAME:" + Topics.METADATA_CHANGE_EVENT + "}")
  private String metadataChangeEventName;

  @Value("${METADATA_AUDIT_EVENT_NAME:" + Topics.METADATA_AUDIT_EVENT + "}")
  private String metadataAuditEventName;

  @Value("${FAILED_METADATA_CHANGE_EVENT_NAME:" + Topics.FAILED_METADATA_CHANGE_EVENT + "}")
  private String failedMetadataChangeEventName;

  @Value("${METADATA_CHANGE_PROPOSAL_TOPIC_NAME:" + Topics.METADATA_CHANGE_PROPOSAL + "}")
  private String metadataChangeProposalName;

  @Value("${METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME:" + Topics.METADATA_CHANGE_LOG_VERSIONED + "}")
  private String metadataChangeLogVersionedTopicName;

  @Value("${METADATA_CHANGE_LOG_TIMESERIES_TOPIC_NAME:" + Topics.METADATA_CHANGE_LOG_TIMESERIES + "}")
  private String metadataChangeLogTimeseriesTopicName;

  @Value("${FAILED_METADATA_CHANGE_PROPOSAL_TOPIC_NAME:" + Topics.FAILED_METADATA_CHANGE_PROPOSAL + "}")
  private String failedMetadataChangeProposalName;

  @Value("${PLATFORM_EVENT_TOPIC_NAME:" + Topics.PLATFORM_EVENT + "}")
  private String platformEventTopicName;

  @Bean(name = TOPIC_CONVENTION_BEAN)
  protected TopicConvention createInstance() {
    return new TopicConventionImpl(metadataChangeEventName, metadataAuditEventName, failedMetadataChangeEventName,
        metadataChangeProposalName, metadataChangeLogVersionedTopicName, metadataChangeLogTimeseriesTopicName,
        failedMetadataChangeProposalName, platformEventTopicName,
        // TODO once we start rolling out v5 add support for changing the new event names.
        TopicConventionImpl.DEFAULT_EVENT_PATTERN);
  }
}
