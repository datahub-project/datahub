package com.linkedin.gms.factory.common;

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

  @Value("${GENERIC_METADATA_CHANGE_EVENT_NAME:" + Topics.METADATA_CHANGE_PROPOSAL + "}")
  private String MetadataChangeProposalName;

  @Value("${GENERIC_METADATA_AUDIT_EVENT_NAME:" + Topics.METADATA_CHANGE_LOG + "}")
  private String MetadataChangeLogName;

  @Value("${GENERIC_FAILED_METADATA_CHANGE_EVENT_NAME:" + Topics.FAILED_METADATA_CHANGE_PROPOSAL + "}")
  private String FailedMetadataChangeProposalName;

  @Bean(name = TOPIC_CONVENTION_BEAN)
  protected TopicConvention createInstance() {
    return new TopicConventionImpl(metadataChangeEventName, metadataAuditEventName, failedMetadataChangeEventName,
        MetadataChangeProposalName, MetadataChangeLogName, FailedMetadataChangeProposalName,
        // TODO once we start rolling out v5 add support for changing the new event names.
        TopicConventionImpl.DEFAULT_EVENT_PATTERN);
  }
}
