package com.linkedin.metadata.kafka.config;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.messaging.MessagingTransport;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PgQueueMessagingProcessorConditionsTest {

  @Mock private ConditionContext context;
  @Mock private AnnotatedTypeMetadata metadata;
  @Mock private Environment env;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    when(context.getEnvironment()).thenReturn(env);
    when(env.getProperty(MessagingTransport.PROPERTY, MessagingTransport.KAFKA))
        .thenReturn(MessagingTransport.PGQUEUE);
  }

  @Test
  public void mcpCondition_matchesWhenPgQueueAndMcpConsumerEnabled() {
    when(env.getProperty("MCE_CONSUMER_ENABLED")).thenReturn("true");
    when(env.getProperty("metadataChangeProposal.consumer.batch.enabled", "false"))
        .thenReturn("false");

    assertTrue(
        new PgQueueMessagingAndMetadataChangeProposalProcessorCondition()
            .matches(context, metadata));
  }

  @Test
  public void mcpCondition_noMatchWhenBatchMcpEnabled() {
    when(env.getProperty("MCE_CONSUMER_ENABLED")).thenReturn("true");
    when(env.getProperty("metadataChangeProposal.consumer.batch.enabled", "false"))
        .thenReturn("true");

    assertFalse(
        new PgQueueMessagingAndMetadataChangeProposalProcessorCondition()
            .matches(context, metadata));
  }

  @Test
  public void batchMcpCondition_matchesWhenPgQueueAndBatchEnabled() {
    when(env.getProperty("MCE_CONSUMER_ENABLED")).thenReturn("true");
    when(env.getProperty("metadataChangeProposal.consumer.batch.enabled", "false"))
        .thenReturn("true");

    assertTrue(
        new PgQueueMessagingAndBatchMetadataChangeProposalProcessorCondition()
            .matches(context, metadata));
  }
}
