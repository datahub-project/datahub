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
  public void usageEventsCondition_matchesWhenPgQueueAndMaeEnabled() {
    when(env.getProperty("MAE_CONSUMER_ENABLED")).thenReturn("true");
    when(env.getProperty("MCL_CONSUMER_ENABLED")).thenReturn("false");

    assertTrue(
        new PgQueueMessagingAndDataHubUsageEventsProcessorCondition().matches(context, metadata));
  }

  @Test
  public void usageEventsCondition_noMatchWhenKafkaTransport() {
    when(env.getProperty(MessagingTransport.PROPERTY, MessagingTransport.KAFKA))
        .thenReturn(MessagingTransport.KAFKA);
    when(env.getProperty("MAE_CONSUMER_ENABLED")).thenReturn("true");

    assertFalse(
        new PgQueueMessagingAndDataHubUsageEventsProcessorCondition().matches(context, metadata));
  }

  @Test
  public void mclCondition_matchesWhenPgQueueAndMaeEnabled() {
    when(env.getProperty("MAE_CONSUMER_ENABLED")).thenReturn("true");
    when(env.getProperty("MCL_CONSUMER_ENABLED")).thenReturn("false");

    assertTrue(
        new PgQueueMessagingAndMetadataChangeLogProcessorCondition().matches(context, metadata));
  }

  @Test
  public void mclCondition_noMatchWhenConsumersDisabled() {
    when(env.getProperty("MAE_CONSUMER_ENABLED")).thenReturn("false");
    when(env.getProperty("MCL_CONSUMER_ENABLED")).thenReturn("false");

    assertFalse(
        new PgQueueMessagingAndMetadataChangeLogProcessorCondition().matches(context, metadata));
  }
}
