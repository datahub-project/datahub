package com.datahub.event.config;

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
  public void platformEventCondition_matchesWhenPgQueueAndPeEnabled() {
    when(env.getProperty("PE_CONSUMER_ENABLED")).thenReturn("true");

    assertTrue(new PgQueueMessagingAndPlatformEventProcessorCondition().matches(context, metadata));
  }

  @Test
  public void platformEventCondition_noMatchWhenKafkaTransport() {
    when(env.getProperty(MessagingTransport.PROPERTY, MessagingTransport.KAFKA))
        .thenReturn(MessagingTransport.KAFKA);
    when(env.getProperty("PE_CONSUMER_ENABLED")).thenReturn("true");

    assertFalse(
        new PgQueueMessagingAndPlatformEventProcessorCondition().matches(context, metadata));
  }

  @Test
  public void platformEventCondition_noMatchWhenPeDisabled() {
    when(env.getProperty("PE_CONSUMER_ENABLED")).thenReturn("false");

    assertFalse(
        new PgQueueMessagingAndPlatformEventProcessorCondition().matches(context, metadata));
  }
}
