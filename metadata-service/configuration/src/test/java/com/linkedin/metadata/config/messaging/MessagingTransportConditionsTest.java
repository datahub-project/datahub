package com.linkedin.metadata.config.messaging;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.env.Environment;
import org.testng.annotations.Test;

public class MessagingTransportConditionsTest {

  @Test
  public void kafkaEnabledWhenTransportIsKafka() {
    Environment env = mock(Environment.class);
    when(env.getProperty(eq(MessagingTransport.PROPERTY), eq(MessagingTransport.KAFKA)))
        .thenReturn(MessagingTransport.KAFKA);
    ConditionContext ctx = mock(ConditionContext.class);
    when(ctx.getEnvironment()).thenReturn(env);

    assertTrue(new KafkaMessagingEnabledCondition().matches(ctx, null));
    assertFalse(new KafkaMessagingDisabledCondition().matches(ctx, null));
    assertTrue(new KafkaOrPgQueueMessagingTransportCondition().matches(ctx, null));
    assertFalse(new PgQueueMessagingTransportCondition().matches(ctx, null));
    assertFalse(new NonKafkaNonPgQueueMessagingTransportCondition().matches(ctx, null));
  }

  @Test
  public void pgQueueEnabledWhenTransportIsPgqueue() {
    Environment env = mock(Environment.class);
    when(env.getProperty(eq(MessagingTransport.PROPERTY), eq(MessagingTransport.KAFKA)))
        .thenReturn(MessagingTransport.PGQUEUE);
    ConditionContext ctx = mock(ConditionContext.class);
    when(ctx.getEnvironment()).thenReturn(env);

    assertFalse(new KafkaMessagingEnabledCondition().matches(ctx, null));
    assertTrue(new KafkaMessagingDisabledCondition().matches(ctx, null));
    assertTrue(new KafkaOrPgQueueMessagingTransportCondition().matches(ctx, null));
    assertTrue(new PgQueueMessagingTransportCondition().matches(ctx, null));
    assertFalse(new NonKafkaNonPgQueueMessagingTransportCondition().matches(ctx, null));
  }

  @Test
  public void nonKafkaNonPgQueueWhenTransportIsUnknown() {
    Environment env = mock(Environment.class);
    when(env.getProperty(eq(MessagingTransport.PROPERTY), eq(MessagingTransport.KAFKA)))
        .thenReturn("nats");
    ConditionContext ctx = mock(ConditionContext.class);
    when(ctx.getEnvironment()).thenReturn(env);

    assertFalse(new KafkaMessagingEnabledCondition().matches(ctx, null));
    assertTrue(new NonKafkaNonPgQueueMessagingTransportCondition().matches(ctx, null));
    assertFalse(new KafkaOrPgQueueMessagingTransportCondition().matches(ctx, null));
  }
}
