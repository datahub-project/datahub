package com.linkedin.metadata.config.messaging;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.env.Environment;
import org.testng.annotations.Test;

public class KafkaMessagingConditionsTest {

  @Test
  public void kafkaEnabledCondition_defaultPropertyIsKafka() {
    Environment env = mock(Environment.class);
    when(env.getProperty(eq(MessagingTransport.PROPERTY), eq(MessagingTransport.KAFKA)))
        .thenReturn(MessagingTransport.KAFKA);
    ConditionContext ctx = mock(ConditionContext.class);
    when(ctx.getEnvironment()).thenReturn(env);
    assertTrue(new KafkaMessagingEnabledCondition().matches(ctx, null));
    assertFalse(new KafkaMessagingDisabledCondition().matches(ctx, null));
  }

  @Test
  public void kafkaDisabledWhenTransportIsPgqueue() {
    Environment env = mock(Environment.class);
    when(env.getProperty(eq(MessagingTransport.PROPERTY), eq(MessagingTransport.KAFKA)))
        .thenReturn(MessagingTransport.PGQUEUE);
    ConditionContext ctx = mock(ConditionContext.class);
    when(ctx.getEnvironment()).thenReturn(env);
    assertFalse(new KafkaMessagingEnabledCondition().matches(ctx, null));
    assertTrue(new KafkaMessagingDisabledCondition().matches(ctx, null));
  }

  @Test
  public void kafkaEnabledCaseInsensitive() {
    Environment env = mock(Environment.class);
    when(env.getProperty(eq(MessagingTransport.PROPERTY), eq(MessagingTransport.KAFKA)))
        .thenReturn("Kafka");
    ConditionContext ctx = mock(ConditionContext.class);
    when(ctx.getEnvironment()).thenReturn(env);
    assertTrue(new KafkaMessagingEnabledCondition().matches(ctx, null));
    assertFalse(new KafkaMessagingDisabledCondition().matches(ctx, null));
  }

  @Test
  public void pgQueueTransportCondition_matchesPgQueueOnly() {
    Environment env = mock(Environment.class);
    when(env.getProperty(eq(MessagingTransport.PROPERTY), eq(MessagingTransport.KAFKA)))
        .thenReturn(MessagingTransport.PGQUEUE);
    ConditionContext ctx = mock(ConditionContext.class);
    when(ctx.getEnvironment()).thenReturn(env);
    assertTrue(new PgQueueMessagingTransportCondition().matches(ctx, null));
  }

  @Test
  public void pgQueueTransportCondition_doesNotMatchKafka() {
    Environment env = mock(Environment.class);
    when(env.getProperty(eq(MessagingTransport.PROPERTY), eq(MessagingTransport.KAFKA)))
        .thenReturn(MessagingTransport.KAFKA);
    ConditionContext ctx = mock(ConditionContext.class);
    when(ctx.getEnvironment()).thenReturn(env);
    assertFalse(new PgQueueMessagingTransportCondition().matches(ctx, null));
  }

  @Test
  public void nonKafkaNonPgQueueCondition_matchesUnknownTransport() {
    Environment env = mock(Environment.class);
    when(env.getProperty(eq(MessagingTransport.PROPERTY), eq(MessagingTransport.KAFKA)))
        .thenReturn("amqp");
    ConditionContext ctx = mock(ConditionContext.class);
    when(ctx.getEnvironment()).thenReturn(env);
    assertTrue(new NonKafkaNonPgQueueMessagingTransportCondition().matches(ctx, null));
  }

  @Test
  public void nonKafkaNonPgQueueCondition_doesNotMatchKafkaOrPgQueue() {
    Environment env = mock(Environment.class);
    when(env.getProperty(eq(MessagingTransport.PROPERTY), eq(MessagingTransport.KAFKA)))
        .thenReturn(MessagingTransport.PGQUEUE);
    ConditionContext ctx = mock(ConditionContext.class);
    when(ctx.getEnvironment()).thenReturn(env);
    assertFalse(new NonKafkaNonPgQueueMessagingTransportCondition().matches(ctx, null));

    when(env.getProperty(eq(MessagingTransport.PROPERTY), eq(MessagingTransport.KAFKA)))
        .thenReturn(MessagingTransport.KAFKA);
    assertFalse(new NonKafkaNonPgQueueMessagingTransportCondition().matches(ctx, null));
  }

  @Test
  public void kafkaOrPgQueue_matchesKafkaDefault() {
    Environment env = mock(Environment.class);
    when(env.getProperty(eq(MessagingTransport.PROPERTY), eq(MessagingTransport.KAFKA)))
        .thenReturn(MessagingTransport.KAFKA);
    ConditionContext ctx = mock(ConditionContext.class);
    when(ctx.getEnvironment()).thenReturn(env);
    assertTrue(new KafkaOrPgQueueMessagingTransportCondition().matches(ctx, null));
  }

  @Test
  public void kafkaOrPgQueue_matchesPgQueue() {
    Environment env = mock(Environment.class);
    when(env.getProperty(eq(MessagingTransport.PROPERTY), eq(MessagingTransport.KAFKA)))
        .thenReturn(MessagingTransport.PGQUEUE);
    ConditionContext ctx = mock(ConditionContext.class);
    when(ctx.getEnvironment()).thenReturn(env);
    assertTrue(new KafkaOrPgQueueMessagingTransportCondition().matches(ctx, null));
  }

  @Test
  public void kafkaOrPgQueue_doesNotMatchUnknownTransport() {
    Environment env = mock(Environment.class);
    when(env.getProperty(eq(MessagingTransport.PROPERTY), eq(MessagingTransport.KAFKA)))
        .thenReturn("amqp");
    ConditionContext ctx = mock(ConditionContext.class);
    when(ctx.getEnvironment()).thenReturn(env);
    assertFalse(new KafkaOrPgQueueMessagingTransportCondition().matches(ctx, null));
  }
}
