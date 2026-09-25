package com.linkedin.metadata.kafka;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.testng.annotations.Test;

public class KafkaConsumerPauseSupportTest {

  @Test
  public void pause_invokesPauseWhenContainerExists() {
    KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);
    MessageListenerContainer container = mock(MessageListenerContainer.class);
    when(registry.getListenerContainer("mcp")).thenReturn(container);

    KafkaConsumerPauseSupport support = new KafkaConsumerPauseSupport(registry);
    support.pause("mcp");

    verify(container).pause();
  }

  @Test
  public void pause_missingContainerIsNoOp() {
    KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);
    when(registry.getListenerContainer("missing")).thenReturn(null);

    KafkaConsumerPauseSupport support = new KafkaConsumerPauseSupport(registry);
    support.pause("missing");
  }

  @Test
  public void resume_invokesResumeWhenContainerExists() {
    KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);
    MessageListenerContainer container = mock(MessageListenerContainer.class);
    when(registry.getListenerContainer("mcp")).thenReturn(container);

    KafkaConsumerPauseSupport support = new KafkaConsumerPauseSupport(registry);
    support.resume("mcp");

    verify(container).resume();
  }

  @Test
  public void resume_missingContainerIsNoOp() {
    KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);
    when(registry.getListenerContainer("missing")).thenReturn(null);

    KafkaConsumerPauseSupport support = new KafkaConsumerPauseSupport(registry);
    support.resume("missing");
  }
}
