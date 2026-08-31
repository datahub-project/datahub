package com.linkedin.gms.factory.kafka;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.kafka.ConsumerConfiguration;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import org.springframework.kafka.event.ConsumerRetryAuthEvent;
import org.springframework.kafka.event.ConsumerRetryAuthSuccessfulEvent;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class KafkaConsumerAuthExceptionGuardTest {

  private KafkaConsumerAuthExceptionGuard guard;
  private MessageListenerContainer container;

  @BeforeMethod
  public void setup() {
    ConfigurationProvider configProvider = mock(ConfigurationProvider.class);
    KafkaConfiguration kafkaConfig = new KafkaConfiguration();
    ConsumerConfiguration consumerConfig = new ConsumerConfiguration();
    consumerConfig.setMaxAuthExceptionRetries(3);
    kafkaConfig.setConsumer(consumerConfig);
    when(configProvider.getKafka()).thenReturn(kafkaConfig);

    guard = new KafkaConsumerAuthExceptionGuard(configProvider);
    container = mock(MessageListenerContainer.class);
    when(container.getListenerId()).thenReturn("test-consumer");
  }

  @Test
  void testStopsContainerAfterMaxConsecutiveRetries() {
    for (int i = 0; i < 2; i++) {
      guard.onAuthRetry(authRetryEvent());
    }
    verify(container, never()).stop();

    // The 3rd retry triggers the stop
    guard.onAuthRetry(authRetryEvent());
    verify(container, timeout(1000)).stop();
  }

  @Test
  void testSuccessResetsRetryCount() {
    // Accumulate failures just below the threshold
    guard.onAuthRetry(authRetryEvent());
    guard.onAuthRetry(authRetryEvent());

    // Transient recovery resets the counter
    guard.onAuthRetrySuccess(authSuccessEvent());

    // Another full round of retries below threshold: container must not stop
    guard.onAuthRetry(authRetryEvent());
    guard.onAuthRetry(authRetryEvent());
    verify(container, never()).stop();
  }

  private ConsumerRetryAuthEvent authRetryEvent() {
    return new ConsumerRetryAuthEvent(
        container, container, ConsumerRetryAuthEvent.Reason.AUTHENTICATION);
  }

  private ConsumerRetryAuthSuccessfulEvent authSuccessEvent() {
    return new ConsumerRetryAuthSuccessfulEvent(container, container);
  }
}
