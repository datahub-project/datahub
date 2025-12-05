package io.datahubproject.event.kafka;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.time.Duration;
import java.util.Collections;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CheckedConsumerTest {

  @Mock private KafkaConsumer<String, GenericRecord> kafkaConsumer;
  @Mock private MetricUtils metricUtils;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testInitialState() {
    CheckedConsumer checkedConsumer =
        new CheckedConsumer(kafkaConsumer, Duration.ofSeconds(2), Duration.ofMinutes(5), null);

    assertEquals(
        checkedConsumer.getState(),
        CheckedConsumer.ConsumerState.AVAILABLE,
        "Consumer should start in AVAILABLE state");
  }

  @Test
  public void testStateTransitions() {
    CheckedConsumer checkedConsumer =
        new CheckedConsumer(kafkaConsumer, Duration.ofSeconds(2), Duration.ofMinutes(5), null);

    checkedConsumer.setState(CheckedConsumer.ConsumerState.BORROWED);
    assertEquals(checkedConsumer.getState(), CheckedConsumer.ConsumerState.BORROWED);

    checkedConsumer.setState(CheckedConsumer.ConsumerState.AVAILABLE);
    assertEquals(checkedConsumer.getState(), CheckedConsumer.ConsumerState.AVAILABLE);

    checkedConsumer.setState(CheckedConsumer.ConsumerState.CLOSED);
    assertEquals(checkedConsumer.getState(), CheckedConsumer.ConsumerState.CLOSED);
  }

  @Test
  public void testValidationCache() {
    CheckedConsumer checkedConsumer =
        new CheckedConsumer(kafkaConsumer, Duration.ofSeconds(2), Duration.ofMillis(100), null);

    when(kafkaConsumer.assignment()).thenReturn(Collections.emptySet());
    when(kafkaConsumer.partitionsFor(anyString(), any(Duration.class)))
        .thenReturn(Collections.emptyList());

    assertTrue(checkedConsumer.isValid("test-topic"));
    verify(kafkaConsumer, times(1)).partitionsFor(anyString(), any(Duration.class));

    assertTrue(checkedConsumer.isValid("test-topic"));
    verify(kafkaConsumer, times(1)).partitionsFor(anyString(), any(Duration.class));
  }

  @Test
  public void testValidationCacheExpiration() throws InterruptedException {
    CheckedConsumer checkedConsumer =
        new CheckedConsumer(kafkaConsumer, Duration.ofSeconds(2), Duration.ofMillis(50), null);

    when(kafkaConsumer.assignment()).thenReturn(Collections.emptySet());
    when(kafkaConsumer.partitionsFor(anyString(), any(Duration.class)))
        .thenReturn(Collections.emptyList());

    assertTrue(checkedConsumer.isValid("test-topic"));
    verify(kafkaConsumer, times(1)).partitionsFor(anyString(), any(Duration.class));

    Thread.sleep(60);

    assertTrue(checkedConsumer.isValid("test-topic"));
    verify(kafkaConsumer, times(2)).partitionsFor(anyString(), any(Duration.class));
  }

  @Test
  public void testValidationFailsForInvalidConsumer() {
    CheckedConsumer checkedConsumer =
        new CheckedConsumer(kafkaConsumer, Duration.ofSeconds(2), Duration.ofMinutes(5), null);

    when(kafkaConsumer.assignment()).thenThrow(new IllegalStateException("Consumer is closed"));

    assertFalse(checkedConsumer.isValid("test-topic"));
  }

  @Test
  public void testValidationRecordsMetricsForInvalidConsumer() {
    CheckedConsumer checkedConsumer =
        new CheckedConsumer(
            kafkaConsumer, Duration.ofSeconds(2), Duration.ofMinutes(5), metricUtils);

    when(kafkaConsumer.assignment()).thenThrow(new IllegalStateException("Consumer is closed"));

    assertFalse(checkedConsumer.isValid("test-topic"));
    verify(metricUtils, times(1))
        .increment(eq(CheckedConsumer.class), eq("invalid_consumer_found"), eq(1.0));
  }

  @Test
  public void testValidationDoesNotRecordMetricsForValidConsumer() {
    CheckedConsumer checkedConsumer =
        new CheckedConsumer(
            kafkaConsumer, Duration.ofSeconds(2), Duration.ofMinutes(5), metricUtils);

    when(kafkaConsumer.assignment()).thenReturn(Collections.emptySet());
    when(kafkaConsumer.partitionsFor(anyString(), any(Duration.class)))
        .thenReturn(Collections.emptyList());

    assertTrue(checkedConsumer.isValid("test-topic"));
    verify(metricUtils, never())
        .increment(eq(CheckedConsumer.class), eq("invalid_consumer_found"), anyDouble());
  }

  @Test
  public void testKafkaExceptionTreatedAsValid() {
    CheckedConsumer checkedConsumer =
        new CheckedConsumer(kafkaConsumer, Duration.ofSeconds(2), Duration.ofMinutes(5), null);

    when(kafkaConsumer.assignment()).thenReturn(Collections.emptySet());
    when(kafkaConsumer.partitionsFor(anyString(), any(Duration.class)))
        .thenThrow(new KafkaException("Transient error"));

    assertTrue(checkedConsumer.isValid("test-topic"), "KafkaException should be treated as valid");
  }

  @Test
  public void testValidationWithNullTopic() {
    CheckedConsumer checkedConsumer =
        new CheckedConsumer(kafkaConsumer, Duration.ofSeconds(2), Duration.ofMinutes(5), null);

    assertFalse(checkedConsumer.isValid(null));
    assertFalse(checkedConsumer.isValid(""));
  }

  @Test
  public void testClose() {
    CheckedConsumer checkedConsumer =
        new CheckedConsumer(kafkaConsumer, Duration.ofSeconds(2), Duration.ofMinutes(5), null);

    checkedConsumer.close();

    verify(kafkaConsumer, times(1)).close();
  }

  @Test
  public void testCloseHandlesException() {
    CheckedConsumer checkedConsumer =
        new CheckedConsumer(kafkaConsumer, Duration.ofSeconds(2), Duration.ofMinutes(5), null);

    doThrow(new RuntimeException("Close failed")).when(kafkaConsumer).close();

    checkedConsumer.close();

    verify(kafkaConsumer, times(1)).close();
  }

  @Test
  public void testGetConsumer() {
    CheckedConsumer checkedConsumer =
        new CheckedConsumer(kafkaConsumer, Duration.ofSeconds(2), Duration.ofMinutes(5), null);

    assertSame(checkedConsumer.getConsumer(), kafkaConsumer);
  }
}
