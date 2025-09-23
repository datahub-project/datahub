package io.datahubproject.event.kafka;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.kafka.core.ConsumerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class KafkaConsumerPoolTest {

  @Mock private ConsumerFactory<String, GenericRecord> consumerFactory;

  @Mock private KafkaConsumer<String, GenericRecord> kafkaConsumer;

  private KafkaConsumerPool kafkaConsumerPool;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.initMocks(this); // Initialize mocks
    when(consumerFactory.createConsumer()).thenReturn(kafkaConsumer);

    // Manually instantiate KafkaConsumerPool after mocks are initialized
    kafkaConsumerPool = new KafkaConsumerPool(consumerFactory, 2, 5);
  }

  @Test
  public void testPoolInitialization() {
    // Verify the consumer is created the initial number of times
    verify(consumerFactory, times(2)).createConsumer();
  }

  @Test
  public void testBorrowConsumerWhenAvailable() throws InterruptedException {
    // Setup initial state
    KafkaConsumer<String, GenericRecord> consumer = kafkaConsumerPool.borrowConsumer();

    // Assertions
    assertNotNull(consumer, "Consumer should not be null when borrowed");
    verify(consumerFactory, times(2)).createConsumer(); // Initial + this borrow
  }

  @Test
  public void testReturnConsumerToPool() throws InterruptedException {
    // Return the consumer and then borrow again to check if it's the same instance
    kafkaConsumerPool.returnConsumer(kafkaConsumer);
    KafkaConsumer<String, GenericRecord> borrowedAgain = kafkaConsumerPool.borrowConsumer();

    // Assertions
    assertSame(borrowedAgain, kafkaConsumer, "The same consumer should be returned from the pool");
  }

  @Test
  public void testShutdownPool() {
    // Call shutdown
    kafkaConsumerPool.shutdownPool();

    // Verify all consumers are closed
    verify(kafkaConsumer, atLeastOnce()).close();
  }
}
