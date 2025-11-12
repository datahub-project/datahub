package io.datahubproject.event.kafka;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.kafka.core.ConsumerFactory;
import org.testng.annotations.AfterClass;
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
    KafkaConsumer<String, GenericRecord> consumer =
        kafkaConsumerPool.borrowConsumer(1000, TimeUnit.MILLISECONDS);

    // Assertions
    assertNotNull(consumer, "Consumer should not be null when borrowed");
    verify(consumerFactory, times(2)).createConsumer(); // Initial + this borrow
    kafkaConsumerPool.returnConsumer(consumer);
  }

  @Test
  public void testBorrowConsumerReturnsNullAfterTimeout() throws InterruptedException {
    // First, exhaust the pool by borrowing all initial consumers
    KafkaConsumer<String, GenericRecord> kafkaConsumer1 =
        kafkaConsumerPool.borrowConsumer(100, TimeUnit.MILLISECONDS);
    KafkaConsumer<String, GenericRecord> kafkaConsumer2 =
        kafkaConsumerPool.borrowConsumer(100, TimeUnit.MILLISECONDS);
    KafkaConsumer<String, GenericRecord> kafkaConsumer3 =
        kafkaConsumerPool.borrowConsumer(100, TimeUnit.MILLISECONDS);
    KafkaConsumer<String, GenericRecord> kafkaConsumer4 =
        kafkaConsumerPool.borrowConsumer(100, TimeUnit.MILLISECONDS);
    KafkaConsumer<String, GenericRecord> kafkaConsumer5 =
        kafkaConsumerPool.borrowConsumer(100, TimeUnit.MILLISECONDS);

    // Now pool is empty and at max size, borrowing should timeout and return null
    long startTime = System.currentTimeMillis();
    KafkaConsumer<String, GenericRecord> consumer =
        kafkaConsumerPool.borrowConsumer(500, TimeUnit.MILLISECONDS);
    long elapsedTime = System.currentTimeMillis() - startTime;

    assertNull(consumer, "Consumer should be null after timeout when pool is exhausted");
    assertTrue(elapsedTime >= 500, "Should wait at least the timeout duration");
    assertTrue(elapsedTime < 1000, "Should not wait significantly longer than timeout");
    kafkaConsumerPool.returnConsumer(kafkaConsumer1);
    kafkaConsumerPool.returnConsumer(kafkaConsumer2);
    kafkaConsumerPool.returnConsumer(kafkaConsumer3);
    kafkaConsumerPool.returnConsumer(kafkaConsumer4);
    kafkaConsumerPool.returnConsumer(kafkaConsumer5);
  }

  @Test
  public void testBorrowConsumerWithZeroTimeout() throws InterruptedException {
    // Exhaust the pool
    KafkaConsumer<String, GenericRecord> kafkaConsumer1 =
        kafkaConsumerPool.borrowConsumer(100, TimeUnit.MILLISECONDS);
    KafkaConsumer<String, GenericRecord> kafkaConsumer2 =
        kafkaConsumerPool.borrowConsumer(100, TimeUnit.MILLISECONDS);
    KafkaConsumer<String, GenericRecord> kafkaConsumer3 =
        kafkaConsumerPool.borrowConsumer(100, TimeUnit.MILLISECONDS);
    KafkaConsumer<String, GenericRecord> kafkaConsumer4 =
        kafkaConsumerPool.borrowConsumer(100, TimeUnit.MILLISECONDS);
    KafkaConsumer<String, GenericRecord> kafkaConsumer5 =
        kafkaConsumerPool.borrowConsumer(100, TimeUnit.MILLISECONDS);

    // Try to borrow with 0 timeout - should return null immediately
    long startTime = System.currentTimeMillis();
    KafkaConsumer<String, GenericRecord> consumer =
        kafkaConsumerPool.borrowConsumer(0, TimeUnit.MILLISECONDS);
    long elapsedTime = System.currentTimeMillis() - startTime;

    assertNull(consumer, "Consumer should be null immediately with 0 timeout");
    assertTrue(elapsedTime < 100, "Should return almost immediately");
    kafkaConsumerPool.returnConsumer(kafkaConsumer1);
    kafkaConsumerPool.returnConsumer(kafkaConsumer2);
    kafkaConsumerPool.returnConsumer(kafkaConsumer3);
    kafkaConsumerPool.returnConsumer(kafkaConsumer4);
    kafkaConsumerPool.returnConsumer(kafkaConsumer5);
  }

  @Test
  public void testBorrowConsumerSucceedsWhenConsumerReturnedDuringWait()
      throws InterruptedException {
    // Create a limited pool
    KafkaConsumerPool limitedPool = new KafkaConsumerPool(consumerFactory, 1, 1);

    // Borrow the only consumer
    KafkaConsumer<String, GenericRecord> consumer1 =
        limitedPool.borrowConsumer(100, TimeUnit.MILLISECONDS);
    assertNotNull(consumer1);

    // Create a thread that will return the consumer after a short delay
    Thread returnThread =
        new Thread(
            () -> {
              try {
                Thread.sleep(200); // Wait 200ms before returning
                limitedPool.returnConsumer(consumer1);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });
    returnThread.start();

    // Try to borrow - should wait and succeed when consumer is returned
    long startTime = System.currentTimeMillis();
    KafkaConsumer<String, GenericRecord> consumer2 =
        limitedPool.borrowConsumer(1000, TimeUnit.MILLISECONDS);
    long elapsedTime = System.currentTimeMillis() - startTime;

    assertNotNull(consumer2, "Should receive consumer when it's returned during wait");
    assertSame(consumer2, consumer1, "Should receive the same consumer that was returned");
    assertTrue(elapsedTime >= 200, "Should wait until consumer is returned");
    assertTrue(elapsedTime < 1000, "Should not wait for full timeout");

    returnThread.join();
    limitedPool.shutdownPool();
  }

  @Test
  public void testBorrowConsumerReturnsNullWhenConsumerNotReturnedInTime()
      throws InterruptedException {
    // Create a limited pool
    KafkaConsumerPool limitedPool = new KafkaConsumerPool(consumerFactory, 1, 1);

    // Borrow the only consumer
    KafkaConsumer<String, GenericRecord> consumer1 =
        limitedPool.borrowConsumer(100, TimeUnit.MILLISECONDS);
    assertNotNull(consumer1);

    // Create a thread that will return the consumer after timeout expires
    Thread returnThread =
        new Thread(
            () -> {
              try {
                Thread.sleep(800); // Wait longer than the timeout
                limitedPool.returnConsumer(consumer1);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });
    returnThread.start();

    // Try to borrow with shorter timeout - should return null
    long startTime = System.currentTimeMillis();
    KafkaConsumer<String, GenericRecord> consumer2 =
        limitedPool.borrowConsumer(300, TimeUnit.MILLISECONDS);
    long elapsedTime = System.currentTimeMillis() - startTime;

    assertNull(consumer2, "Should return null when consumer not returned within timeout");
    assertTrue(elapsedTime >= 300, "Should wait for the full timeout");
    assertTrue(elapsedTime < 600, "Should not wait significantly longer");

    returnThread.join();
    limitedPool.shutdownPool();
  }

  @Test
  public void testMultipleConcurrentBorrowsWithTimeout() throws InterruptedException {
    // Create a pool with limited size
    KafkaConsumerPool limitedPool = new KafkaConsumerPool(consumerFactory, 2, 2);

    // Borrow all consumers
    limitedPool.borrowConsumer(100, TimeUnit.MILLISECONDS);
    limitedPool.borrowConsumer(100, TimeUnit.MILLISECONDS);

    // Try to borrow from multiple threads
    Thread thread1 =
        new Thread(
            () -> {
              try {
                KafkaConsumer<String, GenericRecord> consumer =
                    limitedPool.borrowConsumer(300, TimeUnit.MILLISECONDS);
                assertNull(consumer, "Thread 1 should timeout");
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });

    Thread thread2 =
        new Thread(
            () -> {
              try {
                KafkaConsumer<String, GenericRecord> consumer =
                    limitedPool.borrowConsumer(300, TimeUnit.MILLISECONDS);
                assertNull(consumer, "Thread 2 should timeout");
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });

    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();
    limitedPool.shutdownPool();
  }

  @AfterClass
  public void testShutdownPool() {
    // Call shutdown
    kafkaConsumerPool.shutdownPool();

    // Verify all consumers are closed
    verify(kafkaConsumer, atLeastOnce()).close();
  }
}
