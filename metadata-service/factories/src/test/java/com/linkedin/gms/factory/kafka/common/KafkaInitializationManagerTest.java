package com.linkedin.gms.factory.kafka.common;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@SpringBootTest
@ContextConfiguration(classes = {KafkaInitializationManager.class})
@EnableKafka
@DirtiesContext
public class KafkaInitializationManagerTest extends AbstractTestNGSpringContextTests {

  @Autowired private KafkaInitializationManager manager;

  @MockBean private KafkaListenerEndpointRegistry registry;

  private MessageListenerContainer container1;
  private MessageListenerContainer container2;
  private List<MessageListenerContainer> containers;

  @BeforeMethod
  public void setUp() {
    // Reset the initialization state before each test
    // This is necessary because Spring maintains the same instance across tests
    try {
      java.lang.reflect.Field field =
          KafkaInitializationManager.class.getDeclaredField("isInitialized");
      field.setAccessible(true);
      ((java.util.concurrent.atomic.AtomicBoolean) field.get(manager)).set(false);
    } catch (Exception e) {
      throw new RuntimeException("Failed to reset initialization state", e);
    }

    // Create fresh mocks for each test
    container1 = mock(MessageListenerContainer.class);
    container2 = mock(MessageListenerContainer.class);
    containers = Arrays.asList(container1, container2);

    // Clear any previous mock interactions
    reset(registry);
  }

  @Test
  public void testInitialize_WhenNotInitialized() {
    // Arrange
    when(registry.getAllListenerContainers()).thenReturn(containers);
    when(container1.isRunning()).thenReturn(false);
    when(container2.isRunning()).thenReturn(false);
    when(container1.getListenerId()).thenReturn("container1");
    when(container2.getListenerId()).thenReturn("container2");

    // Act
    manager.initialize("TestInitializer");

    // Assert
    verify(container1, times(1)).start();
    verify(container2, times(1)).start();
    assertTrue(manager.isInitialized());
  }

  @Test
  public void testInitialize_WhenAlreadyInitialized() {
    // Arrange
    when(registry.getAllListenerContainers()).thenReturn(containers);

    // First initialization
    manager.initialize("FirstInitializer");

    // Clear previous interactions
    clearInvocations(container1, container2);

    // Act
    manager.initialize("SecondInitializer");

    // Assert
    verify(container1, never()).start();
    verify(container2, never()).start();
    assertTrue(manager.isInitialized());
  }

  @Test
  public void testInitialize_WithNoContainers() {
    // Arrange
    when(registry.getAllListenerContainers()).thenReturn(Collections.emptyList());

    // Act
    manager.initialize("TestInitializer");

    // Assert
    assertTrue(manager.isInitialized());
  }

  @Test
  public void testInitialize_WithAlreadyRunningContainer() {
    // Arrange
    when(registry.getAllListenerContainers()).thenReturn(Collections.singletonList(container1));
    when(container1.isRunning()).thenReturn(true);
    when(container1.getListenerId()).thenReturn("container1");

    // Act
    manager.initialize("TestInitializer");

    // Assert
    verify(container1, never()).start();
    assertTrue(manager.isInitialized());
  }

  @Test
  public void testOnStateChange_WhenAcceptingTraffic() {
    // Arrange
    AvailabilityChangeEvent<ReadinessState> event =
        new AvailabilityChangeEvent<>(this, ReadinessState.ACCEPTING_TRAFFIC);
    when(registry.getAllListenerContainers()).thenReturn(containers);
    when(container1.isRunning()).thenReturn(false);
    when(container2.isRunning()).thenReturn(false);
    when(container1.getListenerId()).thenReturn("container1");
    when(container2.getListenerId()).thenReturn("container2");

    // Act
    manager.onStateChange(event);

    // Assert
    assertTrue(manager.isInitialized());
    // The method is called twice in the implementation - once for size() and once for iteration
    verify(registry, times(2)).getAllListenerContainers();
    verify(container1).start();
    verify(container2).start();
  }

  @Test
  public void testOnStateChange_WhenNotAcceptingTraffic() {
    // Arrange
    AvailabilityChangeEvent<ReadinessState> event =
        new AvailabilityChangeEvent<>(this, ReadinessState.REFUSING_TRAFFIC);

    // Act
    manager.onStateChange(event);

    // Assert
    assertFalse(manager.isInitialized());
    verify(registry, never()).getAllListenerContainers();
  }

  @Test
  public void testIsInitialized_DefaultState() {
    // Assert
    assertFalse(manager.isInitialized());
  }
}
