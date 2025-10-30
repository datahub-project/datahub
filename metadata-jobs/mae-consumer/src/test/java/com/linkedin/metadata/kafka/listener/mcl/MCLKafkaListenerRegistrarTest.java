package com.linkedin.metadata.kafka.listener.mcl;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.MetadataChangeLogConfig;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.kafka.listener.GenericKafkaListener;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MCLKafkaListenerRegistrarTest {

  @Mock private KafkaListenerEndpointRegistry mockKafkaListenerEndpointRegistry;
  @Mock private KafkaListenerContainerFactory<?> mockKafkaListenerContainerFactory;
  private String mockConsumerGroupBase = "test-consumer-group";
  @Mock private ObjectMapper mockObjectMapper;
  @Mock private OperationContext mockOperationContext;
  @Mock private ConfigurationProvider mockConfigurationProvider;
  @Mock private MetadataChangeLogConfig mockMetadataChangeLogConfig;
  @Mock private MetadataChangeLogConfig.ConsumerBatchConfig mockConsumerBatchConfig;
  @Mock private MetadataChangeLogConfig.BatchConfig mockBatchConfig;

  private MCLKafkaListenerRegistrar registrar;
  private List<MetadataChangeLogHook> hooks;
  private Map<String, Set<String>> aspectsToDrop;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    hooks = Collections.emptyList();
    aspectsToDrop = Collections.emptyMap();

    registrar =
        new MCLKafkaListenerRegistrar(
            mockKafkaListenerEndpointRegistry,
            mockKafkaListenerContainerFactory,
            mockConsumerGroupBase,
            hooks,
            mockObjectMapper,
            mockOperationContext,
            mockConfigurationProvider);

    // Setup configuration mocks
    when(mockConfigurationProvider.getMetadataChangeLog()).thenReturn(mockMetadataChangeLogConfig);
    when(mockMetadataChangeLogConfig.getConsumer()).thenReturn(mockConsumerBatchConfig);
    when(mockConsumerBatchConfig.getBatch()).thenReturn(mockBatchConfig);
  }

  @Test
  public void testCreateListenerWithBatchEnabled() {
    // Configure batch processing to be enabled
    when(mockBatchConfig.isEnabled()).thenReturn(true);

    GenericKafkaListener<
            com.linkedin.mxe.MetadataChangeLog,
            MetadataChangeLogHook,
            org.apache.avro.generic.GenericRecord>
        listener = registrar.createListener("test-consumer-group", hooks, true, aspectsToDrop);

    assertNotNull(listener);
    // Should create MCLBatchKafkaListener when batch is enabled
    assertTrue(listener instanceof MCLBatchKafkaListener);
  }

  @Test
  public void testCreateListenerWithBatchDisabled() {
    // Configure batch processing to be disabled
    when(mockBatchConfig.isEnabled()).thenReturn(false);

    GenericKafkaListener<
            com.linkedin.mxe.MetadataChangeLog,
            MetadataChangeLogHook,
            org.apache.avro.generic.GenericRecord>
        listener = registrar.createListener("test-consumer-group", hooks, true, aspectsToDrop);

    assertNotNull(listener);
    // Should create MCLKafkaListener when batch is disabled
    assertTrue(listener instanceof MCLKafkaListener);
  }

  @Test
  public void testGetProcessorTypeWithBatchEnabled() {
    when(mockBatchConfig.isEnabled()).thenReturn(true);

    String processorType = registrar.getProcessorType();

    assertEquals(processorType, "BatchMetadataChangeLogProcessor");
  }

  @Test
  public void testGetProcessorTypeWithBatchDisabled() {
    when(mockBatchConfig.isEnabled()).thenReturn(false);

    String processorType = registrar.getProcessorType();

    assertEquals(processorType, "MetadataChangeLogProcessor");
  }

  @Test
  public void testCreateListenerWithNullConfiguration() {
    // Test behavior when configuration is null
    when(mockConfigurationProvider.getMetadataChangeLog()).thenReturn(null);

    GenericKafkaListener<
            com.linkedin.mxe.MetadataChangeLog,
            MetadataChangeLogHook,
            org.apache.avro.generic.GenericRecord>
        listener = registrar.createListener("test-consumer-group", hooks, true, aspectsToDrop);

    assertNotNull(listener);
    // Should default to MCLKafkaListener when configuration is null
    assertTrue(listener instanceof MCLKafkaListener);
  }

  @Test
  public void testCreateListenerWithNullBatchConfig() {
    // Test behavior when batch config is null
    when(mockConsumerBatchConfig.getBatch()).thenReturn(null);

    GenericKafkaListener<
            com.linkedin.mxe.MetadataChangeLog,
            MetadataChangeLogHook,
            org.apache.avro.generic.GenericRecord>
        listener = registrar.createListener("test-consumer-group", hooks, true, aspectsToDrop);

    assertNotNull(listener);
    // Should default to MCLKafkaListener when batch config is null
    assertTrue(listener instanceof MCLKafkaListener);
  }

  @Test
  public void testCreateListenerWithDifferentConsumerGroupIds() {
    when(mockBatchConfig.isEnabled()).thenReturn(true);

    // Test with different consumer group IDs
    GenericKafkaListener<
            com.linkedin.mxe.MetadataChangeLog,
            MetadataChangeLogHook,
            org.apache.avro.generic.GenericRecord>
        listener1 = registrar.createListener("consumer-group-1", hooks, true, aspectsToDrop);

    GenericKafkaListener<
            com.linkedin.mxe.MetadataChangeLog,
            MetadataChangeLogHook,
            org.apache.avro.generic.GenericRecord>
        listener2 = registrar.createListener("consumer-group-2", hooks, true, aspectsToDrop);

    assertNotNull(listener1);
    assertNotNull(listener2);
    assertTrue(listener1 instanceof MCLBatchKafkaListener);
    assertTrue(listener2 instanceof MCLBatchKafkaListener);
  }

  @Test
  public void testCreateListenerWithDifferentFineGrainedLoggingSettings() {
    when(mockBatchConfig.isEnabled()).thenReturn(true);

    // Test with fine-grained logging enabled
    GenericKafkaListener<
            com.linkedin.mxe.MetadataChangeLog,
            MetadataChangeLogHook,
            org.apache.avro.generic.GenericRecord>
        listener1 = registrar.createListener("test-consumer-group", hooks, true, aspectsToDrop);

    // Test with fine-grained logging disabled
    GenericKafkaListener<
            com.linkedin.mxe.MetadataChangeLog,
            MetadataChangeLogHook,
            org.apache.avro.generic.GenericRecord>
        listener2 = registrar.createListener("test-consumer-group", hooks, false, aspectsToDrop);

    assertNotNull(listener1);
    assertNotNull(listener2);
    assertTrue(listener1 instanceof MCLBatchKafkaListener);
    assertTrue(listener2 instanceof MCLBatchKafkaListener);
  }

  @Test
  public void testCreateListenerWithDifferentAspectsToDrop() {
    when(mockBatchConfig.isEnabled()).thenReturn(true);

    Map<String, Set<String>> emptyAspectsToDrop = Collections.emptyMap();
    Map<String, Set<String>> nonEmptyAspectsToDrop =
        Map.of("dataset", Set.of("aspect1", "aspect2"));

    GenericKafkaListener<
            com.linkedin.mxe.MetadataChangeLog,
            MetadataChangeLogHook,
            org.apache.avro.generic.GenericRecord>
        listener1 =
            registrar.createListener("test-consumer-group", hooks, true, emptyAspectsToDrop);

    GenericKafkaListener<
            com.linkedin.mxe.MetadataChangeLog,
            MetadataChangeLogHook,
            org.apache.avro.generic.GenericRecord>
        listener2 =
            registrar.createListener("test-consumer-group", hooks, true, nonEmptyAspectsToDrop);

    assertNotNull(listener1);
    assertNotNull(listener2);
    assertTrue(listener1 instanceof MCLBatchKafkaListener);
    assertTrue(listener2 instanceof MCLBatchKafkaListener);
  }
}
