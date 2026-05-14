package com.linkedin.metadata.kafka.listener.mcl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.MetadataChangeLogConfig;
import com.linkedin.metadata.config.kafka.ConsumerConfiguration;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.kafka.listener.BatchKafkaListenerEndpoint;
import com.linkedin.metadata.kafka.listener.GenericKafkaListener;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MCLKafkaListenerRegistrarTest {

  @Mock private KafkaListenerEndpointRegistry mockKafkaListenerEndpointRegistry;
  @Mock private KafkaListenerContainerFactory<?> mockKafkaListenerContainerFactory;
  @Mock private KafkaListenerContainerFactory<?> mockBatchKafkaListenerContainerFactory;
  private String mockConsumerGroupBase = "test-consumer-group";
  @Mock private ObjectMapper mockObjectMapper;
  @Mock private OperationContext mockOperationContext;
  @Mock private ConfigurationProvider mockConfigurationProvider;
  @Mock private MetadataChangeLogConfig mockMetadataChangeLogConfig;
  @Mock private MetadataChangeLogConfig.ConsumerBatchConfig mockConsumerBatchConfig;
  @Mock private MetadataChangeLogConfig.BatchConfig mockBatchConfig;
  @Mock private KafkaConfiguration mockKafkaConfiguration;
  @Mock private ConsumerConfiguration mockConsumerConfiguration;
  @Mock private ConsumerConfiguration.ConsumerOptions mockMclConsumerOptions;

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
            mockBatchKafkaListenerContainerFactory,
            mockConsumerGroupBase,
            hooks,
            mockObjectMapper,
            mockOperationContext,
            mockConfigurationProvider);

    when(mockConfigurationProvider.getMetadataChangeLog()).thenReturn(mockMetadataChangeLogConfig);
    when(mockMetadataChangeLogConfig.getConsumer()).thenReturn(mockConsumerBatchConfig);
    when(mockConsumerBatchConfig.getBatch()).thenReturn(mockBatchConfig);

    when(mockConfigurationProvider.getKafka()).thenReturn(mockKafkaConfiguration);
    when(mockKafkaConfiguration.getConsumer()).thenReturn(mockConsumerConfiguration);
    when(mockConsumerConfiguration.getMcl()).thenReturn(mockMclConsumerOptions);
    when(mockMclConsumerOptions.isFineGrainedLoggingEnabled()).thenReturn(false);
    when(mockMclConsumerOptions.getAspectsToDrop()).thenReturn(null);
  }

  @Test
  public void testCreateListenerWithBatchEnabled() {
    when(mockBatchConfig.isEnabled()).thenReturn(true);

    GenericKafkaListener<
            com.linkedin.mxe.MetadataChangeLog,
            MetadataChangeLogHook,
            org.apache.avro.generic.GenericRecord>
        listener = registrar.createListener("test-consumer-group", hooks, true, aspectsToDrop);

    assertNotNull(listener);
    assertTrue(listener instanceof MCLBatchKafkaListener);
  }

  @Test
  public void testCreateListenerWithBatchDisabled() {
    when(mockBatchConfig.isEnabled()).thenReturn(false);

    GenericKafkaListener<
            com.linkedin.mxe.MetadataChangeLog,
            MetadataChangeLogHook,
            org.apache.avro.generic.GenericRecord>
        listener = registrar.createListener("test-consumer-group", hooks, true, aspectsToDrop);

    assertNotNull(listener);
    assertTrue(listener instanceof MCLKafkaListener);
  }

  @Test
  public void testGetProcessorTypeWithBatchEnabled() {
    when(mockBatchConfig.isEnabled()).thenReturn(true);
    assertEquals(registrar.getProcessorType(), "BatchMetadataChangeLogProcessor");
  }

  @Test
  public void testGetProcessorTypeWithBatchDisabled() {
    when(mockBatchConfig.isEnabled()).thenReturn(false);
    assertEquals(registrar.getProcessorType(), "MetadataChangeLogProcessor");
  }

  @Test
  public void testCreateListenerWithNullConfiguration() {
    when(mockConfigurationProvider.getMetadataChangeLog()).thenReturn(null);

    GenericKafkaListener<
            com.linkedin.mxe.MetadataChangeLog,
            MetadataChangeLogHook,
            org.apache.avro.generic.GenericRecord>
        listener = registrar.createListener("test-consumer-group", hooks, true, aspectsToDrop);

    assertNotNull(listener);
    assertTrue(listener instanceof MCLKafkaListener);
  }

  @Test
  public void testCreateListenerWithNullBatchConfig() {
    when(mockConsumerBatchConfig.getBatch()).thenReturn(null);

    GenericKafkaListener<
            com.linkedin.mxe.MetadataChangeLog,
            MetadataChangeLogHook,
            org.apache.avro.generic.GenericRecord>
        listener = registrar.createListener("test-consumer-group", hooks, true, aspectsToDrop);

    assertNotNull(listener);
    assertTrue(listener instanceof MCLKafkaListener);
  }

  @Test
  public void testCreateEndpointRegistersConsumeMethodWhenBatchDisabled() {
    when(mockBatchConfig.isEnabled()).thenReturn(false);

    KafkaListenerEndpoint endpoint =
        registrar.createListenerEndpoint("test-consumer-group", List.of("topic1"), hooks);

    assertNotNull(endpoint);
    assertTrue(endpoint instanceof MethodKafkaListenerEndpoint);
    MethodKafkaListenerEndpoint<?, ?> methodEndpoint = (MethodKafkaListenerEndpoint<?, ?>) endpoint;
    assertEquals(methodEndpoint.getMethod().getName(), "consume");
  }

  @Test
  public void testCreateEndpointReturnsBatchEndpointWhenBatchEnabled() {
    when(mockBatchConfig.isEnabled()).thenReturn(true);

    KafkaListenerEndpoint endpoint =
        registrar.createListenerEndpoint("test-consumer-group", List.of("topic1"), hooks);

    assertNotNull(endpoint);
    assertTrue(endpoint instanceof BatchKafkaListenerEndpoint);
    assertTrue(endpoint.getBatchListener());
  }

  @Test
  public void testRegisterKafkaListenerUsesBatchFactoryWhenBatchEnabled() {
    when(mockBatchConfig.isEnabled()).thenReturn(true);

    KafkaListenerEndpoint endpoint =
        registrar.createListenerEndpoint("test-consumer-group", List.of("topic1"), hooks);
    registrar.registerKafkaListener(endpoint, false);

    verify(mockKafkaListenerEndpointRegistry)
        .registerListenerContainer(
            any(KafkaListenerEndpoint.class),
            eq(mockBatchKafkaListenerContainerFactory),
            eq(false));
  }

  @Test
  public void testRegisterKafkaListenerUsesStandardFactoryWhenBatchDisabled() {
    when(mockBatchConfig.isEnabled()).thenReturn(false);

    KafkaListenerEndpoint endpoint =
        registrar.createListenerEndpoint("test-consumer-group", List.of("topic1"), hooks);
    registrar.registerKafkaListener(endpoint, false);

    verify(mockKafkaListenerEndpointRegistry)
        .registerListenerContainer(
            any(KafkaListenerEndpoint.class), eq(mockKafkaListenerContainerFactory), eq(false));
  }

  @Test
  public void testCreateEndpointIsBatchEndpointWhenBatchEnabled() {
    when(mockBatchConfig.isEnabled()).thenReturn(true);

    KafkaListenerEndpoint endpoint =
        registrar.createListenerEndpoint("test-consumer-group", List.of("topic1"), hooks);

    assertTrue(endpoint instanceof BatchKafkaListenerEndpoint);
    assertEquals(endpoint.getId(), "test-consumer-group");
    assertEquals(endpoint.getGroupId(), "test-consumer-group");
  }

  @Test
  public void testCreateEndpointBeanIsStandardListenerWhenBatchDisabled() {
    when(mockBatchConfig.isEnabled()).thenReturn(false);

    KafkaListenerEndpoint endpoint =
        registrar.createListenerEndpoint("test-consumer-group", List.of("topic1"), hooks);

    MethodKafkaListenerEndpoint<?, ?> methodEndpoint = (MethodKafkaListenerEndpoint<?, ?>) endpoint;
    assertTrue(methodEndpoint.getBean() instanceof MCLKafkaListener);
  }

  @Test
  public void testIsBatchEnabledReturnsFalseWhenConsumerConfigIsNull() {
    when(mockMetadataChangeLogConfig.getConsumer()).thenReturn(null);

    assertFalse(registrar.isBatchEnabled());
  }

  @Test
  public void testIsBatchEnabledReturnsFalseOnException() {
    when(mockConfigurationProvider.getMetadataChangeLog()).thenThrow(new RuntimeException("boom"));

    assertFalse(registrar.isBatchEnabled());
  }

  @Test
  public void testBatchEndpointSetsConsumerGroupAndTopics() {
    when(mockBatchConfig.isEnabled()).thenReturn(true);

    KafkaListenerEndpoint endpoint =
        registrar.createListenerEndpoint("my-batch-group", List.of("topic-a", "topic-b"), hooks);

    assertTrue(endpoint instanceof BatchKafkaListenerEndpoint);
    assertEquals(endpoint.getId(), "my-batch-group");
    assertEquals(endpoint.getGroupId(), "my-batch-group");
    assertTrue(endpoint.getTopics().contains("topic-a"));
    assertTrue(endpoint.getTopics().contains("topic-b"));
  }
}
