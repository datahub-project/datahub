package com.linkedin.metadata.kafka.batch;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.kafka.context.inbound.DefaultInboundBatchAffinityResolver;
import com.linkedin.metadata.kafka.context.inbound.InboundBatchAffinityResolver;
import com.linkedin.metadata.kafka.listener.BatchKafkaListenerEndpoint;
import com.linkedin.mxe.Topics;
import io.datahubproject.metadata.context.OperationContext;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MCPBatchKafkaListenerRegistrarTest {

  @Mock private KafkaListenerEndpointRegistry mockKafkaListenerEndpointRegistry;
  @Mock private KafkaListenerContainerFactory<?> mockBatchKafkaListenerContainerFactory;
  @Mock private BatchMetadataChangeProposalsProcessor mockProcessor;
  @Mock private OperationContext mockOperationContext;

  private final InboundBatchAffinityResolver batchAffinityResolver =
      new DefaultInboundBatchAffinityResolver();

  private MCPBatchKafkaListenerRegistrar registrar;

  private static final String CONSUMER_GROUP = "generic-mce-consumer-job-client";

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    registrar =
        new MCPBatchKafkaListenerRegistrar(
            mockKafkaListenerEndpointRegistry,
            mockBatchKafkaListenerContainerFactory,
            mockProcessor,
            batchAffinityResolver,
            mockOperationContext);
    ReflectionTestUtils.setField(registrar, "mcpTopicName", Topics.METADATA_CHANGE_PROPOSAL);
    ReflectionTestUtils.setField(registrar, "consumerGroupId", CONSUMER_GROUP);
  }

  @Test
  public void testAfterPropertiesSetRegistersBatchKafkaListenerEndpoint() throws Exception {
    registrar.afterPropertiesSet();

    ArgumentCaptor<KafkaListenerEndpoint> endpointCaptor =
        ArgumentCaptor.forClass(KafkaListenerEndpoint.class);
    verify(mockKafkaListenerEndpointRegistry)
        .registerListenerContainer(
            endpointCaptor.capture(), eq(mockBatchKafkaListenerContainerFactory), eq(false));

    KafkaListenerEndpoint endpoint = endpointCaptor.getValue();
    assertNotNull(endpoint);
    assertTrue(endpoint instanceof BatchKafkaListenerEndpoint);
    assertTrue(endpoint.getBatchListener());
    assertEquals(endpoint.getId(), CONSUMER_GROUP);
    assertEquals(endpoint.getGroupId(), CONSUMER_GROUP);
    assertEquals(endpoint.getGroup(), CONSUMER_GROUP);
    assertEquals(endpoint.getTopics().iterator().next(), Topics.METADATA_CHANGE_PROPOSAL);
  }
}
