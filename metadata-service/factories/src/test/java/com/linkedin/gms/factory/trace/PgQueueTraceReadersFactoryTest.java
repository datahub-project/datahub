package com.linkedin.gms.factory.trace;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.trace.McpFailedTracePort;
import com.linkedin.metadata.trace.McpPendingTracePort;
import com.linkedin.metadata.trace.PgQueueMcpFailedTracePort;
import com.linkedin.metadata.trace.PgQueueMcpPendingTracePort;
import com.linkedin.mxe.Topics;
import java.util.concurrent.ExecutorService;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PgQueueTraceReadersFactoryTest {

  private PgQueueTraceReadersFactory factory;

  @BeforeMethod
  public void setUp() {
    factory = new PgQueueTraceReadersFactory();
    ReflectionTestUtils.setField(factory, "mcpTopicName", Topics.METADATA_CHANGE_PROPOSAL);
    ReflectionTestUtils.setField(
        factory, "mcpFailedTopicName", Topics.FAILED_METADATA_CHANGE_PROPOSAL);
    ReflectionTestUtils.setField(factory, "mceConsumerGroupId", "mce-consumer");
    ReflectionTestUtils.setField(factory, "traceTimeoutSeconds", 30L);
    ReflectionTestUtils.setField(factory, "peekBatchLimit", 100);
    ReflectionTestUtils.setField(factory, "peekMaxRounds", 2);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void mcpTraceReader_buildsPgQueuePendingPort() {
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    ExecutorService executor = mock(ExecutorService.class);
    Deserializer<GenericRecord> deserializer = mock(Deserializer.class);

    McpPendingTracePort port = factory.mcpTraceReader(store, executor, deserializer);

    assertNotNull(port);
    assertTrue(port instanceof PgQueueMcpPendingTracePort);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void mcpFailedTraceReader_buildsPgQueueFailedPort() {
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    ExecutorService executor = mock(ExecutorService.class);
    Deserializer<GenericRecord> deserializer = mock(Deserializer.class);

    McpFailedTracePort port = factory.mcpFailedTraceReader(store, executor, deserializer);

    assertNotNull(port);
    assertTrue(port instanceof PgQueueMcpFailedTracePort);
  }
}
