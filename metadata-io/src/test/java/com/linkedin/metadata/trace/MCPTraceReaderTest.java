package com.linkedin.metadata.trace;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.EventUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.TraceContext;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jetbrains.annotations.Nullable;
import org.testng.annotations.Test;

public final class MCPTraceReaderTest extends BaseKafkaTraceReaderTest<MetadataChangeProposal> {
  @Override
  KafkaTraceReader<MetadataChangeProposal> buildTraceReader() {
    return MCPTraceReader.builder()
        .adminClient(adminClient)
        .consumerSupplier(() -> consumer)
        .pollDurationMs(100)
        .pollMaxAttempts(3)
        .executorService(executorService)
        .timeoutSeconds(5)
        .topicName(TOPIC_NAME)
        .consumerGroupId(CONSUMER_GROUP)
        .build();
  }

  @Override
  MetadataChangeProposal buildMessage(@Nullable SystemMetadata systemMetadata) {
    return new MetadataChangeProposal()
        .setAspectName(ASPECT_NAME)
        .setEntityType(TEST_URN.getEntityType())
        .setChangeType(ChangeType.UPSERT)
        .setEntityUrn(TEST_URN)
        .setSystemMetadata(systemMetadata, SetMode.IGNORE_NULL);
  }

  @Override
  GenericRecord toGenericRecord(MetadataChangeProposal message) throws IOException {
    return EventUtils.pegasusToAvroMCP(message);
  }

  @Override
  MetadataChangeProposal fromGenericRecord(GenericRecord genericRecord) throws IOException {
    return EventUtils.avroToPegasusMCP(genericRecord);
  }

  @Test
  public void testMCPRead() throws Exception {
    MetadataChangeProposal expectedMCP = buildMessage(null);

    GenericRecord genericRecord = toGenericRecord(expectedMCP);

    Optional<MetadataChangeProposal> result = traceReader.read(genericRecord);

    assertTrue(result.isPresent());
    assertEquals(result.get().getAspectName(), ASPECT_NAME);
  }

  @Test
  public void testMCPMatchConsumerRecord() throws Exception {
    ConsumerRecord<String, GenericRecord> mockConsumerRecord = mock(ConsumerRecord.class);

    SystemMetadata systemMetadata = new SystemMetadata();
    Map<String, String> properties = new HashMap<>();
    properties.put(TraceContext.TELEMETRY_TRACE_KEY, TRACE_ID);
    systemMetadata.setProperties(new StringMap(properties));

    MetadataChangeProposal mcp = buildMessage(systemMetadata);

    GenericRecord genericRecord = toGenericRecord(mcp);
    when(mockConsumerRecord.value()).thenReturn(genericRecord);

    Optional<Pair<ConsumerRecord<String, GenericRecord>, SystemMetadata>> result =
        traceReader.matchConsumerRecord(mockConsumerRecord, TRACE_ID, ASPECT_NAME);

    assertTrue(result.isPresent());
    assertEquals(result.get().getFirst(), mockConsumerRecord);
    assertEquals(result.get().getSecond(), systemMetadata);
  }
}
