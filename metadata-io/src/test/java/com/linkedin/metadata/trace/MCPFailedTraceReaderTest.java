package com.linkedin.metadata.trace;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.EventUtils;
import com.linkedin.mxe.FailedMetadataChangeProposal;
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

public class MCPFailedTraceReaderTest
    extends BaseKafkaTraceReaderTest<FailedMetadataChangeProposal> {
  @Override
  KafkaTraceReader<FailedMetadataChangeProposal> buildTraceReader() {
    return MCPFailedTraceReader.builder()
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
  FailedMetadataChangeProposal buildMessage(@Nullable SystemMetadata systemMetadata) {
    return new FailedMetadataChangeProposal()
        .setError("Test failure error")
        .setMetadataChangeProposal(
            new MetadataChangeProposal()
                .setAspectName(ASPECT_NAME)
                .setEntityType(TEST_URN.getEntityType())
                .setChangeType(ChangeType.UPSERT)
                .setEntityUrn(TEST_URN)
                .setSystemMetadata(systemMetadata, SetMode.IGNORE_NULL));
  }

  @Override
  GenericRecord toGenericRecord(FailedMetadataChangeProposal message) throws IOException {
    return EventUtils.pegasusToAvroFailedMCP(message);
  }

  @Override
  FailedMetadataChangeProposal fromGenericRecord(GenericRecord genericRecord) throws IOException {
    return EventUtils.avroToPegasusFailedMCP(genericRecord);
  }

  @Test
  public void testFailedMCPRead() throws Exception {
    FailedMetadataChangeProposal expectedMCP = buildMessage(null);

    GenericRecord genericRecord = toGenericRecord(expectedMCP);

    Optional<FailedMetadataChangeProposal> result = traceReader.read(genericRecord);

    assertTrue(result.isPresent());
    assertEquals(result.get().getMetadataChangeProposal().getAspectName(), ASPECT_NAME);
  }

  @Test
  public void testFailedMCPMatchConsumerRecord() throws Exception {
    ConsumerRecord<String, GenericRecord> mockConsumerRecord = mock(ConsumerRecord.class);

    SystemMetadata systemMetadata = new SystemMetadata();
    Map<String, String> properties = new HashMap<>();
    properties.put(TraceContext.TELEMETRY_TRACE_KEY, TRACE_ID);
    systemMetadata.setProperties(new StringMap(properties));

    FailedMetadataChangeProposal fmcp = buildMessage(systemMetadata);

    GenericRecord genericRecord = toGenericRecord(fmcp);
    when(mockConsumerRecord.value()).thenReturn(genericRecord);

    Optional<Pair<ConsumerRecord<String, GenericRecord>, SystemMetadata>> result =
        traceReader.matchConsumerRecord(mockConsumerRecord, TRACE_ID, ASPECT_NAME);

    assertTrue(result.isPresent());
    assertEquals(result.get().getFirst(), mockConsumerRecord);
    assertEquals(result.get().getSecond(), systemMetadata);
  }
}
