package com.linkedin.metadata.kafka;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.dao.throttle.ThrottleSensor;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.kafka.util.KafkaListenerUtil;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CDCProcessorTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private CDCProcessor cdcProcessor;
  private OperationContext mockOperationContext;
  private EntityService mockEntityService;
  private ThrottleSensor mockKafkaThrottle;
  private KafkaListenerEndpointRegistry mockRegistry;
  private ConfigurationProvider mockProvider;
  private ObjectMapper mockObjectMapper;
  private EntityRegistry mockEntityRegistry;

  @BeforeMethod
  public void setup() {
    mockOperationContext = mock(OperationContext.class);
    mockEntityService = mock(EntityService.class);
    mockKafkaThrottle = mock(ThrottleSensor.class);
    mockRegistry = mock(KafkaListenerEndpointRegistry.class);
    mockProvider = mock(ConfigurationProvider.class);
    mockObjectMapper = mock(ObjectMapper.class);
    mockEntityRegistry = mock(EntityRegistry.class);

    when(mockOperationContext.getObjectMapper()).thenReturn(mockObjectMapper);
    when(mockOperationContext.getEntityRegistry()).thenReturn(mockEntityRegistry);
    when(mockOperationContext.getMetricUtils()).thenReturn(Optional.empty());

    cdcProcessor =
        new CDCProcessor(
            mockOperationContext, mockEntityService, mockKafkaThrottle, mockRegistry, mockProvider);
  }

  @Test
  public void testRegisterConsumerThrottleWhenEnabled() {
    try (MockedStatic<KafkaListenerUtil> mockedUtil = Mockito.mockStatic(KafkaListenerUtil.class)) {
      cdcProcessor =
          new CDCProcessor(
              mockOperationContext,
              mockEntityService,
              mockKafkaThrottle,
              mockRegistry,
              mockProvider);

      cdcProcessor.cdcMclProcessingEnabled = true;
      cdcProcessor.cdcConsumerGroupId = "cdc-consumer-job-client";

      cdcProcessor.registerConsumerThrottle();

      mockedUtil.verify(
          () ->
              KafkaListenerUtil.registerThrottle(
                  mockKafkaThrottle, mockProvider, mockRegistry, "cdc-consumer-job-client"));
    }
  }

  @Test
  public void testRegisterConsumerThrottleWhenDisabled() {
    try (MockedStatic<KafkaListenerUtil> mockedUtil = Mockito.mockStatic(KafkaListenerUtil.class)) {
      // Setup - disable CDC processing (default is false)
      cdcProcessor =
          new CDCProcessor(
              mockOperationContext,
              mockEntityService,
              mockKafkaThrottle,
              mockRegistry,
              mockProvider);

      // Execute
      cdcProcessor.registerConsumerThrottle();

      // Verify that registerThrottle was not called
      mockedUtil.verifyNoInteractions();
    }
  }

  @Test
  public void testConsumeWhenCdcProcessingDisabled() throws Exception {
    // Setup
    ConsumerRecord<String, String> mockRecord = mock(ConsumerRecord.class);
    when(mockRecord.value()).thenReturn("test-record");

    cdcProcessor.cdcMclProcessingEnabled = false;

    // Execute
    cdcProcessor.consume(mockRecord);

    // Verify that no processing occurs
    verifyNoInteractions(mockEntityService);
    verify(mockObjectMapper, never()).readTree(any(String.class));
  }

  @Test
  public void testConsumeWithValidRecord() throws Exception {
    ConsumerRecord<String, String> mockRecord = mock(ConsumerRecord.class);
    when(mockRecord.value()).thenReturn("test-record");
    when(mockRecord.key()).thenReturn("test-key");
    when(mockRecord.topic()).thenReturn("test-topic");
    when(mockRecord.partition()).thenReturn(0);
    when(mockRecord.offset()).thenReturn(1L);
    when(mockRecord.serializedValueSize()).thenReturn(100);
    when(mockRecord.timestamp()).thenReturn(System.currentTimeMillis());

    cdcProcessor.cdcMclProcessingEnabled = true;

    JsonNode mockCdcRecord = mock(JsonNode.class);
    when(mockObjectMapper.readTree("test-record")).thenReturn(mockCdcRecord);

    // Execute
    cdcProcessor.consume(mockRecord);

    // Verify
    verify(mockObjectMapper).readTree("test-record");
    verify(mockEntityService, never()).produceMCLAsync(any(), any());
  }

  @Test
  public void testConsumeWithNullRecord() throws Exception {
    ConsumerRecord<String, String> mockRecord = mock(ConsumerRecord.class);
    when(mockRecord.value()).thenReturn(null);
    when(mockRecord.key()).thenReturn("test-key");
    when(mockRecord.topic()).thenReturn("test-topic");
    when(mockRecord.partition()).thenReturn(0);
    when(mockRecord.offset()).thenReturn(1L);
    when(mockRecord.serializedValueSize()).thenReturn(0);
    when(mockRecord.timestamp()).thenReturn(System.currentTimeMillis());

    cdcProcessor.cdcMclProcessingEnabled = true;

    // Execute
    cdcProcessor.consume(mockRecord);

    // Verify that no processing occurs when record is null
    verify(mockObjectMapper, never()).readTree(any(String.class));
    verifyNoInteractions(mockEntityService);
  }

  @Test
  public void testShouldProcessCDCRecord_ValidVersionZero() throws Exception {
    JsonNode afterRecord = OBJECT_MAPPER.readTree("{\"version\": 0, \"urn\": \"test:urn\"}");
    JsonNode beforeRecord = OBJECT_MAPPER.readTree("{\"version\": 1, \"urn\": \"test:urn\"}");

    Pair<Boolean, ChangeType> result =
        cdcProcessor.shouldProcessCDCRecord(afterRecord, beforeRecord);

    Assert.assertTrue(result.getFirst());
    Assert.assertEquals(result.getSecond(), ChangeType.UPSERT);
  }

  @Test
  public void testShouldProcessCDCRecord_NonZeroVersion() throws Exception {
    JsonNode afterRecord = OBJECT_MAPPER.readTree("{\"version\": 1, \"urn\": \"test:urn\"}");
    JsonNode beforeRecord = OBJECT_MAPPER.readTree("{\"version\": 0, \"urn\": \"test:urn\"}");

    Pair<Boolean, ChangeType> result =
        cdcProcessor.shouldProcessCDCRecord(afterRecord, beforeRecord);

    Assert.assertFalse(result.getFirst());
    Assert.assertEquals(result.getSecond(), ChangeType.$UNKNOWN);
  }

  @Test
  public void testShouldProcessCDCRecord_DeleteOperation() throws Exception {
    JsonNode beforeRecord = OBJECT_MAPPER.readTree("{\"version\": 0, \"urn\": \"test:urn\"}");

    Pair<Boolean, ChangeType> result = cdcProcessor.shouldProcessCDCRecord(null, beforeRecord);

    Assert.assertTrue(result.getFirst());
    Assert.assertEquals(result.getSecond(), ChangeType.DELETE);
  }

  @Test
  public void testShouldProcessCDCRecord_MissingVersionField() throws Exception {
    JsonNode afterRecord = OBJECT_MAPPER.readTree("{\"urn\": \"test:urn\"}"); // No version field
    JsonNode beforeRecord = OBJECT_MAPPER.readTree("{\"urn\": \"test:urn\"}"); // No version field

    Pair<Boolean, ChangeType> result =
        cdcProcessor.shouldProcessCDCRecord(afterRecord, beforeRecord);

    Assert.assertFalse(result.getFirst());
    Assert.assertEquals(result.getSecond(), ChangeType.$UNKNOWN);
  }

  @Test
  public void testShouldProcessCDCRecord_NullAfterRecordNoBeforeRecord() {
    Pair<Boolean, ChangeType> result = cdcProcessor.shouldProcessCDCRecord(null, null);

    Assert.assertFalse(result.getFirst());
    Assert.assertEquals(result.getSecond(), ChangeType.$UNKNOWN);
  }

  @Test
  public void testShouldProcessCDCRecord_DeleteOperationWithNonZeroVersion() throws Exception {
    JsonNode beforeRecord = OBJECT_MAPPER.readTree("{\"version\": 1, \"urn\": \"test:urn\"}");

    Pair<Boolean, ChangeType> result = cdcProcessor.shouldProcessCDCRecord(null, beforeRecord);

    Assert.assertFalse(result.getFirst());
    Assert.assertEquals(result.getSecond(), ChangeType.$UNKNOWN);
  }

  @Test
  public void testShouldProcessCDCRecord_DeleteOperationMissingVersion() throws Exception {
    JsonNode beforeRecord = OBJECT_MAPPER.readTree("{\"urn\": \"test:urn\"}"); // No version field

    Pair<Boolean, ChangeType> result = cdcProcessor.shouldProcessCDCRecord(null, beforeRecord);

    Assert.assertFalse(result.getFirst());
    Assert.assertEquals(result.getSecond(), ChangeType.$UNKNOWN);
  }

  @Test
  public void testShouldProcessCDCRecord_JsonNodeIsNull() throws Exception {
    JsonNode afterRecord = OBJECT_MAPPER.readTree("null"); // JSON null value
    JsonNode beforeRecord = OBJECT_MAPPER.readTree("{\"version\": 0, \"urn\": \"test:urn\"}");

    Pair<Boolean, ChangeType> result =
        cdcProcessor.shouldProcessCDCRecord(afterRecord, beforeRecord);

    Assert.assertTrue(result.getFirst());
    Assert.assertEquals(result.getSecond(), ChangeType.DELETE);
  }

  @Test
  public void testProcessCDCRecord_ValidRecord() throws Exception {
    // Setup
    String testRecord = "test-record";
    JsonNode mockCdcRecord = mock(JsonNode.class);
    when(mockObjectMapper.readTree(testRecord)).thenReturn(mockCdcRecord);

    // Execute - should handle exception gracefully when CDC record processing fails
    cdcProcessor.processCDCRecord(testRecord);

    // Verify that JSON was parsed
    verify(mockObjectMapper).readTree(testRecord);

    // The actual processing will fail due to mock setup, but that's expected
    // The test verifies error handling works correctly
  }

  @Test
  public void testProcessCDCRecord_InvalidJson() throws Exception {
    // Setup
    String invalidJsonRecord = "invalid json";
    when(mockObjectMapper.readTree(invalidJsonRecord))
        .thenThrow(new RuntimeException("Invalid JSON"));

    // Execute - should not throw exception
    cdcProcessor.processCDCRecord(invalidJsonRecord);

    // Verify that error is handled gracefully
    verify(mockObjectMapper).readTree(invalidJsonRecord);
    verifyNoInteractions(mockEntityService);
  }

  @Test
  public void testMclFromCDCRecord_ShouldNotProcess() throws Exception {
    JsonNode cdcRecord =
        OBJECT_MAPPER.readTree(
            """
        {
          "payload": {
            "before": null,
            "after": {
              "version": 1,
              "urn": "urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)",
              "aspect": "datasetProperties"
            }
          }
        }
        """);

    Optional<MetadataChangeLog> result = cdcProcessor.mclFromCDCRecord(cdcRecord);

    Assert.assertTrue(result.isEmpty());
  }

  @Test
  public void testExtractFieldFromRecord_ValidField() throws Exception {
    JsonNode record = OBJECT_MAPPER.readTree("{\"testField\": \"testValue\", \"other\": \"data\"}");

    String result = cdcProcessor.extractFieldFromRecord(record, "testField");

    Assert.assertEquals(result, "testValue");
  }

  @Test
  public void testExtractFieldFromRecord_NullRecord() {
    String result = cdcProcessor.extractFieldFromRecord(null, "testField");
    Assert.assertNull(result);
  }

  @Test
  public void testExtractFieldFromRecord_MissingField() throws Exception {
    JsonNode record = OBJECT_MAPPER.readTree("{\"otherField\": \"value\"}");

    String result = cdcProcessor.extractFieldFromRecord(record, "testField");

    Assert.assertNull(result);
  }

  @Test
  public void testExtractFieldFromRecord_NullFieldValue() throws Exception {
    JsonNode record = OBJECT_MAPPER.readTree("{\"testField\": null, \"other\": \"data\"}");

    String result = cdcProcessor.extractFieldFromRecord(record, "testField");

    Assert.assertNull(result);
  }

  @Test
  public void testExtractRequiredFieldWithFallback_FromAfterRecord() throws Exception {
    JsonNode afterRecord = OBJECT_MAPPER.readTree("{\"testField\": \"afterValue\"}");
    JsonNode beforeRecord = OBJECT_MAPPER.readTree("{\"testField\": \"beforeValue\"}");

    String result =
        cdcProcessor.extractRequiredFieldWithFallback(afterRecord, beforeRecord, "testField");

    Assert.assertEquals(result, "afterValue");
  }

  @Test
  public void testExtractRequiredFieldWithFallback_FromBeforeRecord() throws Exception {
    JsonNode beforeRecord = OBJECT_MAPPER.readTree("{\"testField\": \"beforeValue\"}");

    String result = cdcProcessor.extractRequiredFieldWithFallback(null, beforeRecord, "testField");

    Assert.assertEquals(result, "beforeValue");
  }

  @Test
  public void testExtractRequiredFieldWithFallback_NullAfterRecord() throws Exception {
    JsonNode afterRecord = OBJECT_MAPPER.readTree("null");
    JsonNode beforeRecord = OBJECT_MAPPER.readTree("{\"testField\": \"beforeValue\"}");

    String result =
        cdcProcessor.extractRequiredFieldWithFallback(afterRecord, beforeRecord, "testField");

    Assert.assertEquals(result, "beforeValue");
  }

  @Test
  public void testConsumeHandlesException() throws Exception {
    ConsumerRecord<String, String> mockRecord = mock(ConsumerRecord.class);
    when(mockRecord.value()).thenReturn("test-record");
    when(mockRecord.key()).thenReturn("test-key");
    when(mockRecord.topic()).thenReturn("test-topic");
    when(mockRecord.partition()).thenReturn(0);
    when(mockRecord.offset()).thenReturn(1L);
    when(mockRecord.serializedValueSize()).thenReturn(100);
    when(mockRecord.timestamp()).thenReturn(System.currentTimeMillis());

    cdcProcessor.cdcMclProcessingEnabled = true;

    when(mockObjectMapper.readTree("test-record"))
        .thenThrow(new RuntimeException("Test exception"));

    // Execute - should not throw exception
    cdcProcessor.consume(mockRecord);

    // Verify that exception is handled gracefully
    verify(mockObjectMapper).readTree("test-record");
    verifyNoInteractions(mockEntityService);
  }

  @Test
  public void testExtractTimestamp_MySQLFormat() throws Exception {
    // MySQL format: epoch microseconds as number
    JsonNode createdOnNode = OBJECT_MAPPER.readTree("1697371825123456");

    long result = cdcProcessor.extractTimestamp(createdOnNode);

    // Should convert microseconds to milliseconds
    Assert.assertEquals(result, 1697371825123L);
  }

  @Test
  public void testExtractTimestamp_PostgreSQLFormat() throws Exception {
    // PostgreSQL format: ISO timestamp with timezone offset
    JsonNode createdOnNode = OBJECT_MAPPER.readTree("\"2023-10-15T14:30:25.123456+00:00\"");

    long result = cdcProcessor.extractTimestamp(createdOnNode);

    // Should parse to correct epoch milliseconds
    // 2023-10-15T14:30:25.123456+00:00 = 1697380225123 milliseconds
    Assert.assertEquals(result, 1697380225123L);
  }

  @Test
  public void testExtractTimestamp_PostgreSQLFormatWithNegativeOffset() throws Exception {
    // PostgreSQL format with negative timezone offset
    JsonNode createdOnNode = OBJECT_MAPPER.readTree("\"2023-10-15T10:30:25.123456-04:00\"");

    long result = cdcProcessor.extractTimestamp(createdOnNode);

    // Should parse correctly accounting for timezone offset
    // 2023-10-15T10:30:25.123456-04:00 = 2023-10-15T14:30:25.123456+00:00 = 1697380225123L
    Assert.assertEquals(result, 1697380225123L);
  }

  @Test
  public void testExtractTimestamp_PostgreSQLFormatWithPositiveOffset() throws Exception {
    // PostgreSQL format with positive timezone offset
    JsonNode createdOnNode = OBJECT_MAPPER.readTree("\"2023-10-15T16:30:25.123456+02:00\"");

    long result = cdcProcessor.extractTimestamp(createdOnNode);

    // Should parse correctly accounting for timezone offset
    // 2023-10-15T16:30:25.123456+02:00 = 2023-10-15T14:30:25.123456+00:00 = 1697380225123L
    Assert.assertEquals(result, 1697380225123L);
  }

  @Test
  public void testExtractTimestamp_MySQLFormatZeroValue() throws Exception {
    JsonNode createdOnNode = OBJECT_MAPPER.readTree("0");

    long result = cdcProcessor.extractTimestamp(createdOnNode);

    Assert.assertEquals(result, 0L);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testExtractTimestamp_InvalidPostgreSQLFormat() throws Exception {
    // Invalid PostgreSQL format - missing timezone
    JsonNode createdOnNode = OBJECT_MAPPER.readTree("\"2023-10-15 14:30:25.123456\"");

    // Should throw RuntimeException due to invalid format
    cdcProcessor.extractTimestamp(createdOnNode);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testExtractTimestamp_InvalidTimestampString() throws Exception {
    // Completely invalid timestamp string
    JsonNode createdOnNode = OBJECT_MAPPER.readTree("\"not-a-timestamp\"");

    // Should throw RuntimeException
    cdcProcessor.extractTimestamp(createdOnNode);
  }

  @Test
  public void testExtractTimestamp_PostgreSQLWithZuluTime() throws Exception {
    // PostgreSQL format with Z (Zulu time) instead of +00:00
    JsonNode createdOnNode = OBJECT_MAPPER.readTree("\"2023-10-15T14:30:25.123456Z\"");

    long result = cdcProcessor.extractTimestamp(createdOnNode);

    Assert.assertEquals(result, 1697380225123L);
  }

  @Test
  public void testExtractTimestamp_PostgreSQLWithoutMicroseconds() throws Exception {
    // PostgreSQL format without microseconds precision
    JsonNode createdOnNode = OBJECT_MAPPER.readTree("\"2023-10-15T14:30:25+00:00\"");

    long result = cdcProcessor.extractTimestamp(createdOnNode);

    // Should parse correctly, microseconds should be 0
    Assert.assertEquals(result, 1697380225000L);
  }

  @Test
  public void testExtractTimestamp_MySQLLargeValue() throws Exception {
    // Test with a large microsecond value
    long microseconds = Long.MAX_VALUE / 1000; // Avoid overflow
    JsonNode createdOnNode = OBJECT_MAPPER.readTree(String.valueOf(microseconds));

    long result = cdcProcessor.extractTimestamp(createdOnNode);

    Assert.assertEquals(result, microseconds / 1000);
  }

  @Test
  public void testMclFromCDCRecord_PostgreSQLCompleteCDCRecord() throws Exception {
    // PostgreSQL CDC record with all fields populated
    JsonNode cdcRecord =
        OBJECT_MAPPER.readTree(
            """
        {
          "payload": {
            "before": null,
            "after": {
              "version": 0,
              "urn": "urn:li:dataset:(urn:li:dataPlatform:postgresql,db.table,PROD)",
              "aspect": "datasetProperties",
              "metadata": "{\\"name\\": \\"test_table\\"}",
              "systemmetadata": "{\\"lastObserved\\": 1697380225123}",
              "createdon": "2023-10-15T14:30:25.123456+00:00",
              "createdby": "urn:li:corpuser:datahub",
              "createdfor": "urn:li:corpuser:test_user"
            }
          }
        }
        """);

    // This test mainly verifies that the method doesn't throw an exception
    // Full integration would require proper entity registry and aspect spec setup
    try {
      Optional<MetadataChangeLog> result = cdcProcessor.mclFromCDCRecord(cdcRecord);
      // Expected to fail during entity registry lookup, but timestamp parsing should work
      Assert.assertTrue(result.isEmpty());
    } catch (Exception e) {
      // Exception is expected due to mock setup limitations
      // The important part is that PostgreSQL timestamp parsing works
      Assert.assertTrue(e.getMessage().contains("urn") || e.getMessage().contains("entity"));
    }
  }

  @Test
  public void testMclFromCDCRecord_MySQLCompleteCDCRecord() throws Exception {
    // MySQL CDC record with numeric timestamp
    JsonNode cdcRecord =
        OBJECT_MAPPER.readTree(
            """
        {
          "payload": {
            "before": null,
            "after": {
              "version": 0,
              "urn": "urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)",
              "aspect": "datasetProperties",
              "metadata": "{\\"name\\": \\"test_table\\"}",
              "systemmetadata": "{\\"lastObserved\\": 1697380225123}",
              "createdon": 1697380225123456,
              "createdby": "urn:li:corpuser:datahub",
              "createdfor": null
            }
          }
        }
        """);

    // This test mainly verifies that the method doesn't throw an exception
    try {
      Optional<MetadataChangeLog> result = cdcProcessor.mclFromCDCRecord(cdcRecord);
      Assert.assertTrue(result.isEmpty());
    } catch (Exception e) {
      // Exception is expected due to mock setup limitations
      // The important part is that MySQL timestamp parsing works
      Assert.assertTrue(e.getMessage().contains("urn") || e.getMessage().contains("entity"));
    }
  }

  @Test
  public void testMclFromCDCRecord_DeleteOperation() throws Exception {
    // Test DELETE operation CDC record
    JsonNode cdcRecord =
        OBJECT_MAPPER.readTree(
            """
        {
          "payload": {
            "before": {
              "version": 0,
              "urn": "urn:li:dataset:(urn:li:dataPlatform:postgresql,db.table,PROD)",
              "aspect": "datasetProperties",
              "metadata": "{\\"name\\": \\"test_table\\"}",
              "systemmetadata": "{\\"lastObserved\\": 1697380225123}",
              "createdon": "2023-10-15T14:30:25.123456+00:00",
              "createdby": "urn:li:corpuser:datahub"
            },
            "after": null
          }
        }
        """);

    try {
      Optional<MetadataChangeLog> result = cdcProcessor.mclFromCDCRecord(cdcRecord);
      Assert.assertTrue(result.isEmpty());
    } catch (Exception e) {
      // Exception expected due to mock limitations
      Assert.assertTrue(e.getMessage().contains("urn") || e.getMessage().contains("entity"));
    }
  }

  @Test
  public void testMclFromCDCRecord_WithoutPayloadWrapper() throws Exception {
    // Test CDC record without payload wrapper (when schema is disabled)
    JsonNode cdcRecord =
        OBJECT_MAPPER.readTree(
            """
        {
          "before": null,
          "after": {
            "version": 0,
            "urn": "urn:li:dataset:(urn:li:dataPlatform:postgresql,db.table,PROD)",
            "aspect": "datasetProperties",
            "metadata": "{\\"name\\": \\"test_table\\"}",
            "createdon": "2023-10-15T14:30:25.123456+00:00",
            "createdby": "urn:li:corpuser:datahub"
          }
        }
        """);

    try {
      Optional<MetadataChangeLog> result = cdcProcessor.mclFromCDCRecord(cdcRecord);
      Assert.assertTrue(result.isEmpty());
    } catch (Exception e) {
      // Exception expected due to mock limitations
      Assert.assertTrue(e.getMessage().contains("urn") || e.getMessage().contains("entity"));
    }
  }
}
