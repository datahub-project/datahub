package com.linkedin.gms.factory.kafka.trace;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TraceKafkaConsumerPoolFactoryTest {

  private TraceKafkaConsumerPoolFactory factory;
  private DefaultKafkaConsumerFactory<String, GenericRecord> baseConsumerFactory;

  @BeforeMethod
  public void setup() {
    factory = new TraceKafkaConsumerPoolFactory();
    ReflectionTestUtils.setField(factory, "poolEnabled", true);
    ReflectionTestUtils.setField(factory, "initialPoolSize", 1);
    ReflectionTestUtils.setField(factory, "maxPoolSize", 2);
    ReflectionTestUtils.setField(factory, "borrowTimeoutMs", 1000L);
    ReflectionTestUtils.setField(factory, "validationTimeoutSeconds", 5);
    ReflectionTestUtils.setField(factory, "validationCacheIntervalMinutes", 5);
    ReflectionTestUtils.setField(factory, "leaveGroupOnClose", false);
    ReflectionTestUtils.setField(factory, "mcpGroupId", "trace-reader-mcp");
    ReflectionTestUtils.setField(factory, "mcpFailedGroupId", "trace-reader-mcp-failed");
    ReflectionTestUtils.setField(factory, "mclVersionedGroupId", "trace-reader-mcl-versioned");
    ReflectionTestUtils.setField(factory, "mclTimeseriesGroupId", "trace-reader-mcl-timeseries");

    baseConsumerFactory = mock(DefaultKafkaConsumerFactory.class);
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    when(baseConsumerFactory.getConfigurationProperties()).thenReturn(props);
    when(baseConsumerFactory.getKeyDeserializer()).thenReturn(null);
    when(baseConsumerFactory.getValueDeserializer()).thenReturn(null);
  }

  @Test
  public void testCreateTraceConsumerFactory_UsesStableGroupId() {
    DefaultKafkaConsumerFactory<String, GenericRecord> traceConsumerFactory =
        factory.createTraceConsumerFactory(baseConsumerFactory, "trace-reader-mcp");

    assertEquals(
        traceConsumerFactory.getConfigurationProperties().get(ConsumerConfig.GROUP_ID_CONFIG),
        "trace-reader-mcp");
    assertEquals(
        traceConsumerFactory
            .getConfigurationProperties()
            .get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG),
        "false");
  }

  @Test
  public void testGroupIdsConfiguredPerReaderType() {
    assertEquals(factory.getGroupIdForPoolType("mcp"), "trace-reader-mcp");
    assertEquals(factory.getGroupIdForPoolType("mcpFailed"), "trace-reader-mcp-failed");
    assertEquals(factory.getGroupIdForPoolType("mclVersioned"), "trace-reader-mcl-versioned");
    assertEquals(factory.getGroupIdForPoolType("mclTimeseries"), "trace-reader-mcl-timeseries");
  }
}
