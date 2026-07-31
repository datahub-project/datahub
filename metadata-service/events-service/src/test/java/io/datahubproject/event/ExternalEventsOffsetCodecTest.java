package io.datahubproject.event;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.testng.annotations.Test;

public class ExternalEventsOffsetCodecTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void testRoundTrip() throws Exception {
    Map<TopicPartition, Long> original =
        Map.of(
            new TopicPartition("MetadataChangeLog_Versioned_v1", 0), 100L,
            new TopicPartition("MetadataChangeLog_Versioned_v1", 1), 200L);

    String encoded = ExternalEventsOffsetCodec.encodeOffsetId(original, MAPPER);
    assertNotNull(encoded);

    Map<TopicPartition, Long> decoded = ExternalEventsOffsetCodec.decodeOffsetId(encoded, MAPPER);
    assertEquals(decoded, original);
  }

  @Test
  public void testHyphenatedTopicNameRoundTrip() throws Exception {
    Map<TopicPartition, Long> original = Map.of(new TopicPartition("my-hyphenated-topic", 3), 42L);

    String encoded = ExternalEventsOffsetCodec.encodeOffsetId(original, MAPPER);
    Map<TopicPartition, Long> decoded = ExternalEventsOffsetCodec.decodeOffsetId(encoded, MAPPER);

    assertEquals(decoded.size(), 1);
    TopicPartition tp = decoded.keySet().iterator().next();
    assertEquals(tp.topic(), "my-hyphenated-topic");
    assertEquals(tp.partition(), 3);
    assertEquals(decoded.get(tp).longValue(), 42L);
  }

  @Test
  public void testDecodeInvalidFormatNoHyphen() throws Exception {
    String json = MAPPER.writeValueAsString(Map.of("topicnohyphen", 10L));
    String encoded =
        java.util.Base64.getEncoder()
            .encodeToString(json.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    try {
      ExternalEventsOffsetCodec.decodeOffsetId(encoded, MAPPER);
      fail("Expected exception for key without hyphen");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Failed to decode"));
    }
  }

  @Test
  public void testDecodeInvalidFormatTrailingHyphen() throws Exception {
    String json = MAPPER.writeValueAsString(Map.of("topic-", 10L));
    String encoded =
        java.util.Base64.getEncoder()
            .encodeToString(json.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    try {
      ExternalEventsOffsetCodec.decodeOffsetId(encoded, MAPPER);
      fail("Expected exception for trailing hyphen");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Failed to decode"));
    }
  }

  @Test
  public void testDecodeInvalidPartitionNonNumeric() throws Exception {
    String json = MAPPER.writeValueAsString(Map.of("topic-abc", 10L));
    String encoded =
        java.util.Base64.getEncoder()
            .encodeToString(json.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    try {
      ExternalEventsOffsetCodec.decodeOffsetId(encoded, MAPPER);
      fail("Expected exception for non-numeric partition");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Failed to decode"));
    }
  }

  @Test
  public void testMultipleTopics() throws Exception {
    Map<TopicPartition, Long> original =
        Map.of(
            new TopicPartition("topic-a", 0), 10L,
            new TopicPartition("topic-b", 1), 20L);

    String encoded = ExternalEventsOffsetCodec.encodeOffsetId(original, MAPPER);
    Map<TopicPartition, Long> decoded = ExternalEventsOffsetCodec.decodeOffsetId(encoded, MAPPER);
    assertEquals(decoded, original);
  }
}
