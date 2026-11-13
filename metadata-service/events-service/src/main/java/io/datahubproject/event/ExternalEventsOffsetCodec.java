package io.datahubproject.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;

/**
 * Encodes/decodes External Events offset tokens (Kafka partition offsets or pgQueue enqueue_seq).
 */
public final class ExternalEventsOffsetCodec {

  private ExternalEventsOffsetCodec() {}

  public static String encodeOffsetId(
      final Map<TopicPartition, Long> offsets, final ObjectMapper objectMapper) throws Exception {
    Map<String, Long> serializedOffsets =
        offsets.entrySet().stream()
            .collect(
                Collectors.toMap(
                    entry -> entry.getKey().topic() + "-" + entry.getKey().partition(),
                    Map.Entry::getValue));

    String json = objectMapper.writeValueAsString(serializedOffsets);
    return Base64.getEncoder().encodeToString(json.getBytes());
  }

  public static Map<TopicPartition, Long> decodeOffsetId(
      final String offsetId, final ObjectMapper objectMapper) throws Exception {
    try {
      String json = new String(Base64.getDecoder().decode(offsetId));
      Map<String, Long> serializedOffsets =
          objectMapper.readValue(
              json,
              objectMapper.getTypeFactory().constructMapType(Map.class, String.class, Long.class));

      Map<TopicPartition, Long> out = new HashMap<>();
      for (Map.Entry<String, Long> entry : serializedOffsets.entrySet()) {
        String key = entry.getKey();
        int lastHyphenIndex = key.lastIndexOf('-');

        if (lastHyphenIndex == -1 || lastHyphenIndex == key.length() - 1) {
          throw new IllegalArgumentException("Invalid offset format: " + key);
        }

        String topic = key.substring(0, lastHyphenIndex);
        String partitionStr = key.substring(lastHyphenIndex + 1);

        if (!partitionStr.matches("\\d+")) {
          throw new IllegalArgumentException("Invalid partition number in offset: " + key);
        }

        out.put(new TopicPartition(topic, Integer.parseInt(partitionStr)), entry.getValue());
      }
      return out;
    } catch (IllegalArgumentException | IOException e) {
      throw new Exception("Failed to decode and parse offsetId: " + offsetId, e);
    }
  }
}
