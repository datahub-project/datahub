package io.datahubproject.models.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.security.MessageDigest;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataHubGuidGenerator {
  private static final ObjectMapper objectMapper = new ObjectMapper();

  @SneakyThrows
  public static String dataHubGuid(Map<String, String> obj) {
    // Configure ObjectMapper for consistent serialization
    objectMapper.configure(
        com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

    // Convert map to JSON string with sorted keys
    String jsonKey = objectMapper.writeValueAsString(obj);

    // Generate MD5 hash
    MessageDigest md = MessageDigest.getInstance("MD5");
    byte[] hashBytes = md.digest(jsonKey.getBytes());

    // Convert byte array to hexadecimal string
    StringBuilder hexString = new StringBuilder();
    for (byte hashByte : hashBytes) {
      String hex = Integer.toHexString(0xff & hashByte);
      if (hex.length() == 1) {
        hexString.append('0');
      }
      hexString.append(hex);
    }

    if (log.isDebugEnabled()) {
      log.debug("DataHub Guid for {} is : {}", jsonKey, hexString);
    }
    return hexString.toString();
  }
}
