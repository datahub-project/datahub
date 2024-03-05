package com.linkedin.metadata.integration;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StreamingJsonChunkProcessor {

  private final JsonFactory _factory = new JsonFactory();
  private final ObjectMapper _mapper = new ObjectMapper(_factory);

  public void processJsonStream(
      InputStream jsonStream,
      Consumer<List<String>> headerProcessor,
      Consumer<List<String>> rowProcessor,
      Consumer<List<String>> errorProcessor)
      throws Exception {
    JsonParser parser = _factory.createParser(jsonStream);

    // Assuming the top-level JSON structure is an array
    if (parser.nextToken() != JsonToken.START_ARRAY) {
      log.error("Expected an array");
      throw new IllegalStateException("Expected an array");
    }

    while (parser.nextToken() != JsonToken.END_ARRAY) {
      JsonNode node = _mapper.readTree(parser);
      String type = node.get("type").asText();
      JsonNode valuesNode = node.get("values");

      if ("header".equals(type)) {
        headerProcessor.accept(nodeToListOfString(valuesNode));
      } else if ("data".equals(type)) {
        rowProcessor.accept(nodeToListOfString(valuesNode));
      } else if ("error".equals(type)) {
        errorProcessor.accept(nodeToListOfString(valuesNode));
      } else {
        log.error("Unknown type of row in response: {}", type);
      }
    }
  }

  private static List<String> nodeToListOfString(JsonNode jsonNode) {
    // Assuming json node is an array of strings
    List<String> strings = new ArrayList<>();
    jsonNode.forEach(value -> strings.add(value.isNull() ? null : value.asText()));
    return strings;
  }
}
