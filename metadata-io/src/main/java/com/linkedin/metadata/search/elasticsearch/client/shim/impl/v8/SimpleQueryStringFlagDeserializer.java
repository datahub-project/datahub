package com.linkedin.metadata.search.elasticsearch.client.shim.impl.v8;

import co.elastic.clients.elasticsearch._types.query_dsl.SimpleQueryStringFlag;
import co.elastic.clients.json.JsonpDeserializerBase;
import co.elastic.clients.json.JsonpMapper;
import jakarta.json.stream.JsonParser;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class SimpleQueryStringFlagDeserializer
    extends JsonpDeserializerBase<List<SimpleQueryStringFlag>> {
  public SimpleQueryStringFlagDeserializer() {
    super(EnumSet.of(JsonParser.Event.VALUE_STRING, JsonParser.Event.VALUE_NUMBER));
  }

  @Override
  public List<SimpleQueryStringFlag> deserialize(
      JsonParser parser, JsonpMapper mapper, JsonParser.Event event) {
    if (JsonParser.Event.VALUE_STRING == event) {
      // Handle if we pass in a piped string later on
      return Optional.ofNullable(parser.getString().split("\\|"))
          .map(Arrays::asList)
          .orElse(Collections.emptyList())
          .stream()
          .map(flag -> SimpleQueryStringFlag._DESERIALIZER.deserialize(flag.trim(), parser))
          .collect(Collectors.toList());
    } else { // (JsonParser.Event.VALUE_NUMBER == event)
      return flagsToString(parser.getInt(), parser);
    }
  }

  /**
   * Converts a resolved flag integer back to its string representation. This is the reverse
   * operation of SimpleQueryStringFlag.resolveFlags().
   *
   * @param resolvedFlags The integer representing OR'd flag values
   * @return List of resolved SimpleQueryStringFlags
   */
  public static List<SimpleQueryStringFlag> flagsToString(int resolvedFlags, JsonParser parser) {
    if (resolvedFlags == org.opensearch.index.query.SimpleQueryStringFlag.ALL.value()) {
      return List.of(SimpleQueryStringFlag.All);
    }
    if (resolvedFlags == org.opensearch.index.query.SimpleQueryStringFlag.NONE.value()) {
      return List.of(SimpleQueryStringFlag.None);
    }

    List<SimpleQueryStringFlag> activeFlags = new ArrayList<>();

    // Check each flag (except ALL and NONE) to see if it's set in the resolved value
    for (org.opensearch.index.query.SimpleQueryStringFlag flag :
        org.opensearch.index.query.SimpleQueryStringFlag.values()) {
      if (flag == org.opensearch.index.query.SimpleQueryStringFlag.ALL
          || flag == org.opensearch.index.query.SimpleQueryStringFlag.NONE) {
        continue;
      }

      // Handle NEAR/SLOP synonyms - prefer SLOP
      if (flag == org.opensearch.index.query.SimpleQueryStringFlag.NEAR) {
        continue;
      }

      // Check if this flag's bits are set in the resolved value
      if ((resolvedFlags & flag.value()) == flag.value() && flag.value() != 0) {
        activeFlags.add(convertFlag(flag, parser));
      }
    }

    // If no flags were found, something went wrong
    if (activeFlags.isEmpty()) {
      return List.of(SimpleQueryStringFlag.All);
    }

    return activeFlags;
  }

  private static SimpleQueryStringFlag convertFlag(
      org.opensearch.index.query.SimpleQueryStringFlag osFlag, JsonParser parser) {
    return SimpleQueryStringFlag._DESERIALIZER.deserialize(osFlag.name(), parser);
  }
}
