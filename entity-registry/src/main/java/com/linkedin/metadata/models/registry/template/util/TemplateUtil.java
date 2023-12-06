package com.linkedin.metadata.models.registry.template.util;

import static com.linkedin.metadata.Constants.*;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.Patch;
import java.util.ArrayList;
import java.util.List;

public class TemplateUtil {

  private TemplateUtil() {}

  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static {
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    OBJECT_MAPPER
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());
  }

  public static List<String> getPaths(Patch jsonPatch) {
    JsonNode patchNode = OBJECT_MAPPER.valueToTree(jsonPatch);
    List<String> paths = new ArrayList<>();
    patchNode
        .elements()
        .forEachRemaining(
            node -> {
              paths.add(node.get("path").asText());
            });
    return paths;
  }
}
