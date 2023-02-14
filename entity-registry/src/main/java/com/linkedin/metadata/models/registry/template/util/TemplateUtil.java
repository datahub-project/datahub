package com.linkedin.metadata.models.registry.template.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.Patch;
import java.util.ArrayList;
import java.util.List;


public class TemplateUtil {

  private TemplateUtil() {

  }

  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static List<String> getPaths(Patch jsonPatch) {
    JsonNode patchNode = new ObjectMapper().valueToTree(jsonPatch);
    List<String> paths = new ArrayList<>();
    patchNode.elements().forEachRemaining(node -> {
      paths.add(node.get("path").asText());
    });
    return paths;
  }
}
