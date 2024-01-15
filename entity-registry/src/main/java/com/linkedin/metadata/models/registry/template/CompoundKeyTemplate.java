package com.linkedin.metadata.models.registry.template;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.*;
import static com.linkedin.metadata.models.registry.template.util.TemplateUtil.*;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.fge.jsonpatch.JsonPatchException;
import com.github.fge.jsonpatch.Patch;
import com.linkedin.data.template.RecordTemplate;
import java.util.List;

public abstract class CompoundKeyTemplate<T extends RecordTemplate>
    implements ArrayMergingTemplate<T> {

  /**
   * Necessary step for templates with compound keys due to JsonPatch not allowing non-existent
   * paths to be specified
   *
   * @param transformedNode transformed node to have keys populated
   * @return transformed node that has top level keys populated
   */
  public JsonNode populateTopLevelKeys(JsonNode transformedNode, Patch jsonPatch) {
    JsonNode transformedNodeClone = transformedNode.deepCopy();
    List<String> paths = getPaths(jsonPatch);
    for (String path : paths) {
      String[] keys = path.split("/");
      // Skip first as it will always be blank due to path starting with /, skip last key as we only
      // need to populate top level
      JsonNode parent = transformedNodeClone;
      for (int i = 1; i < keys.length - 1; i++) {
        if (parent.get(keys[i]) == null) {
          ((ObjectNode) parent).set(keys[i], instance.objectNode());
        }
        parent = parent.get(keys[i]);
      }
    }

    return transformedNodeClone;
  }

  @Override
  public T applyPatch(RecordTemplate recordTemplate, Patch jsonPatch)
      throws JsonProcessingException, JsonPatchException {
    JsonNode transformed = populateTopLevelKeys(preprocessTemplate(recordTemplate), jsonPatch);
    JsonNode patched = jsonPatch.apply(transformed);
    JsonNode postProcessed = rebaseFields(patched);
    return RecordUtils.toRecordTemplate(getTemplateType(), postProcessed.toString());
  }
}
