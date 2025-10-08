package com.linkedin.metadata.aspect.patch.template.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.aspect.patch.template.CompoundKeyTemplate;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.Builder;

@Builder
public class GenericPatchTemplate<T extends RecordTemplate> extends CompoundKeyTemplate<T> {

  @Nonnull private final GenericJsonPatch genericJsonPatch;
  @Nonnull private final Class<T> templateType;
  @Nonnull private final T templateDefault;

  @Nonnull
  @Override
  public Class<T> getTemplateType() {
    return templateType;
  }

  @Nonnull
  @Override
  public T getDefault() {
    return templateDefault;
  }

  @Nonnull
  @Override
  public JsonNode transformFields(final JsonNode baseNode) {
    JsonNode transformedNode = baseNode;
    for (Map.Entry<String, List<String>> composite :
        genericJsonPatch.getArrayPrimaryKeys().entrySet()) {
      transformedNode = arrayFieldToMap(transformedNode, composite.getKey(), composite.getValue());
    }
    return transformedNode;
  }

  @Nonnull
  @Override
  public JsonNode rebaseFields(JsonNode patched) {
    JsonNode transformedNode = patched;
    for (Map.Entry<String, List<String>> composite :
        genericJsonPatch.getArrayPrimaryKeys().entrySet()) {
      transformedNode =
          transformedMapToArray(transformedNode, composite.getKey(), composite.getValue());
    }
    return transformedNode;
  }

  public T applyPatch(RecordTemplate recordTemplate) throws IOException {
    T result = super.applyPatch(recordTemplate, genericJsonPatch.getJsonPatch());

    // Convert null strings to empty strings for glossary aspects (mimic createGlossaryTerm
    // behavior)
    if (result instanceof com.linkedin.glossary.GlossaryTermInfo) {
      com.linkedin.glossary.GlossaryTermInfo termInfo =
          (com.linkedin.glossary.GlossaryTermInfo) result;
      if (termInfo.hasDefinition() && termInfo.getDefinition() == null) {
        termInfo.setDefinition("");
      }
    } else if (result instanceof com.linkedin.glossary.GlossaryNodeInfo) {
      com.linkedin.glossary.GlossaryNodeInfo nodeInfo =
          (com.linkedin.glossary.GlossaryNodeInfo) result;
      if (nodeInfo.hasDefinition() && nodeInfo.getDefinition() == null) {
        nodeInfo.setDefinition("");
      }
    }

    return result;
  }
}
