package com.linkedin.metadata.aspect.patch.template;

import static com.linkedin.metadata.aspect.patch.template.TemplateUtil.populateTopLevelKeys;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jsonpatch.JsonPatchException;
import com.github.fge.jsonpatch.Patch;
import com.linkedin.data.template.RecordTemplate;

public abstract class CompoundKeyTemplate<T extends RecordTemplate>
    implements ArrayMergingTemplate<T> {

  @Override
  public T applyPatch(RecordTemplate recordTemplate, Patch jsonPatch)
      throws JsonProcessingException, JsonPatchException {
    JsonNode transformed = populateTopLevelKeys(preprocessTemplate(recordTemplate), jsonPatch);
    JsonNode patched = jsonPatch.apply(transformed);
    JsonNode postProcessed = rebaseFields(patched);
    return RecordUtils.toRecordTemplate(getTemplateType(), postProcessed.toString());
  }
}
