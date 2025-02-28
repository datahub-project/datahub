package com.linkedin.metadata.aspect.patch.template.glossary;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.metadata.aspect.patch.template.Template;
import javax.annotation.Nonnull;

public class GlossaryNodeInfoTemplate implements Template<GlossaryNodeInfo> {

  @Override
  public GlossaryNodeInfo getSubtype(RecordTemplate recordTemplate) throws ClassCastException {
    if (recordTemplate instanceof GlossaryNodeInfo) {
      return (GlossaryNodeInfo) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to GlossaryNodeInfo");
  }

  @Override
  public Class<GlossaryNodeInfo> getTemplateType() {
    return GlossaryNodeInfo.class;
  }

  @Nonnull
  @Override
  public GlossaryNodeInfo getDefault() {
    return new GlossaryNodeInfo();
  }

  @Nonnull
  @Override
  public JsonNode transformFields(JsonNode baseNode) {
    return baseNode;
  }

  @Nonnull
  @Override
  public JsonNode rebaseFields(JsonNode patched) {
    return patched;
  }
}
