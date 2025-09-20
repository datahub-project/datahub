package com.linkedin.metadata.aspect.patch.template.glossary;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringMap;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.metadata.aspect.patch.template.Template;
import javax.annotation.Nonnull;

/**
 * Template for patching GlossaryNodeInfo aspects.
 * 
 * Handles: name, description (definition), parent_nodes, custom_properties
 */
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
    GlossaryNodeInfo glossaryNodeInfo = new GlossaryNodeInfo();
    // Set required fields with defaults
    glossaryNodeInfo.setDefinition(""); // Required field
    glossaryNodeInfo.setCustomProperties(new StringMap());
    
    return glossaryNodeInfo;
  }

  @Nonnull
  @Override
  public JsonNode transformFields(JsonNode baseNode) {
    // No array fields to transform, return as-is
    return baseNode;
  }

  @Nonnull
  @Override
  public JsonNode rebaseFields(JsonNode patched) {
    // No array fields to rebase, return as-is
    return patched;
  }
}
