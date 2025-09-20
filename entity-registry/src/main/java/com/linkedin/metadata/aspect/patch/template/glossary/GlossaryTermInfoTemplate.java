package com.linkedin.metadata.aspect.patch.template.glossary;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringMap;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.metadata.aspect.patch.template.Template;
import javax.annotation.Nonnull;

/**
 * Template for patching GlossaryTermInfo aspects.
 * 
 * Handles: name, description (definition), term_source, source_ref, source_url, 
 * parent_nodes, custom_properties
 */
public class GlossaryTermInfoTemplate implements Template<GlossaryTermInfo> {

  @Override
  public GlossaryTermInfo getSubtype(RecordTemplate recordTemplate) throws ClassCastException {
    if (recordTemplate instanceof GlossaryTermInfo) {
      return (GlossaryTermInfo) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to GlossaryTermInfo");
  }

  @Override
  public Class<GlossaryTermInfo> getTemplateType() {
    return GlossaryTermInfo.class;
  }

  @Nonnull
  @Override
  public GlossaryTermInfo getDefault() {
    GlossaryTermInfo glossaryTermInfo = new GlossaryTermInfo();
    // Set required fields with defaults
    glossaryTermInfo.setDefinition(""); // Required field
    glossaryTermInfo.setTermSource("INTERNAL"); // Default value as per schema
    glossaryTermInfo.setCustomProperties(new StringMap());
    
    return glossaryTermInfo;
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
