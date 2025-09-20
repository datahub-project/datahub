package com.linkedin.metadata.aspect.patch.template.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.Status;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.patch.template.Template;
import javax.annotation.Nonnull;

/**
 * Template for patching Status aspects.
 *
 * <p>Handles: removed (boolean field for soft deletion)
 */
public class StatusTemplate implements Template<Status> {

  @Override
  public Status getSubtype(RecordTemplate recordTemplate) throws ClassCastException {
    if (recordTemplate instanceof Status) {
      return (Status) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to Status");
  }

  @Override
  public Class<Status> getTemplateType() {
    return Status.class;
  }

  @Nonnull
  @Override
  public Status getDefault() {
    Status status = new Status();
    status.setRemoved(false); // Default to active (not removed)
    return status;
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
