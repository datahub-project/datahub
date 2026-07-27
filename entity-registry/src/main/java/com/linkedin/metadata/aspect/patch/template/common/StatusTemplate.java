package com.linkedin.metadata.aspect.patch.template.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.Status;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.patch.template.Template;
import javax.annotation.Nonnull;

/**
 * Patch template for the {@link Status} aspect.
 *
 * <p>Status is a flat record with no arrays, so transformFields / rebaseFields are identity
 * transforms. Registering a template enables JSON Patch support (changeType=PATCH) for this aspect,
 * allowing callers to set individual fields (e.g. {@code /removed}) without overwriting the entire
 * aspect (which would wipe lifecycleStage, lifecycleState, lifecycleLastUpdated).
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
    status.setRemoved(false);
    return status;
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
