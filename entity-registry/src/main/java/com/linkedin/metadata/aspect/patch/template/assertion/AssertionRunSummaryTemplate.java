package com.linkedin.metadata.aspect.patch.template.assertion;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.assertion.AssertionRunSummary;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.patch.template.Template;
import javax.annotation.Nonnull;

/**
 * Patch template for the {@link AssertionRunSummary} aspect.
 *
 * <p>Registering a template is what allows the first PATCH for an assertion to be applied: without
 * a default, {@code PatchItemImpl.applyTemplatePatch} has no base value and rejects the patch, so
 * the summary could never be created.
 */
public class AssertionRunSummaryTemplate implements Template<AssertionRunSummary> {

  @Override
  public AssertionRunSummary getSubtype(RecordTemplate recordTemplate) throws ClassCastException {
    if (recordTemplate instanceof AssertionRunSummary) {
      return (AssertionRunSummary) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to AssertionRunSummary");
  }

  @Override
  public Class<AssertionRunSummary> getTemplateType() {
    return AssertionRunSummary.class;
  }

  @Nonnull
  @Override
  public AssertionRunSummary getDefault() {
    // Every field is optional; the hook patches individual fields onto this empty base.
    return new AssertionRunSummary();
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
