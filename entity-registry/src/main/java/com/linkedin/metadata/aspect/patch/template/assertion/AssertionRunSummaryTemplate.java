package com.linkedin.metadata.aspect.patch.template.assertion;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.assertion.AssertionRunSummary;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.patch.template.CompoundKeyTemplate;
import javax.annotation.Nonnull;

public class AssertionRunSummaryTemplate extends CompoundKeyTemplate<AssertionRunSummary> {

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
