package com.linkedin.metadata.aspect.patch.template.assertion;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.assertion.AssertionInferenceDetails;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.patch.template.Template;
import javax.annotation.Nonnull;

public class AssertionInferenceTemplate implements Template<AssertionInferenceDetails> {

  @Override
  public AssertionInferenceDetails getSubtype(RecordTemplate recordTemplate)
      throws ClassCastException {
    if (recordTemplate instanceof AssertionInferenceDetails) {
      return (AssertionInferenceDetails) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to AssertionInferenceDetails");
  }

  @Override
  public Class<AssertionInferenceDetails> getTemplateType() {
    return AssertionInferenceDetails.class;
  }

  @Nonnull
  @Override
  public AssertionInferenceDetails getDefault() {
    AssertionInferenceDetails assertionInferenceDetails = new AssertionInferenceDetails();
    assertionInferenceDetails.setGeneratedAt(System.currentTimeMillis());
    return assertionInferenceDetails;
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
