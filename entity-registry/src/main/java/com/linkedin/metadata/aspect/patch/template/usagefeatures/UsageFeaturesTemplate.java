package com.linkedin.metadata.aspect.patch.template.usagefeatures;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.patch.template.Template;
import com.linkedin.metadata.search.features.UsageFeatures;
import javax.annotation.Nonnull;

public class UsageFeaturesTemplate implements Template<UsageFeatures> {

  @Override
  public UsageFeatures getSubtype(RecordTemplate recordTemplate) throws ClassCastException {
    if (recordTemplate instanceof UsageFeatures) {
      return (UsageFeatures) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to UsageFeatures");
  }

  @Override
  public Class<UsageFeatures> getTemplateType() {
    return UsageFeatures.class;
  }

  @Nonnull
  @Override
  public UsageFeatures getDefault() {
    return new UsageFeatures();
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
