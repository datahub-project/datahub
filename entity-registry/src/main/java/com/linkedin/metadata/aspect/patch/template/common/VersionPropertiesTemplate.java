package com.linkedin.metadata.aspect.patch.template.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.VersionProperties;
import com.linkedin.common.VersionTag;
import com.linkedin.common.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.patch.template.Template;
import javax.annotation.Nonnull;

public class VersionPropertiesTemplate implements Template<VersionProperties> {

  public static final String IS_LATEST_FIELD = "isLatest";

  @Override
  public VersionProperties getSubtype(RecordTemplate recordTemplate) throws ClassCastException {
    if (recordTemplate instanceof VersionProperties) {
      return (VersionProperties) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to VersionProperties");
  }

  @Override
  public Class<VersionProperties> getTemplateType() {
    return VersionProperties.class;
  }

  @Nonnull
  @Override
  public VersionProperties getDefault() {
    VersionProperties versionProperties = new VersionProperties();
    // Set default values for required fields
    versionProperties.setVersionSet(UrnUtils.getUrn("urn:li:versionSet:default"));
    versionProperties.setVersion(new VersionTag().setVersionTag("1.0.0"));
    versionProperties.setSortId("00000000");
    // versioningScheme has a default value in the PDL, so we don't need to set it explicitly
    return versionProperties;
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
