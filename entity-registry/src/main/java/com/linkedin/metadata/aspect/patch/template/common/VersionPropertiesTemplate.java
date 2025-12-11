/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.aspect.patch.template.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.VersionProperties;
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
    throw new UnsupportedOperationException(
        "Unable to generate default version properties, no sensible default for " + "version set.");
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
