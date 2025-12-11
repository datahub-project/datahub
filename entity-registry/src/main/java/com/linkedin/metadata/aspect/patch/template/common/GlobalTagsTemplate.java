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
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.patch.template.ArrayMergingTemplate;
import java.util.Collections;
import javax.annotation.Nonnull;

public class GlobalTagsTemplate implements ArrayMergingTemplate<GlobalTags> {

  private static final String TAGS_FIELD_NAME = "tags";
  private static final String TAG_FIELD_NAME = "tag";

  @Override
  public GlobalTags getSubtype(RecordTemplate recordTemplate) throws ClassCastException {
    if (recordTemplate instanceof GlobalTags) {
      return (GlobalTags) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to GlobalTags");
  }

  @Override
  public Class<GlobalTags> getTemplateType() {
    return GlobalTags.class;
  }

  @Nonnull
  @Override
  public GlobalTags getDefault() {
    GlobalTags globalTags = new GlobalTags();
    globalTags.setTags(new TagAssociationArray());

    return globalTags;
  }

  @Nonnull
  @Override
  public JsonNode transformFields(JsonNode baseNode) {
    return arrayFieldToMap(baseNode, TAGS_FIELD_NAME, Collections.singletonList(TAG_FIELD_NAME));
  }

  @Nonnull
  @Override
  public JsonNode rebaseFields(JsonNode patched) {
    return transformedMapToArray(
        patched, TAGS_FIELD_NAME, Collections.singletonList(TAG_FIELD_NAME));
  }
}
