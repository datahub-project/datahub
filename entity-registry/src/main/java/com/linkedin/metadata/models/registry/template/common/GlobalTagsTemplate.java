package com.linkedin.metadata.models.registry.template.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.registry.template.ArrayMergingTemplate;
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
