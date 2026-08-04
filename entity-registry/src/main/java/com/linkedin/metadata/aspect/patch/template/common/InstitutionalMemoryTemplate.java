package com.linkedin.metadata.aspect.patch.template.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.InstitutionalMemoryMetadataArray;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.patch.template.ArrayMergingTemplate;
import java.util.Collections;
import javax.annotation.Nonnull;

public class InstitutionalMemoryTemplate implements ArrayMergingTemplate<InstitutionalMemory> {

  private static final String ELEMENTS_FIELD_NAME = "elements";
  private static final String URL_FIELD_NAME = "url";

  @Override
  public InstitutionalMemory getSubtype(RecordTemplate recordTemplate) throws ClassCastException {
    if (recordTemplate instanceof InstitutionalMemory) {
      return (InstitutionalMemory) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to InstitutionalMemory");
  }

  @Override
  public Class<InstitutionalMemory> getTemplateType() {
    return InstitutionalMemory.class;
  }

  @Nonnull
  @Override
  public InstitutionalMemory getDefault() {
    InstitutionalMemory institutionalMemory = new InstitutionalMemory();
    institutionalMemory.setElements(new InstitutionalMemoryMetadataArray());

    return institutionalMemory;
  }

  @Nonnull
  @Override
  public JsonNode transformFields(JsonNode baseNode) {
    return arrayFieldToMap(
        baseNode, ELEMENTS_FIELD_NAME, Collections.singletonList(URL_FIELD_NAME));
  }

  @Nonnull
  @Override
  public JsonNode rebaseFields(JsonNode patched) {
    return transformedMapToArray(
        patched, ELEMENTS_FIELD_NAME, Collections.singletonList(URL_FIELD_NAME));
  }
}
