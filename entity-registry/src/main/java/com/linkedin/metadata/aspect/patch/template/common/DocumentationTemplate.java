package com.linkedin.metadata.aspect.patch.template.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.Documentation;
import com.linkedin.common.DocumentationAssociationArray;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.patch.template.ArrayMergingTemplate;
import java.util.List;
import javax.annotation.Nonnull;

public class DocumentationTemplate implements ArrayMergingTemplate<Documentation> {

  private static final String DOCUMENTATIONS_FIELD_NAME = "documentations";
  private static final String ATTRIBUTION_SOURCE =
      "attribution" + UNIT_SEPARATOR_DELIMITER + "source";

  @Override
  public Documentation getSubtype(RecordTemplate recordTemplate) throws ClassCastException {
    if (recordTemplate instanceof Documentation) {
      return (Documentation) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to Documentation");
  }

  @Override
  public Class<Documentation> getTemplateType() {
    return Documentation.class;
  }

  @Nonnull
  @Override
  public Documentation getDefault() {
    return new Documentation().setDocumentations(new DocumentationAssociationArray());
  }

  @Nonnull
  @Override
  public JsonNode transformFields(JsonNode baseNode) {
    return arrayFieldToMap(baseNode, DOCUMENTATIONS_FIELD_NAME, List.of(ATTRIBUTION_SOURCE));
  }

  @Nonnull
  @Override
  public JsonNode rebaseFields(JsonNode patched) {
    return transformedMapToArray(patched, DOCUMENTATIONS_FIELD_NAME, List.of(ATTRIBUTION_SOURCE));
  }
}
