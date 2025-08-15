package com.linkedin.metadata.aspect.patch.template.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.Siblings;
import com.linkedin.common.UrnArray;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.patch.template.ArrayMergingTemplate;
import java.util.Collections;
import javax.annotation.Nonnull;

/**
 * Template for applying JSON patches to the Siblings aspect.
 *
 * <p>This template handles the merging logic for sibling relationships, allowing additive updates
 * to the siblings list while preserving existing relationships. It supports both primary
 * designation and sibling list management.
 *
 * <p>Used by the DataHub GMS backend when processing PATCH operations on the siblings aspect,
 * typically from dbt ingestion with dbt_is_primary_sibling=false.
 */
public class SiblingsTemplate implements ArrayMergingTemplate<Siblings> {

  private static final String SIBLINGS_FIELD_NAME = "siblings";

  @Override
  public Siblings getSubtype(RecordTemplate recordTemplate) throws ClassCastException {
    if (recordTemplate instanceof Siblings) {
      return (Siblings) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to Siblings");
  }

  @Override
  public Class<Siblings> getTemplateType() {
    return Siblings.class;
  }

  @Nonnull
  @Override
  public Siblings getDefault() {
    Siblings siblings = new Siblings();
    siblings.setSiblings(new UrnArray());
    siblings.setPrimary(false); // Default to false
    return siblings;
  }

  @Nonnull
  @Override
  public JsonNode transformFields(JsonNode baseNode) {
    // Siblings field is a simple array of URNs, so we use empty key fields list
    return arrayFieldToMap(baseNode, SIBLINGS_FIELD_NAME, Collections.emptyList());
  }

  @Nonnull
  @Override
  public JsonNode rebaseFields(JsonNode patched) {
    // Transform the map back to an array of URNs
    return transformedMapToArray(patched, SIBLINGS_FIELD_NAME, Collections.emptyList());
  }
}
