package com.linkedin.metadata.aspect.patch.template.glossary;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.glossary.GlossaryRelatedTerms;
import com.linkedin.metadata.aspect.patch.template.ArrayMergingTemplate;
import java.util.Collections;
import javax.annotation.Nonnull;

/**
 * Template for patching GlossaryRelatedTerms aspects.
 * 
 * Handles: isRelatedTerms, hasRelatedTerms, values, relatedTerms arrays
 */
public class GlossaryRelatedTermsTemplate implements ArrayMergingTemplate<GlossaryRelatedTerms> {

  private static final String IS_RELATED_TERMS_FIELD = "isRelatedTerms";
  private static final String HAS_RELATED_TERMS_FIELD = "hasRelatedTerms";
  private static final String VALUES_FIELD = "values";
  private static final String RELATED_TERMS_FIELD = "relatedTerms";

  @Override
  public GlossaryRelatedTerms getSubtype(RecordTemplate recordTemplate) throws ClassCastException {
    if (recordTemplate instanceof GlossaryRelatedTerms) {
      return (GlossaryRelatedTerms) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to GlossaryRelatedTerms");
  }

  @Override
  public Class<GlossaryRelatedTerms> getTemplateType() {
    return GlossaryRelatedTerms.class;
  }

  @Nonnull
  @Override
  public GlossaryRelatedTerms getDefault() {
    GlossaryRelatedTerms glossaryRelatedTerms = new GlossaryRelatedTerms();
    // All fields are optional arrays, so we can leave them null
    return glossaryRelatedTerms;
  }

  @Nonnull
  @Override
  public JsonNode transformFields(JsonNode baseNode) {
    JsonNode transformed = baseNode;
    
    // Transform array fields to maps for patching
    transformed = arrayFieldToMap(transformed, IS_RELATED_TERMS_FIELD, Collections.emptyList());
    transformed = arrayFieldToMap(transformed, HAS_RELATED_TERMS_FIELD, Collections.emptyList());
    transformed = arrayFieldToMap(transformed, VALUES_FIELD, Collections.emptyList());
    transformed = arrayFieldToMap(transformed, RELATED_TERMS_FIELD, Collections.emptyList());
    
    return transformed;
  }

  @Nonnull
  @Override
  public JsonNode rebaseFields(JsonNode patched) {
    JsonNode transformed = patched;
    
    // Rebase maps back to arrays
    transformed = transformedMapToArray(transformed, IS_RELATED_TERMS_FIELD, Collections.emptyList());
    transformed = transformedMapToArray(transformed, HAS_RELATED_TERMS_FIELD, Collections.emptyList());
    transformed = transformedMapToArray(transformed, VALUES_FIELD, Collections.emptyList());
    transformed = transformedMapToArray(transformed, RELATED_TERMS_FIELD, Collections.emptyList());
    
    return transformed;
  }
}
