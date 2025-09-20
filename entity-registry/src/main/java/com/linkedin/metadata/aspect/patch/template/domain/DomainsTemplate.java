package com.linkedin.metadata.aspect.patch.template.domain;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.UrnArray;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.domain.Domains;
import com.linkedin.metadata.aspect.patch.template.ArrayMergingTemplate;
import java.util.Collections;
import javax.annotation.Nonnull;

/**
 * Template for patching Domains aspects.
 * 
 * Handles: domains array
 */
public class DomainsTemplate implements ArrayMergingTemplate<Domains> {

  private static final String DOMAINS_FIELD = "domains";

  @Override
  public Domains getSubtype(RecordTemplate recordTemplate) throws ClassCastException {
    if (recordTemplate instanceof Domains) {
      return (Domains) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to Domains");
  }

  @Override
  public Class<Domains> getTemplateType() {
    return Domains.class;
  }

  @Nonnull
  @Override
  public Domains getDefault() {
    Domains domains = new Domains();
    // Set empty array for required field
    domains.setDomains(new UrnArray());
    return domains;
  }

  @Nonnull
  @Override
  public JsonNode transformFields(JsonNode baseNode) {
    // Transform domains array to map for patching
    return arrayFieldToMap(baseNode, DOMAINS_FIELD, Collections.emptyList());
  }

  @Nonnull
  @Override
  public JsonNode rebaseFields(JsonNode patched) {
    // Rebase domains map back to array
    return transformedMapToArray(patched, DOMAINS_FIELD, Collections.emptyList());
  }
}
