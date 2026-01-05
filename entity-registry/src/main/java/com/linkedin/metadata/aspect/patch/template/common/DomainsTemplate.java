package com.linkedin.metadata.aspect.patch.template.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.UrnArray;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.domain.Domains;
import com.linkedin.metadata.aspect.patch.template.ArrayMergingTemplate;
import java.util.Collections;
import javax.annotation.Nonnull;

public class DomainsTemplate implements ArrayMergingTemplate<Domains> {

  private static final String DOMAINS_FIELD_NAME = "domains";

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
    domains.setDomains(new UrnArray());

    return domains;
  }

  @Nonnull
  @Override
  public JsonNode transformFields(JsonNode baseNode) {
    return arrayFieldToMap(baseNode, DOMAINS_FIELD_NAME, Collections.emptyList());
  }

  @Nonnull
  @Override
  public JsonNode rebaseFields(JsonNode patched) {
    return transformedMapToArray(patched, DOMAINS_FIELD_NAME, Collections.emptyList());
  }
}
