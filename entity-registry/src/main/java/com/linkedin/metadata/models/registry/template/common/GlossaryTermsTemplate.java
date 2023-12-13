package com.linkedin.metadata.models.registry.template.common;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.*;
import static com.linkedin.metadata.Constants.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlossaryTermAssociationArray;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.registry.template.ArrayMergingTemplate;
import java.util.Collections;
import javax.annotation.Nonnull;

public class GlossaryTermsTemplate implements ArrayMergingTemplate<GlossaryTerms> {

  private static final String TERMS_FIELD_NAME = "terms";
  private static final String URN_FIELD_NAME = "urn";
  private static final String AUDIT_STAMP_FIELD = "auditStamp";
  private static final String TIME_FIELD = "time";
  private static final String ACTOR_FIELD = "actor";

  @Override
  public GlossaryTerms getSubtype(RecordTemplate recordTemplate) throws ClassCastException {
    if (recordTemplate instanceof GlossaryTerms) {
      return (GlossaryTerms) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to GlossaryTerms");
  }

  @Override
  public Class<GlossaryTerms> getTemplateType() {
    return GlossaryTerms.class;
  }

  @Nonnull
  @Override
  public GlossaryTerms getDefault() {
    GlossaryTerms glossaryTerms = new GlossaryTerms();
    glossaryTerms
        .setTerms(new GlossaryTermAssociationArray())
        .setAuditStamp(
            new AuditStamp()
                .setActor(UrnUtils.getUrn(SYSTEM_ACTOR))
                .setTime(System.currentTimeMillis()));

    return glossaryTerms;
  }

  @Nonnull
  @Override
  public JsonNode transformFields(JsonNode baseNode) {
    // Set required deprecated field
    if (baseNode.get(AUDIT_STAMP_FIELD) == null) {
      ObjectNode auditStampNode = instance.objectNode();
      auditStampNode.put(ACTOR_FIELD, SYSTEM_ACTOR).put(TIME_FIELD, System.currentTimeMillis());
      ((ObjectNode) baseNode).set(AUDIT_STAMP_FIELD, auditStampNode);
    }
    return arrayFieldToMap(baseNode, TERMS_FIELD_NAME, Collections.singletonList(URN_FIELD_NAME));
  }

  @Nonnull
  @Override
  public JsonNode rebaseFields(JsonNode patched) {
    // Set required deprecated field
    if (patched.get(AUDIT_STAMP_FIELD) == null) {
      ObjectNode auditStampNode = instance.objectNode();
      auditStampNode.put(ACTOR_FIELD, SYSTEM_ACTOR).put(TIME_FIELD, System.currentTimeMillis());
      ((ObjectNode) patched).set(AUDIT_STAMP_FIELD, auditStampNode);
    }
    return transformedMapToArray(
        patched, TERMS_FIELD_NAME, Collections.singletonList(URN_FIELD_NAME));
  }
}
