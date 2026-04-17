package com.linkedin.metadata.aspect.patch.template.common;

import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.patch.template.ArrayMergingTemplate;
import java.util.Arrays;
import java.util.Collections;
import javax.annotation.Nonnull;

public class OwnershipTemplate implements ArrayMergingTemplate<Ownership> {

  private static final String OWNERS_FIELD_NAME = "owners";
  private static final String OWNER_FIELD_NAME = "owner";
  private static final String TYPE_FIELD_NAME = "type";
  private static final String TYPE_URN_FIELD_NAME = "typeUrn";
  private static final String ATTRIBUTION_SOURCE =
      "attribution" + UNIT_SEPARATOR_DELIMITER + "source";

  @Override
  public Ownership getSubtype(RecordTemplate recordTemplate) throws ClassCastException {
    if (recordTemplate instanceof Ownership) {
      return (Ownership) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to Ownership");
  }

  @Override
  public Class<Ownership> getTemplateType() {
    return Ownership.class;
  }

  @Nonnull
  @Override
  public Ownership getDefault() {
    Ownership ownership = new Ownership();
    ownership.setOwners(new OwnerArray());
    ownership.setLastModified(
        new AuditStamp()
            .setTime(System.currentTimeMillis())
            .setActor(UrnUtils.getUrn(SYSTEM_ACTOR)));

    return ownership;
  }

  @Nonnull
  @Override
  public JsonNode transformFields(JsonNode baseNode) {
    return arrayFieldToMap(
        baseNode,
        OWNERS_FIELD_NAME,
        Collections.unmodifiableList(
            Arrays.asList(
                OWNER_FIELD_NAME, TYPE_FIELD_NAME, TYPE_URN_FIELD_NAME, ATTRIBUTION_SOURCE)));
  }

  @Nonnull
  @Override
  public JsonNode rebaseFields(JsonNode patched) {
    return transformedMapToArray(
        patched,
        OWNERS_FIELD_NAME,
        Collections.unmodifiableList(
            Arrays.asList(
                OWNER_FIELD_NAME, TYPE_FIELD_NAME, TYPE_URN_FIELD_NAME, ATTRIBUTION_SOURCE)));
  }
}
