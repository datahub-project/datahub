package com.linkedin.metadata.models.registry.template.common;

import static com.linkedin.metadata.Constants.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.registry.template.CompoundKeyTemplate;
import java.util.Arrays;
import javax.annotation.Nonnull;

public class OwnershipTemplate extends CompoundKeyTemplate<Ownership> {

  private static final String OWNERS_FIELD_NAME = "owners";
  private static final String OWNER_FIELD_NAME = "owner";
  private static final String TYPE_FIELD_NAME = "type";

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
        baseNode, OWNERS_FIELD_NAME, Arrays.asList(OWNER_FIELD_NAME, TYPE_FIELD_NAME));
  }

  @Nonnull
  @Override
  public JsonNode rebaseFields(JsonNode patched) {
    return transformedMapToArray(
        patched, OWNERS_FIELD_NAME, Arrays.asList(OWNER_FIELD_NAME, TYPE_FIELD_NAME));
  }
}
