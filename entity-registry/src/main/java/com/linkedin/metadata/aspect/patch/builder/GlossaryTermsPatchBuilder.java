package com.linkedin.metadata.aspect.patch.builder;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;
import static com.linkedin.metadata.Constants.GLOSSARY_TERMS_ASPECT_NAME;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.ImmutableTriple;

public class GlossaryTermsPatchBuilder
    extends AbstractMultiFieldPatchBuilder<GlossaryTermsPatchBuilder> {

  private static final String BASE_PATH = "/terms/";
  private static final String URN_KEY = "urn";
  private static final String CONTEXT_KEY = "context";
  private static final String AUDIT_STAMP_KEY = "auditStamp";
  private static final String TIME_KEY = "time";
  private static final String ACTOR_KEY = "actor";

  /**
   * Adds a term with an optional context string
   *
   * @param urn required
   * @param context optional
   * @return
   */
  public GlossaryTermsPatchBuilder addTerm(@Nonnull Urn urn, @Nullable String context) {
    ObjectNode value = instance.objectNode();
    value.put(URN_KEY, urn.toString());

    if (context != null) {
      value.put(CONTEXT_KEY, context);
    }

    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(), BASE_PATH + encodeValueUrn(urn), value));
    return this;
  }

  public GlossaryTermsPatchBuilder removeTerm(@Nonnull Urn urn) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(), BASE_PATH + encodeValueUrn(urn), null));
    return this;
  }

  public GlossaryTermsPatchBuilder addAuditStamp(@Nonnull AuditStamp auditStamp) {
    ObjectNode lastModifiedValue = instance.objectNode();
    lastModifiedValue.put(TIME_KEY, auditStamp.getTime());
    lastModifiedValue.put(ACTOR_KEY, auditStamp.getActor().toString());

    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(), "/" + AUDIT_STAMP_KEY, lastModifiedValue));

    return this;
  }

  @Override
  protected String getAspectName() {
    return GLOSSARY_TERMS_ASPECT_NAME;
  }

  @Override
  protected String getEntityType() {
    if (this.targetEntityUrn == null) {
      throw new IllegalStateException(
          "Target Entity Urn must be set to determine entity type before building Patch.");
    }
    return this.targetEntityUrn.getEntityType();
  }
}
