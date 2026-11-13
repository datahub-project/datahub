package com.linkedin.metadata.aspect.patch.builder;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;
import static com.linkedin.metadata.Constants.GLOSSARY_TERMS_ASPECT_NAME;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.ImmutableTriple;

public class GlossaryTermsPatchBuilder
    extends AbstractMultiFieldPatchBuilder<GlossaryTermsPatchBuilder> {

  private static final String BASE_PATH = "/terms/";
  private static final String URN_KEY = "urn";
  private static final String CONTEXT_KEY = "context";
  private static final String ATTRIBUTION_SOURCE_KEY = "attribution\u241fsource";

  /**
   * Adds a term with an optional context string
   *
   * @param urn required
   * @param context optional
   * @return
   */
  public GlossaryTermsPatchBuilder addTerm(@Nonnull GlossaryTermUrn urn, @Nullable String context) {
    ObjectNode value = instance.objectNode();
    value.put(URN_KEY, urn.toString());

    if (context != null) {
      value.put(CONTEXT_KEY, context);
    }

    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(), BASE_PATH + encodeValueUrn(urn) + "/", value));
    return this;
  }

  public GlossaryTermsPatchBuilder addTerm(
      @Nonnull GlossaryTermUrn urn, @Nullable String context, @Nonnull Urn attributionSource) {
    ObjectNode value = instance.objectNode();
    value.put(URN_KEY, urn.toString());

    if (context != null) {
      value.put(CONTEXT_KEY, context);
    }

    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            BASE_PATH + encodeValueUrn(urn) + "/" + encodeValue(attributionSource.toString()),
            value));
    return this;
  }

  public GlossaryTermsPatchBuilder removeTerm(@Nonnull GlossaryTermUrn urn) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(), BASE_PATH + encodeValueUrn(urn), null));
    return this;
  }

  public GlossaryTermsPatchBuilder removeTerm(
      @Nonnull GlossaryTermUrn urn, @Nonnull Urn attributionSource) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(),
            BASE_PATH + encodeValueUrn(urn) + "/" + encodeValue(attributionSource.toString()),
            null));
    return this;
  }

  @Override
  protected Map<String, List<String>> getArrayPrimaryKeys() {
    return Collections.singletonMap(
        "terms", Collections.unmodifiableList(Arrays.asList(URN_KEY, ATTRIBUTION_SOURCE_KEY)));
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
