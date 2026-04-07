package com.linkedin.metadata.aspect.patch.builder;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;
import static com.linkedin.metadata.Constants.GLOBAL_TAGS_ASPECT_NAME;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.ImmutableTriple;

public class GlobalTagsPatchBuilder extends AbstractMultiFieldPatchBuilder<GlobalTagsPatchBuilder> {

  private static final String BASE_PATH = "/tags/";
  private static final String TAG_KEY = "tag";
  private static final String CONTEXT_KEY = "context";

  public GlobalTagsPatchBuilder addTag(@Nonnull TagUrn urn, @Nullable String context) {
    return addTag(urn, context, null);
  }

  public GlobalTagsPatchBuilder addTag(
      @Nonnull TagUrn urn, @Nullable String context, @Nullable Urn attributionSource) {
    ObjectNode value = instance.objectNode();
    value.put(TAG_KEY, urn.toString());
    if (context != null) {
      value.put(CONTEXT_KEY, context);
    }
    String sourcePath = attributionSource != null ? encodeValue(attributionSource.toString()) : "";
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            BASE_PATH + encodeValueUrn(urn) + "/" + sourcePath,
            value));
    return this;
  }

  /** Removes all entries for this tag URN across every attribution source. */
  public GlobalTagsPatchBuilder removeTag(@Nonnull TagUrn urn) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(), BASE_PATH + encodeValueUrn(urn), null));
    return this;
  }

  /** Removes only the entry for this tag URN attributed to a specific source. */
  public GlobalTagsPatchBuilder removeTag(@Nonnull TagUrn urn, @Nonnull Urn attributionSource) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(),
            BASE_PATH + encodeValueUrn(urn) + "/" + encodeValue(attributionSource.toString()),
            null));
    return this;
  }

  @Override
  protected String getAspectName() {
    return GLOBAL_TAGS_ASPECT_NAME;
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
