package com.linkedin.metadata.aspect.patch.builder;

import static com.linkedin.metadata.Constants.SEMANTIC_CONTENT_ASPECT_NAME;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.tuple.ImmutableTriple;

/**
 * JSON Patch builder for one embedding profile on {@code semanticContent}. Paths are {@code
 * /embeddings/<key>} and {@code /embeddings/<key>/<field>}.
 */
public class SemanticContentPatchBuilder
    extends AbstractMultiFieldPatchBuilder<SemanticContentPatchBuilder> {

  private static final String EMBEDDINGS_PATH = "/embeddings/";

  @Nonnull
  public SemanticContentPatchBuilder setProfile(
      @Nonnull String profileKey, @Nonnull JsonNode profile) {
    pathValues.add(
        ImmutableTriple.of(PatchOperationType.ADD.getValue(), profilePath(profileKey), profile));
    return this;
  }

  @Nonnull
  public SemanticContentPatchBuilder removeProfile(@Nonnull String profileKey) {
    pathValues.add(
        ImmutableTriple.of(PatchOperationType.REMOVE.getValue(), profilePath(profileKey), null));
    return this;
  }

  @Nonnull
  public SemanticContentPatchBuilder setProfileField(
      @Nonnull String profileKey, @Nonnull String field, @Nonnull JsonNode value) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            profilePath(profileKey) + "/" + encodeValue(field),
            value));
    return this;
  }

  /**
   * Removes the legacy entity-wide skip marker. {@code SemanticContentTemplate} ignores either
   * remove when that field is already absent, so a document that was never skipped still accepts
   * the profile write.
   */
  @Nonnull
  public SemanticContentPatchBuilder clearLegacySkip() {
    pathValues.add(ImmutableTriple.of(PatchOperationType.REMOVE.getValue(), "/skipReason", null));
    pathValues.add(ImmutableTriple.of(PatchOperationType.REMOVE.getValue(), "/skippedAt", null));
    return this;
  }

  /** Moves the entity refresh marker for one watch key forward. */
  @Nonnull
  public SemanticContentPatchBuilder setRefreshSource(@Nonnull String watchKey, long timeMillis) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            "/refreshMarker/sources/" + encodeValue(watchKey),
            LongNode.valueOf(timeMillis)));
    return this;
  }

  @Nonnull
  private static String profilePath(@Nonnull String profileKey) {
    return EMBEDDINGS_PATH + encodeValue(profileKey);
  }

  @Override
  protected String getAspectName() {
    return SEMANTIC_CONTENT_ASPECT_NAME;
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
