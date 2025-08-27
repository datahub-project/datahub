package com.linkedin.metadata.aspect.patch.builder;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;
import static com.linkedin.metadata.Constants.SIBLINGS_ASPECT_NAME;

import com.fasterxml.jackson.databind.node.TextNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.tuple.ImmutableTriple;

/**
 * Builder for creating JSON patch operations on the Siblings aspect.
 *
 * <p>Provides fluent API for constructing sibling relationship patches, including adding/removing
 * siblings and setting primary designation. Follows the builder pattern consistent with other
 * DataHub patch builders.
 *
 * <p>Example usage:
 *
 * <pre>
 *   SiblingsPatchBuilder builder = new SiblingsPatchBuilder()
 *       .urn(datasetUrn)
 *       .addSibling(siblingUrn, true)
 *       .setPrimary(true);
 *   JsonPatch patch = builder.build();
 * </pre>
 */
public class SiblingsPatchBuilder extends AbstractMultiFieldPatchBuilder<SiblingsPatchBuilder> {

  private static final String BASE_PATH = "/siblings/";
  private static final String PRIMARY_PATH = "/primary";

  /**
   * Adds a sibling relationship with an optional primary flag
   *
   * @param siblingUrn the URN of the sibling entity
   * @param primary whether this entity should be marked as primary
   * @return this builder instance for chaining
   */
  public SiblingsPatchBuilder addSibling(@Nonnull Urn siblingUrn, boolean primary) {
    // Add the sibling URN to the siblings array
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            BASE_PATH + encodeValueUrn(siblingUrn),
            new TextNode(siblingUrn.toString())));

    // Set primary flag if true
    if (primary) {
      pathValues.add(
          ImmutableTriple.of(
              PatchOperationType.ADD.getValue(), PRIMARY_PATH, instance.booleanNode(true)));
    }

    return this;
  }

  /**
   * Removes a sibling relationship
   *
   * @param siblingUrn the URN of the sibling entity to remove
   * @return this builder instance for chaining
   */
  public SiblingsPatchBuilder removeSibling(@Nonnull Urn siblingUrn) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(), BASE_PATH + encodeValueUrn(siblingUrn), null));
    return this;
  }

  @Override
  protected String getAspectName() {
    return SIBLINGS_ASPECT_NAME;
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
