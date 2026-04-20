package com.linkedin.metadata.aspect.patch.builder;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;
import static com.linkedin.metadata.Constants.DOCUMENTATION_ASPECT_NAME;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.tuple.ImmutableTriple;

public class DocumentationPatchBuilder
    extends AbstractMultiFieldPatchBuilder<DocumentationPatchBuilder> {

  private static final String BASE_PATH = "/documentations/";
  private static final String DOCUMENTATION_KEY = "documentation";
  private static final String ATTRIBUTION_KEY = "attribution";
  private static final String SOURCE_KEY = "source";

  public DocumentationPatchBuilder addDocumentation(@Nonnull String documentation) {
    ObjectNode value = instance.objectNode();
    value.put(DOCUMENTATION_KEY, documentation);
    pathValues.add(ImmutableTriple.of(PatchOperationType.ADD.getValue(), BASE_PATH, value));
    return this;
  }

  /** Removes all documentation entries. */
  public DocumentationPatchBuilder removeDocumentation() {
    pathValues.add(
        ImmutableTriple.of(PatchOperationType.REMOVE.getValue(), "/documentations", null));
    return this;
  }

  /** Removes the entry for a specific source. */
  public DocumentationPatchBuilder removeDocumentation(@Nonnull Urn attributionSource) {
    String path = BASE_PATH + encodeValue(attributionSource.toString());
    pathValues.add(ImmutableTriple.of(PatchOperationType.REMOVE.getValue(), path, null));
    return this;
  }

  @Override
  protected String getAspectName() {
    return DOCUMENTATION_ASPECT_NAME;
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
