package com.linkedin.metadata.aspect.patch.builder;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;
import static com.linkedin.metadata.Constants.INSTITUTIONAL_MEMORY_ASPECT_NAME;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.tuple.ImmutableTriple;

/**
 * Builds patches for {@code institutionalMemory.elements}. Keys are {@code url} then {@code
 * description}, the same compound-key convention as ownership. {@code /} and {@code ~} in either
 * segment are JSON Pointer escaped.
 */
public class InstitutionalMemoryPatchBuilder
    extends AbstractMultiFieldPatchBuilder<InstitutionalMemoryPatchBuilder> {

  private static final String BASE_PATH = "/elements/";
  private static final String URL_KEY = "url";
  private static final String DESCRIPTION_KEY = "description";
  private static final String CREATE_STAMP_KEY = "createStamp";
  private static final String ACTOR_KEY = "actor";
  private static final String TIME_KEY = "time";

  public InstitutionalMemoryPatchBuilder addLink(
      @Nonnull String url, @Nonnull String description, @Nonnull Urn actor) {
    ObjectNode createStamp = instance.objectNode();
    createStamp.put(TIME_KEY, System.currentTimeMillis());
    createStamp.put(ACTOR_KEY, actor.toString());

    ObjectNode value = instance.objectNode();
    value.put(URL_KEY, url);
    value.put(DESCRIPTION_KEY, description);
    value.set(CREATE_STAMP_KEY, createStamp);

    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            BASE_PATH + encodeValue(url) + "/" + encodeValue(description),
            value));
    return this;
  }

  /** Removes every link with this URL, regardless of description. */
  public InstitutionalMemoryPatchBuilder removeLink(@Nonnull String url) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(), BASE_PATH + encodeValue(url), null));
    return this;
  }

  public InstitutionalMemoryPatchBuilder removeLink(
      @Nonnull String url, @Nonnull String description) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(),
            BASE_PATH + encodeValue(url) + "/" + encodeValue(description),
            null));
    return this;
  }

  @Override
  protected Map<String, List<String>> getArrayPrimaryKeys() {
    return Collections.singletonMap(
        "elements", Collections.unmodifiableList(Arrays.asList(URL_KEY, DESCRIPTION_KEY)));
  }

  @Override
  protected String getAspectName() {
    return INSTITUTIONAL_MEMORY_ASPECT_NAME;
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
