package com.linkedin.metadata.aspect.patch.builder;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;
import static com.linkedin.metadata.Constants.GLOBAL_TAGS_ASPECT_NAME;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.ByteString;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.ImmutableTriple;

public class GlobalTagsPatchBuilder extends AbstractMultiFieldPatchBuilder<GlobalTagsPatchBuilder> {

  private static final String BASE_PATH = "/tags/";
  private static final String URN_KEY = "tag";
  private static final String CONTEXT_KEY = "context";
  private static final String ATTRIBUTION_SOURCE_KEY = "attribution\u241fsource";

  /**
   * Unattributed removes use tag-first APK to perform a wildcard match across all sources. They
   * cannot be batched with add ops in the same patch envelope because the two operations require
   * different arrayPrimaryKeys orderings. Use {@link #buildAll()} when both are needed.
   */
  private final List<ImmutableTriple<String, String, JsonNode>> wildcardRemoveOps =
      new ArrayList<>();

  private static final Map<String, List<String>> WILDCARD_REMOVE_APK =
      Collections.singletonMap(
          "tags", Collections.unmodifiableList(Arrays.asList(URN_KEY, ATTRIBUTION_SOURCE_KEY)));

  /**
   * Adds an unattributed tag with an optional context string.
   *
   * <p>Emits a source-first path ({@code /tags//<encodedUrn>}) so that Parsson (Jakarta JSON-P) can
   * apply the patch even when the {@code globalTags} aspect does not yet exist for the entity. The
   * old tag-first layout produced a trailing-slash path ({@code /tags/<urn>/}) whose empty final
   * component causes a {@code JsonException} in Parsson.
   */
  public GlobalTagsPatchBuilder addTag(@Nonnull TagUrn urn, @Nullable String context) {
    ObjectNode value = instance.objectNode();
    value.put(URN_KEY, urn.toString());

    if (context != null) {
      value.put(CONTEXT_KEY, context);
    }

    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(), BASE_PATH + "/" + encodeValueUrn(urn), value));
    return this;
  }

  public GlobalTagsPatchBuilder addTag(
      @Nonnull TagUrn urn, @Nullable String context, @Nonnull Urn attributionSource) {
    ObjectNode value = instance.objectNode();
    value.put(URN_KEY, urn.toString());

    if (context != null) {
      value.put(CONTEXT_KEY, context);
    }

    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            BASE_PATH + encodeValue(attributionSource.toString()) + "/" + encodeValueUrn(urn),
            value));
    return this;
  }

  /**
   * Removes all associations for the given tag, regardless of attribution source.
   *
   * <p>Uses a tag-first {@code arrayPrimaryKeys} ordering ({@code ["tag", "source"]}) so the server
   * treats the path as a prefix match on the tag key, deleting every source-variant of that tag in
   * one operation.
   *
   * <p><b>Mixing with add operations:</b> this operation requires a different APK ordering than
   * {@link #addTag}. Calling {@link #build()} when both types are present throws {@link
   * IllegalStateException}. Use {@link #buildAll()} to emit the two groups as separate patch MCPs.
   */
  public GlobalTagsPatchBuilder removeTag(@Nonnull TagUrn urn) {
    wildcardRemoveOps.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(), BASE_PATH + encodeValueUrn(urn), null));
    return this;
  }

  public GlobalTagsPatchBuilder removeTag(@Nonnull TagUrn urn, @Nonnull Urn attributionSource) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(),
            BASE_PATH + encodeValue(attributionSource.toString()) + "/" + encodeValueUrn(urn),
            null));
    return this;
  }

  /**
   * Builds a single patch MCP.
   *
   * <p>Throws {@link IllegalStateException} when both add/attributed-remove and unattributed-remove
   * operations are present, because they require incompatible {@code arrayPrimaryKeys} orderings.
   * Use {@link #buildAll()} in that case.
   */
  @Override
  public MetadataChangeProposal build() {
    if (!pathValues.isEmpty() && !wildcardRemoveOps.isEmpty()) {
      throw new IllegalStateException(
          "Cannot build a single patch for mixed add/attributed-remove and unattributed-remove "
              + "operations — each requires a different arrayPrimaryKeys ordering. "
              + "Use buildAll() to emit separate MCPs.");
    }
    if (pathValues.isEmpty() && !wildcardRemoveOps.isEmpty()) {
      return buildMcpForOps(wildcardRemoveOps, WILDCARD_REMOVE_APK);
    }
    return super.build();
  }

  /**
   * Builds one or two patch MCPs, grouping add/attributed-remove operations and unattributed-remove
   * operations into separate envelopes with their respective {@code arrayPrimaryKeys} orderings.
   *
   * @return a list of one or two MCPs; never empty
   * @throws IllegalArgumentException if no operations have been added
   */
  public List<MetadataChangeProposal> buildAll() {
    if (pathValues.isEmpty() && wildcardRemoveOps.isEmpty()) {
      throw new IllegalArgumentException("No patches specified.");
    }
    List<MetadataChangeProposal> result = new ArrayList<>();
    if (!pathValues.isEmpty()) {
      result.add(super.build());
    }
    if (!wildcardRemoveOps.isEmpty()) {
      result.add(buildMcpForOps(wildcardRemoveOps, WILDCARD_REMOVE_APK));
    }
    return result;
  }

  private MetadataChangeProposal buildMcpForOps(
      List<ImmutableTriple<String, String, JsonNode>> ops,
      @Nullable Map<String, List<String>> apk) {
    ArrayNode patches = instance.arrayNode();
    ops.forEach(
        triple ->
            patches.add(
                instance
                    .objectNode()
                    .put(OP_KEY, triple.left)
                    .put(PATH_KEY, triple.middle)
                    .set(VALUE_KEY, triple.right)));

    JsonNode payload;
    if (apk != null) {
      ObjectNode apkNode = instance.objectNode();
      for (Map.Entry<String, List<String>> entry : apk.entrySet()) {
        ArrayNode keys = instance.arrayNode();
        entry.getValue().forEach(keys::add);
        apkNode.set(entry.getKey(), keys);
      }
      ObjectNode envelope = instance.objectNode();
      envelope.set("arrayPrimaryKeys", apkNode);
      envelope.set("patch", patches);
      payload = envelope;
    } else {
      payload = patches;
    }

    GenericAspect genericAspect = new GenericAspect();
    genericAspect.setContentType("application/json-patch+json");
    genericAspect.setValue(ByteString.copyString(payload.toString(), StandardCharsets.UTF_8));

    MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setChangeType(ChangeType.PATCH);
    proposal.setEntityType(getEntityType());
    proposal.setEntityUrn(this.targetEntityUrn);
    proposal.setAspectName(getAspectName());
    proposal.setAspect(genericAspect);
    return proposal;
  }

  @Override
  protected Map<String, List<String>> getArrayPrimaryKeys() {
    return Collections.singletonMap(
        "tags", Collections.unmodifiableList(Arrays.asList(ATTRIBUTION_SOURCE_KEY, URN_KEY)));
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
