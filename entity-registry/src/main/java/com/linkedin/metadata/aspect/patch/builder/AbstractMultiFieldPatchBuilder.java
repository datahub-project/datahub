package com.linkedin.metadata.aspect.patch.builder;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.ByteString;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import jakarta.json.Json;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.ImmutableTriple;

public abstract class AbstractMultiFieldPatchBuilder<T extends AbstractMultiFieldPatchBuilder<T>> {

  public static final String OP_KEY = "op";
  public static final String VALUE_KEY = "value";
  public static final String PATH_KEY = "path";

  protected List<ImmutableTriple<String, String, JsonNode>> pathValues = new ArrayList<>();
  protected Urn targetEntityUrn = null;

  /**
   * Per-instance override for arrayPrimaryKeys. When set, takes precedence over {@link
   * #getArrayPrimaryKeys()}. Subclasses may set this for operations that require a non-default key
   * ordering (e.g. {@code removeOwner} with non-contiguous null fields).
   */
  @Nullable protected Map<String, List<String>> arrayPrimaryKeysOverride = null;

  /**
   * Builder method
   *
   * @return a {@link MetadataChangeProposal} constructed from the builder's properties
   */
  public MetadataChangeProposal build() {
    MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setChangeType(ChangeType.PATCH);
    proposal.setEntityType(getEntityType());
    proposal.setEntityUrn(this.targetEntityUrn);
    proposal.setAspectName(getAspectName());
    proposal.setAspect(buildPatch());
    return proposal;
  }

  /**
   * Sets the target entity urn to be updated by this patch
   *
   * @param urn The target entity whose aspect is to be patched by this update
   * @return this PatchBuilder subtype's instance
   */
  @SuppressWarnings("unchecked")
  public T urn(Urn urn) {
    this.targetEntityUrn = urn;
    return (T) this;
  }

  /**
   * The aspect name associated with this builder
   *
   * @return aspect name
   */
  protected abstract String getAspectName();

  /**
   * Returns the String representation of the Entity type associated with this aspect
   *
   * @return entity type name
   */
  protected abstract String getEntityType();

  /**
   * Returns the arrayPrimaryKeys map for this aspect's GenericJsonPatch envelope. When non-null,
   * the patch payload is wrapped in {@code {"arrayPrimaryKeys": ..., "patch": [...]}}.
   */
  @Nullable
  protected Map<String, List<String>> getArrayPrimaryKeys() {
    return null;
  }

  protected static String encodeValue(@Nonnull String value) {
    return value.replace("~ ", "~0").replace("/", "~1");
  }

  protected static String encodeValueUrn(@Nonnull Urn urn) {
    return encodeValue(urn.toString());
  }

  /**
   * Overrides basic behavior to construct multiple patches based on properties
   *
   * @return a JsonPatch wrapped by GenericAspect
   */
  protected GenericAspect buildPatch() {
    if (pathValues.isEmpty()) {
      throw new IllegalArgumentException("No patches specified.");
    }

    ArrayNode patches = instance.arrayNode();
    List<ImmutableTriple<String, String, JsonNode>> triples = getPathValues();
    triples.forEach(
        triple ->
            patches.add(
                instance
                    .objectNode()
                    .put(OP_KEY, triple.left)
                    .put(PATH_KEY, triple.middle)
                    .set(VALUE_KEY, triple.right)));

    Map<String, List<String>> apk =
        arrayPrimaryKeysOverride != null ? arrayPrimaryKeysOverride : getArrayPrimaryKeys();
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

    return genericAspect;
  }

  @VisibleForTesting
  public GenericJsonPatch getJsonPatch() {
    Map<String, List<String>> apk =
        arrayPrimaryKeysOverride != null ? arrayPrimaryKeysOverride : getArrayPrimaryKeys();

    List<GenericJsonPatch.PatchOp> patchOps =
        getPathValues().stream()
            .map(
                triple -> {
                  GenericJsonPatch.PatchOp op = new GenericJsonPatch.PatchOp();
                  op.setOp(triple.left);
                  op.setPath(triple.middle);
                  if (triple.right != null) {
                    op.setValue(
                        Json.createReader(new StringReader(triple.right.toString())).readValue());
                  }
                  return op;
                })
            .collect(Collectors.toList());

    return GenericJsonPatch.builder().arrayPrimaryKeys(apk).patch(patchOps).build();
  }

  /**
   * Constructs a list of Op, Path, Value triples to create as patches. Not idempotent and should
   * not be called more than once
   *
   * @return list of patch precursor triples
   */
  protected List<ImmutableTriple<String, String, JsonNode>> getPathValues() {
    return this.pathValues;
  }
}
