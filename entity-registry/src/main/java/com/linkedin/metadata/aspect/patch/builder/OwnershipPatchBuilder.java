package com.linkedin.metadata.aspect.patch.builder;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;
import static com.linkedin.metadata.Constants.OWNERSHIP_ASPECT_NAME;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.ImmutableTriple;

public class OwnershipPatchBuilder extends AbstractMultiFieldPatchBuilder<OwnershipPatchBuilder> {

  private static final String BASE_PATH = "/owners/";
  private static final String OWNER_KEY = "owner";
  private static final String TYPE_KEY = "type";
  private static final String TYPE_URN_KEY = "typeUrn";
  private static final String ATTRIBUTION_SOURCE_KEY = "attribution\u241fsource";

  public OwnershipPatchBuilder addOwner(@Nonnull Urn owner, @Nonnull OwnershipType type) {
    return addOwner(owner, type, null);
  }

  public OwnershipPatchBuilder addOwner(
      @Nonnull Urn owner, @Nonnull OwnershipType type, @Nullable Urn typeUrn) {
    ObjectNode value = instance.objectNode();
    value.put(OWNER_KEY, owner.toString());
    value.put(TYPE_KEY, type.toString());
    if (typeUrn != null) {
      value.put(TYPE_URN_KEY, typeUrn.toString());
    }
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            BASE_PATH
                + encodeValueUrn(owner)
                + "/"
                + encodeValue(type.toString())
                + "/"
                + encodeValue(typeUrn != null ? typeUrn.toString() : "")
                + "/",
            value));
    return this;
  }

  public OwnershipPatchBuilder addOwner(
      @Nonnull Urn owner,
      @Nonnull OwnershipType type,
      @Nullable Urn typeUrn,
      @Nonnull Urn attributionSource) {
    ObjectNode value = instance.objectNode();
    value.put(OWNER_KEY, owner.toString());
    value.put(TYPE_KEY, type.toString());
    if (typeUrn != null) {
      value.put(TYPE_URN_KEY, typeUrn.toString());
    }
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            BASE_PATH
                + encodeValueUrn(owner)
                + "/"
                + encodeValue(type.toString())
                + "/"
                + encodeValue(typeUrn != null ? typeUrn.toString() : "")
                + "/"
                + encodeValue(attributionSource.toString()),
            value));
    return this;
  }

  /** Removes all ownership entries for this owner. */
  public OwnershipPatchBuilder removeOwner(@Nonnull Urn owner) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(), BASE_PATH + encodeValueUrn(owner), null));
    return this;
  }

  /** Removes all entries for a given owner with a specific ownership-type enum. */
  public OwnershipPatchBuilder removeOwnershipType(
      @Nonnull Urn owner, @Nonnull OwnershipType type) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(),
            BASE_PATH + encodeValueUrn(owner) + "/" + encodeValue(type.toString()),
            null));
    return this;
  }

  /** Removes all entries for a given owner with a specific ownership type and typeUrn. */
  public OwnershipPatchBuilder removeOwner(
      @Nonnull Urn owner, @Nonnull OwnershipType type, @Nonnull Urn typeUrn) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(),
            BASE_PATH
                + encodeValueUrn(owner)
                + "/"
                + encodeValue(type.toString())
                + "/"
                + encodeValue(typeUrn.toString()),
            null));
    return this;
  }

  public OwnershipPatchBuilder removeOwner(
      @Nonnull Urn owner,
      @Nonnull OwnershipType type,
      @Nonnull Urn typeUrn,
      @Nonnull Urn attributionSource) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(),
            BASE_PATH
                + encodeValueUrn(owner)
                + "/"
                + encodeValue(type.toString())
                + "/"
                + encodeValue(typeUrn.toString())
                + "/"
                + encodeValue(attributionSource.toString()),
            null));
    return this;
  }

  @Override
  protected Map<String, List<String>> getArrayPrimaryKeys() {
    return Collections.singletonMap(
        "owners",
        Collections.unmodifiableList(
            Arrays.asList(OWNER_KEY, TYPE_KEY, TYPE_URN_KEY, ATTRIBUTION_SOURCE_KEY)));
  }

  @Override
  protected String getAspectName() {
    return OWNERSHIP_ASPECT_NAME;
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
