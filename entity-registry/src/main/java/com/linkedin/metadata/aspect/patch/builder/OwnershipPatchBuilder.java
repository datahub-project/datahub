/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.aspect.patch.builder;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;
import static com.linkedin.metadata.Constants.OWNERSHIP_ASPECT_NAME;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.tuple.ImmutableTriple;

public class OwnershipPatchBuilder extends AbstractMultiFieldPatchBuilder<OwnershipPatchBuilder> {

  private static final String BASE_PATH = "/owners/";
  private static final String OWNER_KEY = "owner";
  private static final String TYPE_KEY = "type";

  public OwnershipPatchBuilder addOwner(@Nonnull Urn owner, @Nonnull OwnershipType type) {
    ObjectNode value = instance.objectNode();
    value.put(OWNER_KEY, owner.toString());
    value.put(TYPE_KEY, type.toString());

    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            BASE_PATH + encodeValueUrn(owner) + "/" + encodeValue(type.toString()),
            value));

    return this;
  }

  /**
   * Remove all ownership types for an owner
   *
   * @param owner
   * @return
   */
  public OwnershipPatchBuilder removeOwner(@Nonnull Urn owner) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(), BASE_PATH + encodeValueUrn(owner), null));

    return this;
  }

  /**
   * Removes a specific ownership type for a particular owner, a single owner may have multiple
   * ownership types
   *
   * @param owner
   * @param type
   * @return
   */
  public OwnershipPatchBuilder removeOwnershipType(
      @Nonnull Urn owner, @Nonnull OwnershipType type) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(),
            BASE_PATH + encodeValueUrn(owner) + "/" + encodeValue(type.toString()),
            null));
    return this;
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
