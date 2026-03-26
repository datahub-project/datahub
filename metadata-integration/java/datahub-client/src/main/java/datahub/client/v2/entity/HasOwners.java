package datahub.client.v2.entity;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.patch.builder.OwnershipPatchBuilder;
import datahub.event.MetadataChangeProposalWrapper;
import java.net.URISyntaxException;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * Mixin interface providing ownership operations for entities that support ownership.
 *
 * <p>Entities implementing this interface can have owners assigned with different ownership types
 * (e.g., DATA_OWNER, TECHNICAL_OWNER, etc.).
 *
 * <p>Example usage:
 *
 * <pre>
 * Dataset dataset = Dataset.builder()
 *     .platform("snowflake")
 *     .name("my_table")
 *     .build();
 *
 * dataset.addOwner("urn:li:corpuser:johndoe", OwnershipType.DATA_OWNER);
 * dataset.addOwner("urn:li:corpuser:janedoe", OwnershipType.TECHNICAL_OWNER);
 * </pre>
 *
 * @param <T> the concrete entity type (self-bounded generic for fluent returns)
 */
public interface HasOwners<T extends Entity & HasOwners<T>> {

  /**
   * Adds an owner to this entity.
   *
   * @param ownerUrn the URN of the owner (user or group)
   * @param type the ownership type
   * @return this entity for fluent chaining
   */
  @Nonnull
  default T addOwner(@Nonnull String ownerUrn, @Nonnull OwnershipType type) {
    try {
      return addOwner(Urn.createFromString(ownerUrn), type);
    } catch (URISyntaxException e) {
      throw new datahub.client.v2.exceptions.InvalidUrnException(ownerUrn, e);
    }
  }

  /**
   * Adds an owner to this entity.
   *
   * @param ownerUrn the URN object of the owner (user or group)
   * @param type the ownership type
   * @return this entity for fluent chaining
   */
  @Nonnull
  default T addOwner(@Nonnull Urn ownerUrn, @Nonnull OwnershipType type) {
    Entity entity = (Entity) this;

    // Get or create accumulated patch builder for ownership
    OwnershipPatchBuilder builder =
        entity.getPatchBuilder("ownership", OwnershipPatchBuilder.class);
    if (builder == null) {
      builder = new OwnershipPatchBuilder().urn(entity.getUrn());
      entity.registerPatchBuilder("ownership", builder);
    }

    builder.addOwner(ownerUrn, type);
    return (T) this;
  }

  /**
   * Removes an owner from this entity.
   *
   * @param ownerUrn the URN of the owner to remove
   * @return this entity for fluent chaining
   */
  @Nonnull
  default T removeOwner(@Nonnull String ownerUrn) {
    try {
      return removeOwner(Urn.createFromString(ownerUrn));
    } catch (URISyntaxException e) {
      throw new datahub.client.v2.exceptions.InvalidUrnException(ownerUrn, e);
    }
  }

  /**
   * Removes an owner from this entity.
   *
   * @param ownerUrn the URN object of the owner to remove
   * @return this entity for fluent chaining
   */
  @Nonnull
  default T removeOwner(@Nonnull Urn ownerUrn) {
    Entity entity = (Entity) this;

    // If the ownership aspect is cached (entity was fetched from server),
    // we need to remove it from cache so the patch can take effect
    entity.removeAspect(com.linkedin.common.Ownership.class);

    // Get or create accumulated patch builder for ownership
    OwnershipPatchBuilder builder =
        entity.getPatchBuilder("ownership", OwnershipPatchBuilder.class);
    if (builder == null) {
      builder = new OwnershipPatchBuilder().urn(entity.getUrn());
      entity.registerPatchBuilder("ownership", builder);
    }

    builder.removeOwner(ownerUrn);
    return (T) this;
  }

  /**
   * Sets the complete list of owners for this entity, replacing any existing owners.
   *
   * <p>Unlike {@link #addOwner(String, OwnershipType)} which creates a patch, this method creates a
   * full aspect replacement. This ensures that ONLY the specified owners will be present after
   * save.
   *
   * @param owners the list of owners to set
   * @return this entity for fluent chaining
   */
  @Nonnull
  default T setOwners(@Nonnull List<Owner> owners) {
    Entity entity = (Entity) this;

    // Create full Ownership aspect with complete list
    Ownership ownership = new Ownership();
    OwnerArray ownerArray = new OwnerArray();
    ownerArray.addAll(owners);
    ownership.setOwners(ownerArray);

    // Set audit stamp
    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(UrnUtils.getUrn("urn:li:corpuser:datahub"));
    auditStamp.setTime(System.currentTimeMillis());
    ownership.setLastModified(auditStamp);

    // Create MCP wrapper for full aspect replacement
    MetadataChangeProposalWrapper mcp =
        MetadataChangeProposalWrapper.builder()
            .entityType(entity.getEntityType())
            .entityUrn(entity.getUrn())
            .upsert()
            .aspect(ownership)
            .build();

    // Add to pending MCPs (this clears any patches for ownership)
    entity.addPendingMCP(mcp);

    return (T) this;
  }

  /**
   * Gets the list of owners associated with this entity.
   *
   * @return the list of owners, or null if no owners are present
   */
  default List<Owner> getOwners() {
    Entity entity = (Entity) this;
    Ownership aspect = entity.getAspectLazy(Ownership.class);
    return aspect != null && aspect.hasOwners() ? aspect.getOwners() : null;
  }
}
