package com.linkedin.metadata.entity.coordinator;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import javax.annotation.Nonnull;

/**
 * A proposed upsert of an aspect. Carries the existing {@link ChangeMCP} so that apply reuses the
 * standard write logic; the coordinator-side view (key + proposed read state) is derived from it.
 */
public record PlannedUpsert(@Nonnull ChangeMCP mcp) implements PlannedMutation {

  @Nonnull
  public static PlannedUpsert of(@Nonnull ChangeMCP mcp) {
    return new PlannedUpsert(mcp);
  }

  @Override
  @Nonnull
  public AspectKey key() {
    return AspectKey.latest(mcp.getUrn().toString(), mcp.getAspectName());
  }

  /**
   * The proposed aspect value as an {@link Aspect} (restli view), for {@code
   * getLatestAspectObjects} reads served from the overlay.
   */
  @Nonnull
  public Aspect proposedAspect() {
    RecordTemplate recordTemplate = mcp.getRecordTemplate();
    if (recordTemplate == null) {
      throw new IllegalStateException(
          "PlannedUpsert has no record template for " + mcp.toAbbreviatedString());
    }
    return new Aspect(recordTemplate.data());
  }

  /**
   * The proposed aspect value as a {@link SystemAspect} (with system metadata / audit stamp), for
   * {@code getLatestSystemAspects} reads served from the overlay. Constructed as a version-0
   * (latest) {@link EntityAspect.EntitySystemAspect} with no backing database row, mirroring the
   * type the entity service uses for the in-flight aspect state.
   */
  @Nonnull
  public SystemAspect proposedSystemAspect() {
    // Uses the public all-args constructor because EntitySystemAspect's lombok builder exposes only
    // forInsert/forUpdate (its build() is private) — neither of which fits a purely in-memory
    // proposed aspect that has no EntityAspect DB row yet.
    return new EntityAspect.EntitySystemAspect(
        /* entityAspect */ null,
        mcp.getUrn(),
        mcp.getRecordTemplate(),
        mcp.getSystemMetadata(),
        mcp.getAuditStamp(),
        mcp.getEntitySpec(),
        mcp.getAspectSpec(),
        /* systemAspectValidators */ null,
        /* operationContext */ null);
  }
}
