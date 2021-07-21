package com.linkedin.metadata.event;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.snapshot.Snapshot;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * Interface implemented by producers of {@link com.linkedin.mxe.MetadataAuditEvent}s.
 */
public interface EntityEventProducer {

  /**
   * Produces a {@link com.linkedin.mxe.MetadataAuditEvent} from a
   * new & previous Entity {@link Snapshot}.
   *
   * @param urn the urn associated with the entity changed
   * @param oldSnapshot a {@link RecordTemplate} corresponding to the old snapshot.
   * @param newSnapshot a {@link RecordTemplate} corresponding to the new snapshot.
   */
  void produceMetadataAuditEvent(
      @Nonnull final Urn urn,
      @Nullable final Snapshot oldSnapshot,
      @Nonnull final Snapshot newSnapshot);

  /**
   * Produces a {@link com.linkedin.mxe.MetadataChangeLog} from a
   * new & previous aspect.
   *
   * @param urn the urn associated with the entity changed
   * @param entityName name of the entity
   * @param changeType type of change that is being applied
   * @param aspectName name of the aspect
   * @param oldAspect a {@link RecordTemplate} corresponding to the old aspect.
   * @param newAspect a {@link RecordTemplate} corresponding to the new aspect.
   */
  void produceMetadataChangeLog(
      @Nonnull final Urn urn,
      @Nonnull final String entityName,
      @Nonnull final ChangeType changeType,
      @Nullable final String aspectName,
      @Nullable final RecordTemplate oldAspect,
      @Nullable final RecordTemplate newAspect);

  /**
   * Produces an aspect-specific {@link com.linkedin.mxe.MetadataChangeEvent} from a
   * new & previous Entity Aspect.
   *
   * @param urn the urn associated with the entity changed
   * @param oldValue a {@link RecordTemplate} corresponding to the old aspect.
   * @param newValue a {@link RecordTemplate} corresponding to the new aspect.
   */
  void produceAspectSpecificMetadataAuditEvent(
      @Nonnull final Urn urn,
      @Nullable final RecordTemplate oldValue,
      @Nonnull final RecordTemplate newValue);
}
