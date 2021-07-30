package com.linkedin.metadata.event;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.mxe.MetadataAuditOperation;
import com.linkedin.mxe.SystemMetadata;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * Interface implemented by producers of {@link com.linkedin.mxe.MetadataAuditEvent}s.
 */
public interface EntityEventProducer {

  /**
   * Produces a {@link com.linkedin.mxe.MetadataAuditEvent} from a
   * new & previous Entity {@link Snapshot}.
   *  @param urn the urn associated with the entity changed
   * @param oldSnapshot a {@link RecordTemplate} corresponding to the old snapshot.
   * @param newSnapshot a {@link RecordTemplate} corresponding to the new snapshot.
   * @param oldSystemMetadata
   * @param newSystemMetadata
   */
  void produceMetadataAuditEvent(@Nonnull final Urn urn, @Nullable final Snapshot oldSnapshot,
      @Nonnull final Snapshot newSnapshot, @Nullable SystemMetadata oldSystemMetadata, @Nullable SystemMetadata newSystemMetadata,
      MetadataAuditOperation operation
  );

  /**
   * Produces an aspect-specific {@link com.linkedin.mxe.MetadataAuditEvent} from a
   * new & previous Entity Aspect.
   *
   * @param urn the urn associated with the entity changed
   * @param oldValue a {@link RecordTemplate} corresponding to the old aspect.
   * @param newValue a {@link RecordTemplate} corresponding to the new aspect.
   */
  void produceAspectSpecificMetadataAuditEvent(
      @Nonnull final Urn urn,
      @Nullable final RecordTemplate oldValue,
      @Nonnull final RecordTemplate newValue,
      SystemMetadata oldSystemMetadata,
      SystemMetadata newSystemMetadata,
      MetadataAuditOperation operation
  );
}
