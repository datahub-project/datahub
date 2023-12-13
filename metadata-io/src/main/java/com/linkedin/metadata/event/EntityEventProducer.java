package com.linkedin.metadata.event;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.mxe.MetadataAuditOperation;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Interface implemented by producers of {@link com.linkedin.mxe.MetadataAuditEvent}s. */
public interface EntityEventProducer {

  /**
   * Produces a {@link com.linkedin.mxe.MetadataAuditEvent} from a new & previous Entity {@link
   * Snapshot}.
   *
   * @param urn the urn associated with the entity changed
   * @param oldSnapshot a {@link RecordTemplate} corresponding to the old snapshot.
   * @param newSnapshot a {@link RecordTemplate} corresponding to the new snapshot.
   * @param oldSystemMetadata
   * @param newSystemMetadata
   */
  void produceMetadataAuditEvent(
      @Nonnull final Urn urn,
      @Nullable final Snapshot oldSnapshot,
      @Nonnull final Snapshot newSnapshot,
      @Nullable SystemMetadata oldSystemMetadata,
      @Nullable SystemMetadata newSystemMetadata,
      MetadataAuditOperation operation);

  /**
   * Produces a {@link com.linkedin.mxe.MetadataChangeLog} from a new & previous aspect.
   *
   * @param urn the urn associated with the entity changed
   * @param aspectSpec aspect spec of the aspect being updated
   * @param metadataChangeLog metadata change log to push into MCL kafka topic
   */
  void produceMetadataChangeLog(
      @Nonnull final Urn urn,
      @Nonnull AspectSpec aspectSpec,
      @Nonnull final MetadataChangeLog metadataChangeLog);
}
