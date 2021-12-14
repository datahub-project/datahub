package com.linkedin.metadata.event;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.mxe.MetadataChangeLog;
import javax.annotation.Nonnull;


/**
 * Interface implemented by producers of {@link com.linkedin.mxe.MetadataAuditEvent}s.
 */
public interface EntityEventProducer {

  /**
   * Produces a {@link com.linkedin.mxe.MetadataChangeLog} from a
   * new & previous aspect.
   *
   * @param urn the urn associated with the entity changed
   * @param aspectSpec aspect spec of the aspect being updated
   * @param metadataChangeLog metadata change log to push into MCL kafka topic
   */
  void produceMetadataChangeLog(@Nonnull final Urn urn, @Nonnull AspectSpec aspectSpec,
      @Nonnull final MetadataChangeLog metadataChangeLog);
}
