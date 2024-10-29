package com.datahub.metadata.dao.producer;

import com.datahub.util.ModelUtils;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A base class for all metadata event producers.
 *
 * <p>See http://go/gma for more details.
 */
public abstract class BaseMetadataEventProducer<
    SNAPSHOT extends RecordTemplate, ASPECT_UNION extends UnionTemplate, URN extends Urn> {

  protected final Class<SNAPSHOT> _snapshotClass;
  protected final Class<ASPECT_UNION> _aspectUnionClass;

  public BaseMetadataEventProducer(
      @Nonnull Class<SNAPSHOT> snapshotClass, @Nonnull Class<ASPECT_UNION> aspectUnionClass) {
    ModelUtils.validateSnapshotAspect(snapshotClass, aspectUnionClass);
    _snapshotClass = snapshotClass;
    _aspectUnionClass = aspectUnionClass;
  }

  /**
   * Produces a Metadata Change Event (MCE) with a snapshot-base metadata change proposal.
   *
   * @param urn {@link Urn} of the entity
   * @param newValue the proposed new value for the metadata
   * @param <ASPECT> must be a supported aspect type in {@code ASPECT_UNION}
   */
  public abstract <ASPECT extends RecordTemplate> void produceSnapshotBasedMetadataChangeEvent(
      @Nonnull URN urn, @Nonnull ASPECT newValue);

  /**
   * Produces a Metadata Audit Event (MAE) after a metadata aspect is updated for an entity.
   *
   * @param urn {@link Urn} of the entity
   * @param oldValue the value prior to the update, or null if there's none.
   * @param newValue the value after the update
   * @param <ASPECT> must be a supported aspect type in {@code ASPECT_UNION}
   */
  public abstract <ASPECT extends RecordTemplate> void produceMetadataAuditEvent(
      @Nonnull URN urn, @Nullable ASPECT oldValue, @Nonnull ASPECT newValue);

  /**
   * Produces an aspect specific Metadata Audit Event (MAE) after a metadata aspect is updated for
   * an entity.
   *
   * @param urn {@link Urn} of the entity
   * @param oldValue the value prior to the update, or null if there's none.
   * @param newValue the value after the update
   */
  public abstract <ASPECT extends RecordTemplate> void produceAspectSpecificMetadataAuditEvent(
      @Nonnull URN urn, @Nullable ASPECT oldValue, @Nonnull ASPECT newValue);
}
