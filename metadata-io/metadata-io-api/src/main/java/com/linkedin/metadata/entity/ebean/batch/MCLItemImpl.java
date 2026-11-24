package com.linkedin.metadata.entity.ebean.batch;

import static com.linkedin.metadata.utils.UnknownDataHandlings.handling;

import com.datahub.util.exception.ModelConversionException;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.entity.validation.ValidationApiUtils;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.PegasusUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@Builder(toBuilder = true)
public class MCLItemImpl implements MCLItem {

  @Nonnull private final MetadataChangeLog metadataChangeLog;

  @Nullable private final RecordTemplate recordTemplate;

  @Nullable private final RecordTemplate previousRecordTemplate;

  // derived
  private final EntitySpec entitySpec;
  private final AspectSpec aspectSpec;

  /**
   * This item with {@code previousRecordTemplate} as its previous value. Used when the previous
   * value was left out of the build because it can't be validated here (a newer version wrote it),
   * so readers of the item can still diff against it.
   */
  public MCLItemImpl withPreviousRecordTemplate(
      @Nullable final RecordTemplate previousRecordTemplate) {
    return new MCLItemImpl(
        metadataChangeLog, recordTemplate, previousRecordTemplate, entitySpec, aspectSpec);
  }

  /**
   * The item for an MCL whose previous value fails validation here because a newer version wrote it
   * (for example it holds an enum symbol only that version defines): built without the previous
   * value. Null when unknown data isn't tolerated, the MCL has no previous value, the previous
   * value can't be attributed to a newer version, or the MCL is invalid without it too.
   */
  @Nullable
  public static MCLItemImpl withoutPreviousFromNewerVersion(
      @Nonnull final MetadataChangeLog mcl, @Nonnull final AspectRetriever aspectRetriever) {
    if (!handling().toleratesUnknownData() || !mcl.hasPreviousAspectValue()) {
      return null;
    }
    final EntityRegistry registry = aspectRetriever.getEntityRegistry();
    final AspectSpec aspectSpec =
        registry.findAspectSpec(mcl.getEntityType(), mcl.getAspectName()).orElse(null);
    if (!handling()
        .isFromNewerVersion(
            // An unreadable previous value isn't attributed to a newer version.
            GenericRecordUtils.deserializeAspectOrNull(mcl.getPreviousAspectValue(), aspectSpec),
            mcl.getPreviousSystemMetadata(),
            aspectSpec,
            registry)) {
      return null;
    }
    return buildWithoutPrevious(mcl, aspectRetriever);
  }

  /** The item built from a copy of the MCL without its previous value; null if that's invalid. */
  @Nullable
  public static MCLItemImpl buildWithoutPrevious(
      @Nonnull final MetadataChangeLog mcl, @Nonnull final AspectRetriever aspectRetriever) {
    final MetadataChangeLog withoutPrevious;
    try {
      withoutPrevious = mcl.copy();
    } catch (CloneNotSupportedException e) {
      return null;
    }
    withoutPrevious.removePreviousAspectValue();
    withoutPrevious.removePreviousSystemMetadata();
    try {
      return MCLItemImpl.builder().build(withoutPrevious, aspectRetriever);
    } catch (RuntimeException invalid) {
      return null;
    }
  }

  public static class MCLItemImplBuilder {

    // Ensure use of other builders
    private MCLItemImpl build() {
      return null;
    }

    public MCLItemImpl build(
        MCPItem mcpItem,
        @Nullable RecordTemplate oldAspectValue,
        @Nullable SystemMetadata oldSystemMetadata,
        AspectRetriever aspectRetriever) {
      return MCLItemImpl.builder()
          .build(
              PegasusUtils.constructMCL(
                  mcpItem.getMetadataChangeProposal(),
                  mcpItem.getUrn().getEntityType(),
                  mcpItem.getUrn(),
                  mcpItem.getChangeType(),
                  mcpItem.getAspectName(),
                  mcpItem.getAuditStamp(),
                  mcpItem.getRecordTemplate(),
                  mcpItem.getSystemMetadata(),
                  oldAspectValue,
                  oldSystemMetadata),
              aspectRetriever);
    }

    public MCLItemImpl build(MetadataChangeLog metadataChangeLog, AspectRetriever aspectRetriever) {
      return MCLItemImpl.builder().metadataChangeLog(metadataChangeLog).build(aspectRetriever);
    }

    public MCLItemImpl build(AspectRetriever aspectRetriever) {
      EntityRegistry entityRegistry = aspectRetriever.getEntityRegistry();

      log.debug("entity type = {}", this.metadataChangeLog.getEntityType());
      entitySpec(
          aspectRetriever
              .getEntityRegistry()
              .getEntitySpec(this.metadataChangeLog.getEntityType()));
      aspectSpec(AspectUtils.validateAspect(this.metadataChangeLog, this.entitySpec));

      Urn urn = this.metadataChangeLog.getEntityUrn();
      if (urn == null) {
        urn =
            EntityKeyUtils.getUrnFromLog(
                this.metadataChangeLog, this.entitySpec.getKeyAspectSpec());
      }
      ValidationApiUtils.validateUrn(entityRegistry, urn);
      log.debug("entity type = {}", urn.getEntityType());

      entitySpec(entityRegistry.getEntitySpec(urn.getEntityType()));
      log.debug("entity spec = {}", this.entitySpec);

      aspectSpec(
          ValidationApiUtils.validateAspect(
              this.entitySpec, this.metadataChangeLog.getAspectName()));
      log.debug("aspect spec = {}", this.aspectSpec);

      Pair<RecordTemplate, RecordTemplate> aspects =
          convertToRecordTemplate(this.metadataChangeLog, aspectSpec);

      // validate new
      ValidationApiUtils.validateRecordTemplate(
          this.entitySpec, urn, aspects.getFirst(), aspectRetriever);
      // Validating the new aspect removed its references to entity types this registry doesn't
      // know (written by a newer version). Remove them from the previous aspect too, so the index
      // diff doesn't read them as removed and delete the newer version's edges and values. A
      // DELETE has no new aspect, and its previous aspect is removed in full.
      if (aspects.getFirst() != null && aspects.getSecond() != null) {
        handling().stripUnknownReferences(aspects.getSecond(), entityRegistry);
      }

      return new MCLItemImpl(
          this.metadataChangeLog,
          aspects.getFirst(),
          aspects.getSecond(),
          this.entitySpec,
          this.aspectSpec);
    }

    private MCLItemImplBuilder entitySpec(EntitySpec entitySpec) {
      this.entitySpec = entitySpec;
      return this;
    }

    private MCLItemImplBuilder aspectSpec(AspectSpec aspectSpec) {
      this.aspectSpec = aspectSpec;
      return this;
    }

    private static Pair<RecordTemplate, RecordTemplate> convertToRecordTemplate(
        MetadataChangeLog mcl, AspectSpec aspectSpec) {
      final RecordTemplate aspect;
      final RecordTemplate prevAspect;
      try {

        if (!ChangeType.DELETE.equals(mcl.getChangeType())) {
          aspect =
              GenericRecordUtils.deserializeAspect(
                  mcl.getAspect().getValue(), mcl.getAspect().getContentType(), aspectSpec);
          ValidationApiUtils.validateTrimOrThrow(aspect);
        } else {
          aspect = null;
        }

        if (mcl.getPreviousAspectValue() != null) {
          prevAspect =
              GenericRecordUtils.deserializeAspect(
                  mcl.getPreviousAspectValue().getValue(),
                  mcl.getPreviousAspectValue().getContentType(),
                  aspectSpec);
          ValidationApiUtils.validateTrimOrThrow(prevAspect);
        } else {
          prevAspect = null;
        }
      } catch (ModelConversionException e) {
        throw new RuntimeException(
            String.format(
                "Could not deserialize %s for aspect %s",
                mcl.getAspect().getValue(), mcl.getAspectName()));
      }

      return Pair.of(aspect, prevAspect);
    }
  }

  @Override
  public boolean isDatabaseDuplicateOf(BatchItem other) {
    return equals(other);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MCLItemImpl that = (MCLItemImpl) o;

    return metadataChangeLog.equals(that.metadataChangeLog);
  }

  @Override
  public int hashCode() {
    return metadataChangeLog.hashCode();
  }
}
