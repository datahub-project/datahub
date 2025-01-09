package com.linkedin.metadata.entity.ebean.batch;

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
          ValidationApiUtils.validate(this.entitySpec, this.metadataChangeLog.getAspectName()));
      log.debug("aspect spec = {}", this.aspectSpec);

      Pair<RecordTemplate, RecordTemplate> aspects =
          convertToRecordTemplate(this.metadataChangeLog, aspectSpec);

      // validate new
      ValidationApiUtils.validateRecordTemplate(
          this.entitySpec, urn, aspects.getFirst(), aspectRetriever);

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
          ValidationApiUtils.validateOrThrow(aspect);
        } else {
          aspect = null;
        }

        if (mcl.getPreviousAspectValue() != null) {
          prevAspect =
              GenericRecordUtils.deserializeAspect(
                  mcl.getPreviousAspectValue().getValue(),
                  mcl.getPreviousAspectValue().getContentType(),
                  aspectSpec);
          ValidationApiUtils.validateOrThrow(prevAspect);
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
