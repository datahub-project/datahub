package com.linkedin.metadata.entity.ebean.batch;

import static com.linkedin.metadata.entity.AspectUtils.validateAspect;

import com.datahub.util.exception.ModelConversionException;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.MCLBatchItem;
import com.linkedin.metadata.aspect.plugins.validation.AspectRetriever;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.entity.validation.ValidationUtils;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.util.Pair;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@Builder(toBuilder = true)
public class MCLBatchItemImpl implements MCLBatchItem {

  @Nonnull private final MetadataChangeLog metadataChangeLog;

  @Nullable private final RecordTemplate aspect;

  @Nullable private final RecordTemplate previousAspect;

  // derived
  private final EntitySpec entitySpec;
  private final AspectSpec aspectSpec;

  public static class MCLBatchItemImplBuilder {

    public MCLBatchItemImpl build(
        MetadataChangeLog metadataChangeLog,
        EntityRegistry entityRegistry,
        AspectRetriever aspectRetriever) {
      return MCLBatchItemImpl.builder()
          .metadataChangeLog(metadataChangeLog)
          .build(entityRegistry, aspectRetriever);
    }

    public MCLBatchItemImpl build(EntityRegistry entityRegistry, AspectRetriever aspectRetriever) {
      log.debug("entity type = {}", this.metadataChangeLog.getEntityType());
      entitySpec(entityRegistry.getEntitySpec(this.metadataChangeLog.getEntityType()));
      aspectSpec(validateAspect(this.metadataChangeLog, this.entitySpec));

      Urn urn = this.metadataChangeLog.getEntityUrn();
      if (urn == null) {
        urn =
            EntityKeyUtils.getUrnFromLog(
                this.metadataChangeLog, this.entitySpec.getKeyAspectSpec());
      }
      EntityUtils.validateUrn(entityRegistry, urn);
      log.debug("entity type = {}", urn.getEntityType());

      entitySpec(entityRegistry.getEntitySpec(urn.getEntityType()));
      log.debug("entity spec = {}", this.entitySpec);

      aspectSpec(ValidationUtils.validate(this.entitySpec, this.metadataChangeLog.getAspectName()));
      log.debug("aspect spec = {}", this.aspectSpec);

      Pair<RecordTemplate, RecordTemplate> aspects =
          convertToRecordTemplate(this.metadataChangeLog, aspectSpec);

      // validate new
      ValidationUtils.validateRecordTemplate(
          this.metadataChangeLog.getChangeType(),
          entityRegistry,
          this.entitySpec,
          this.aspectSpec,
          urn,
          aspects.getFirst(),
          aspectRetriever);

      return new MCLBatchItemImpl(
          this.metadataChangeLog,
          aspects.getFirst(),
          aspects.getSecond(),
          this.entitySpec,
          this.aspectSpec);
    }

    private MCLBatchItemImplBuilder entitySpec(EntitySpec entitySpec) {
      this.entitySpec = entitySpec;
      return this;
    }

    private MCLBatchItemImplBuilder aspectSpec(AspectSpec aspectSpec) {
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
          ValidationUtils.validateOrThrow(aspect);
        } else {
          aspect = null;
        }

        if (mcl.getPreviousAspectValue() != null) {
          prevAspect =
              GenericRecordUtils.deserializeAspect(
                  mcl.getPreviousAspectValue().getValue(),
                  mcl.getPreviousAspectValue().getContentType(),
                  aspectSpec);
          ValidationUtils.validateOrThrow(prevAspect);
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
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MCLBatchItemImpl that = (MCLBatchItemImpl) o;

    return metadataChangeLog.equals(that.metadataChangeLog);
  }

  @Override
  public int hashCode() {
    return metadataChangeLog.hashCode();
  }
}
