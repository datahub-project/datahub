package com.linkedin.metadata.entity.ebean.transactions;

import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;

import com.datahub.util.exception.ModelConversionException;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.entity.EntityAspect;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.entity.transactions.AbstractBatchItem;
import com.linkedin.metadata.entity.validation.ValidationUtils;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import java.sql.Timestamp;
import java.util.Objects;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@Builder(toBuilder = true)
public class UpsertBatchItem extends AbstractBatchItem {

  // urn an urn associated with the new aspect
  private final Urn urn;
  // aspectName name of the aspect being inserted
  private final String aspectName;
  private final SystemMetadata systemMetadata;

  private final RecordTemplate aspect;

  private final MetadataChangeProposal metadataChangeProposal;

  // derived
  private final EntitySpec entitySpec;
  private final AspectSpec aspectSpec;

  @Override
  public ChangeType getChangeType() {
    return ChangeType.UPSERT;
  }

  @Override
  public void validateUrn(EntityRegistry entityRegistry, Urn urn) {
    EntityUtils.validateUrn(entityRegistry, urn);
  }

  public EntityAspect toLatestEntityAspect(AuditStamp auditStamp) {
    EntityAspect latest = new EntityAspect();
    latest.setAspect(getAspectName());
    latest.setMetadata(EntityUtils.toJsonAspect(getAspect()));
    latest.setUrn(getUrn().toString());
    latest.setVersion(ASPECT_LATEST_VERSION);
    latest.setCreatedOn(new Timestamp(auditStamp.getTime()));
    latest.setCreatedBy(auditStamp.getActor().toString());
    return latest;
  }

  public static class UpsertBatchItemBuilder {

    public UpsertBatchItem build(EntityRegistry entityRegistry) {
      EntityUtils.validateUrn(entityRegistry, this.urn);
      log.debug("entity type = {}", this.urn.getEntityType());

      entitySpec(entityRegistry.getEntitySpec(this.urn.getEntityType()));
      log.debug("entity spec = {}", this.entitySpec);

      aspectSpec(ValidationUtils.validate(this.entitySpec, this.aspectName));
      log.debug("aspect spec = {}", this.aspectSpec);

      ValidationUtils.validateRecordTemplate(
          entityRegistry, this.entitySpec, this.urn, this.aspect);

      return new UpsertBatchItem(
          this.urn,
          this.aspectName,
          AbstractBatchItem.generateSystemMetadataIfEmpty(this.systemMetadata),
          this.aspect,
          this.metadataChangeProposal,
          this.entitySpec,
          this.aspectSpec);
    }

    public static UpsertBatchItem build(MetadataChangeProposal mcp, EntityRegistry entityRegistry) {
      if (!mcp.getChangeType().equals(ChangeType.UPSERT)) {
        throw new IllegalArgumentException(
            "Invalid MCP, this class only supports change type of UPSERT.");
      }

      log.debug("entity type = {}", mcp.getEntityType());
      EntitySpec entitySpec = entityRegistry.getEntitySpec(mcp.getEntityType());
      AspectSpec aspectSpec = validateAspect(mcp, entitySpec);

      if (!isValidChangeType(ChangeType.UPSERT, aspectSpec)) {
        throw new UnsupportedOperationException(
            "ChangeType not supported: "
                + mcp.getChangeType()
                + " for aspect "
                + mcp.getAspectName());
      }

      Urn urn = mcp.getEntityUrn();
      if (urn == null) {
        urn = EntityKeyUtils.getUrnFromProposal(mcp, entitySpec.getKeyAspectSpec());
      }

      UpsertBatchItemBuilder builder =
          UpsertBatchItem.builder()
              .urn(urn)
              .aspectName(mcp.getAspectName())
              .systemMetadata(mcp.getSystemMetadata())
              .metadataChangeProposal(mcp)
              .aspect(convertToRecordTemplate(mcp, aspectSpec));

      return builder.build(entityRegistry);
    }

    private UpsertBatchItemBuilder entitySpec(EntitySpec entitySpec) {
      this.entitySpec = entitySpec;
      return this;
    }

    private UpsertBatchItemBuilder aspectSpec(AspectSpec aspectSpec) {
      this.aspectSpec = aspectSpec;
      return this;
    }

    private static RecordTemplate convertToRecordTemplate(
        MetadataChangeProposal mcp, AspectSpec aspectSpec) {
      RecordTemplate aspect;
      try {
        aspect =
            GenericRecordUtils.deserializeAspect(
                mcp.getAspect().getValue(), mcp.getAspect().getContentType(), aspectSpec);
        ValidationUtils.validateOrThrow(aspect);
      } catch (ModelConversionException e) {
        throw new RuntimeException(
            String.format(
                "Could not deserialize %s for aspect %s",
                mcp.getAspect().getValue(), mcp.getAspectName()));
      }
      log.debug("aspect = {}", aspect);
      return aspect;
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
    UpsertBatchItem that = (UpsertBatchItem) o;
    return urn.equals(that.urn)
        && aspectName.equals(that.aspectName)
        && Objects.equals(systemMetadata, that.systemMetadata)
        && aspect.equals(that.aspect);
  }

  @Override
  public int hashCode() {
    return Objects.hash(urn, aspectName, systemMetadata, aspect);
  }

  @Override
  public String toString() {
    return "UpsertBatchItem{"
        + "urn="
        + urn
        + ", aspectName='"
        + aspectName
        + '\''
        + ", systemMetadata="
        + systemMetadata
        + ", aspect="
        + aspect
        + '}';
  }
}
