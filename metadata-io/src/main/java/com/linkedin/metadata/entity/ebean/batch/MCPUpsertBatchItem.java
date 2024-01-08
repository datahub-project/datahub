package com.linkedin.metadata.entity.ebean.batch;

import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;
import static com.linkedin.metadata.entity.AspectUtils.validateAspect;

import com.datahub.util.exception.ModelConversionException;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.SystemAspect;
import com.linkedin.metadata.aspect.batch.UpsertItem;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectRetriever;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.entity.EntityAspect;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.entity.validation.ValidationUtils;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import java.sql.Timestamp;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@Builder(toBuilder = true)
public class MCPUpsertBatchItem extends UpsertItem {

  // urn an urn associated with the new aspect
  @Nonnull private final Urn urn;

  // aspectName name of the aspect being inserted
  @Nonnull private final String aspectName;

  @Nonnull private final RecordTemplate aspect;

  @Nonnull private final SystemMetadata systemMetadata;

  @Nonnull private final AuditStamp auditStamp;

  @Nullable private final MetadataChangeProposal metadataChangeProposal;

  // derived
  @Nonnull private final EntitySpec entitySpec;
  @Nonnull private final AspectSpec aspectSpec;

  @Nonnull
  @Override
  public ChangeType getChangeType() {
    return ChangeType.UPSERT;
  }

  public void applyMutationHooks(
      @Nullable RecordTemplate oldAspectValue,
      @Nullable SystemMetadata oldSystemMetadata,
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull AspectRetriever aspectRetriever) {
    // add audit stamp/system meta if needed
    for (MutationHook mutationHook :
        entityRegistry.getMutationHooks(
            getChangeType(), entitySpec.getName(), aspectSpec.getName())) {
      mutationHook.applyMutation(
          getChangeType(),
          entitySpec,
          aspectSpec,
          oldAspectValue,
          aspect,
          oldSystemMetadata,
          systemMetadata,
          auditStamp,
          aspectRetriever);
    }
  }

  @Override
  public SystemAspect toLatestEntityAspect() {
    EntityAspect latest = new EntityAspect();
    latest.setAspect(getAspectName());
    latest.setMetadata(EntityUtils.toJsonAspect(getAspect()));
    latest.setUrn(getUrn().toString());
    latest.setVersion(ASPECT_LATEST_VERSION);
    latest.setCreatedOn(new Timestamp(auditStamp.getTime()));
    latest.setCreatedBy(auditStamp.getActor().toString());
    return latest.asSystemAspect();
  }

  @Override
  public void validatePreCommit(
      @Nullable RecordTemplate previous,
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull AspectRetriever aspectRetriever)
      throws AspectValidationException {

    for (AspectPayloadValidator validator :
        entityRegistry.getAspectPayloadValidators(
            getChangeType(), entitySpec.getName(), aspectSpec.getName())) {
      validator.validatePreCommit(
          getChangeType(), urn, getAspectSpec(), previous, this.aspect, aspectRetriever);
    }
  }

  public static class MCPUpsertBatchItemBuilder {

    // Ensure use of other builders
    private MCPUpsertBatchItem build() {
      return null;
    }

    public MCPUpsertBatchItemBuilder systemMetadata(SystemMetadata systemMetadata) {
      this.systemMetadata = SystemMetadataUtils.generateSystemMetadataIfEmpty(systemMetadata);
      return this;
    }

    @SneakyThrows
    public MCPUpsertBatchItem build(
        EntityRegistry entityRegistry, AspectRetriever aspectRetriever) {
      EntityUtils.validateUrn(entityRegistry, this.urn);
      log.debug("entity type = {}", this.urn.getEntityType());

      entitySpec(entityRegistry.getEntitySpec(this.urn.getEntityType()));
      log.debug("entity spec = {}", this.entitySpec);

      aspectSpec(ValidationUtils.validate(this.entitySpec, this.aspectName));
      log.debug("aspect spec = {}", this.aspectSpec);

      ValidationUtils.validateRecordTemplate(
          ChangeType.UPSERT,
          entityRegistry,
          this.entitySpec,
          this.aspectSpec,
          this.urn,
          this.aspect,
          aspectRetriever);

      return new MCPUpsertBatchItem(
          this.urn,
          this.aspectName,
          this.aspect,
          SystemMetadataUtils.generateSystemMetadataIfEmpty(this.systemMetadata),
          this.auditStamp,
          this.metadataChangeProposal,
          this.entitySpec,
          this.aspectSpec);
    }

    public static MCPUpsertBatchItem build(
        MetadataChangeProposal mcp,
        AuditStamp auditStamp,
        EntityRegistry entityRegistry,
        AspectRetriever aspectRetriever) {
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

      return MCPUpsertBatchItem.builder()
          .urn(urn)
          .aspectName(mcp.getAspectName())
          .systemMetadata(
              SystemMetadataUtils.generateSystemMetadataIfEmpty(mcp.getSystemMetadata()))
          .metadataChangeProposal(mcp)
          .auditStamp(auditStamp)
          .aspect(convertToRecordTemplate(mcp, aspectSpec))
          .build(entityRegistry, aspectRetriever);
    }

    private MCPUpsertBatchItemBuilder entitySpec(EntitySpec entitySpec) {
      this.entitySpec = entitySpec;
      return this;
    }

    private MCPUpsertBatchItemBuilder aspectSpec(AspectSpec aspectSpec) {
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
    MCPUpsertBatchItem that = (MCPUpsertBatchItem) o;
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
