package com.linkedin.metadata.entity.ebean.batch;

import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;
import static com.linkedin.metadata.entity.AspectUtils.validateAspect;

import com.datahub.util.exception.ModelConversionException;
import com.github.fge.jsonpatch.JsonPatchException;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.SystemAspect;
import com.linkedin.metadata.aspect.batch.UpsertItem;
import com.linkedin.metadata.aspect.patch.template.common.GenericPatchTemplate;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectRetriever;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.utils.DefaultAspectsUtil;
import com.linkedin.metadata.entity.EntityAspect;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.entity.validation.ValidationUtils;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import java.io.IOException;
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

  public static MCPUpsertBatchItem fromPatch(
      @Nonnull Urn urn,
      @Nonnull AspectSpec aspectSpec,
      @Nullable RecordTemplate recordTemplate,
      GenericPatchTemplate<? extends RecordTemplate> genericPatchTemplate,
      @Nonnull AuditStamp auditStamp,
      AspectRetriever aspectRetriever) {
    MCPUpsertBatchItem.MCPUpsertBatchItemBuilder builder =
        MCPUpsertBatchItem.builder()
            .urn(urn)
            .auditStamp(auditStamp)
            .aspectName(aspectSpec.getName());

    RecordTemplate currentValue =
        recordTemplate != null ? recordTemplate : genericPatchTemplate.getDefault();

    try {
      builder.recordTemplate(genericPatchTemplate.applyPatch(currentValue));
    } catch (JsonPatchException | IOException e) {
      throw new RuntimeException(e);
    }

    return builder.build(aspectRetriever);
  }

  // urn an urn associated with the new aspect
  @Nonnull private final Urn urn;

  // aspectName name of the aspect being inserted
  @Nonnull private final String aspectName;

  @Nonnull private final RecordTemplate recordTemplate;

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

  @Override
  @Nonnull
  public MetadataChangeProposal getMetadataChangeProposal() {
    if (metadataChangeProposal != null) {
      return metadataChangeProposal;
    } else {
      return DefaultAspectsUtil.getProposalFromAspect(
          getAspectName(), getRecordTemplate(), null, this);
    }
  }

  public void applyMutationHooks(
      @Nullable RecordTemplate oldAspectValue,
      @Nullable SystemMetadata oldSystemMetadata,
      @Nonnull AspectRetriever aspectRetriever) {
    // add audit stamp/system meta if needed
    for (MutationHook mutationHook :
        aspectRetriever
            .getEntityRegistry()
            .getMutationHooks(getChangeType(), entitySpec.getName(), aspectSpec.getName())) {
      mutationHook.applyMutation(
          getChangeType(),
          entitySpec,
          aspectSpec,
          oldAspectValue,
          recordTemplate,
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
    latest.setMetadata(EntityUtils.toJsonAspect(getRecordTemplate()));
    latest.setUrn(getUrn().toString());
    latest.setVersion(ASPECT_LATEST_VERSION);
    latest.setCreatedOn(new Timestamp(auditStamp.getTime()));
    latest.setCreatedBy(auditStamp.getActor().toString());
    return latest.asSystemAspect();
  }

  @Override
  public void validatePreCommit(
      @Nullable RecordTemplate previous, @Nonnull AspectRetriever aspectRetriever)
      throws AspectValidationException {

    for (AspectPayloadValidator validator :
        aspectRetriever
            .getEntityRegistry()
            .getAspectPayloadValidators(
                getChangeType(), entitySpec.getName(), aspectSpec.getName())) {
      validator.validatePreCommit(
          getChangeType(), urn, getAspectSpec(), previous, this.recordTemplate, aspectRetriever);
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
    public MCPUpsertBatchItem build(AspectRetriever aspectRetriever) {
      EntityUtils.validateUrn(aspectRetriever.getEntityRegistry(), this.urn);
      log.debug("entity type = {}", this.urn.getEntityType());

      entitySpec(aspectRetriever.getEntityRegistry().getEntitySpec(this.urn.getEntityType()));
      log.debug("entity spec = {}", this.entitySpec);

      aspectSpec(ValidationUtils.validate(this.entitySpec, this.aspectName));
      log.debug("aspect spec = {}", this.aspectSpec);

      ValidationUtils.validateRecordTemplate(
          ChangeType.UPSERT,
          this.entitySpec,
          this.aspectSpec,
          this.urn,
          this.recordTemplate,
          aspectRetriever);

      return new MCPUpsertBatchItem(
          this.urn,
          this.aspectName,
          this.recordTemplate,
          SystemMetadataUtils.generateSystemMetadataIfEmpty(this.systemMetadata),
          this.auditStamp,
          this.metadataChangeProposal,
          this.entitySpec,
          this.aspectSpec);
    }

    public static MCPUpsertBatchItem build(
        MetadataChangeProposal mcp, AuditStamp auditStamp, AspectRetriever aspectRetriever) {
      if (!mcp.getChangeType().equals(ChangeType.UPSERT)) {
        throw new IllegalArgumentException(
            "Invalid MCP, this class only supports change type of UPSERT.");
      }

      log.debug("entity type = {}", mcp.getEntityType());
      EntitySpec entitySpec =
          aspectRetriever.getEntityRegistry().getEntitySpec(mcp.getEntityType());
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
          .recordTemplate(convertToRecordTemplate(mcp, aspectSpec))
          .build(aspectRetriever);
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
        && recordTemplate.equals(that.recordTemplate);
  }

  @Override
  public int hashCode() {
    return Objects.hash(urn, aspectName, systemMetadata, recordTemplate);
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
        + ", recordTemplate="
        + recordTemplate
        + '}';
  }
}
