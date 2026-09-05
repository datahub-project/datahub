package com.linkedin.metadata.entity.ebean.batch;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.entity.validation.ValidationApiUtils;
import com.linkedin.metadata.entity.validation.ValidationException;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/** Represents an unvalidated wrapped MCP */
@Slf4j
@Getter
@Builder(toBuilder = true)
public class ProposedItem implements MCPItem {
  @Nonnull private final Urn urn;
  @Nonnull private final MetadataChangeProposal metadataChangeProposal;
  @Nonnull private final AuditStamp auditStamp;
  // derived
  @Nonnull private EntitySpec entitySpec;
  @Nullable private AspectSpec aspectSpec;

  @Nonnull
  @Override
  public String getAspectName() {
    if (metadataChangeProposal.getAspectName() != null) {
      return metadataChangeProposal.getAspectName();
    } else {
      return MCPItem.super.getAspectName();
    }
  }

  @Nullable
  public AspectSpec getAspectSpec() {
    if (aspectSpec != null) {
      return aspectSpec;
    }
    if (entitySpec.getAspectSpecMap().containsKey(getAspectName())) {
      return entitySpec.getAspectSpecMap().get(getAspectName());
    }
    return null;
  }

  @Nullable
  @Override
  public RecordTemplate getRecordTemplate() {
    if (getAspectSpec() != null) {
      return GenericRecordUtils.deserializeAspect(
          getMetadataChangeProposal().getAspect().getValue(),
          getMetadataChangeProposal().getAspect().getContentType(),
          getAspectSpec());
    }
    return null;
  }

  @Nonnull
  @Override
  public SystemMetadata getSystemMetadata() {
    if (metadataChangeProposal.getSystemMetadata() == null) {
      metadataChangeProposal.setSystemMetadata(SystemMetadataUtils.createDefaultSystemMetadata());
    }
    return metadataChangeProposal.getSystemMetadata();
  }

  @Nonnull
  @Override
  public ChangeType getChangeType() {
    return metadataChangeProposal.getChangeType();
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

    ProposedItem that = (ProposedItem) o;
    return metadataChangeProposal.equals(that.metadataChangeProposal)
        && auditStamp.equals(that.auditStamp);
  }

  @Override
  public int hashCode() {
    int result = metadataChangeProposal.hashCode();
    result = 31 * result + auditStamp.hashCode();
    return result;
  }

  public static class ProposedItemBuilder {
    // Ensure use of other builders
    private ProposedItem build() {
      return null;
    }

    public ProposedItem build(
        @Nonnull MetadataChangeProposal metadataChangeProposal,
        AuditStamp auditStamp,
        @Nonnull EntityRegistry entityRegistry) {
      return buildInternal(metadataChangeProposal, auditStamp, entityRegistry, true);
    }

    /**
     * Same as {@link #build} but does not require the aspect to be registered on the entity type.
     * Used by alternate MCP validation so unknown aspects can reach {@code IgnoreUnknownMutator}.
     */
    public ProposedItem buildAllowingUnknownAspect(
        @Nonnull MetadataChangeProposal metadataChangeProposal,
        AuditStamp auditStamp,
        @Nonnull EntityRegistry entityRegistry) {
      return buildInternal(metadataChangeProposal, auditStamp, entityRegistry, false);
    }

    private ProposedItem buildInternal(
        @Nonnull MetadataChangeProposal metadataChangeProposal,
        AuditStamp auditStamp,
        @Nonnull EntityRegistry entityRegistry,
        boolean requireRegisteredAspect) {

      if (requireRegisteredAspect) {
        this.metadataChangeProposal =
            ValidationApiUtils.validateMCP(entityRegistry, metadataChangeProposal);
      } else {
        this.metadataChangeProposal =
            validateMcpWithoutAspect(entityRegistry, metadataChangeProposal);
      }
      this.auditStamp = auditStamp;
      SystemMetadata systemMetadata =
          SystemMetadataUtils.setAspectModified(
              SystemMetadataUtils.generateSystemMetadataIfEmpty(
                  this.metadataChangeProposal.getSystemMetadata()),
              auditStamp);
      this.metadataChangeProposal.setSystemMetadata(systemMetadata);

      this.urn = this.metadataChangeProposal.getEntityUrn();
      log.debug("entity type = {}", this.urn.getEntityType());

      entitySpec(entityRegistry.getEntitySpec(this.urn.getEntityType()));
      log.debug("entity spec = {}", this.entitySpec);

      aspectSpec(entitySpec.getAspectSpec(this.metadataChangeProposal.getAspectName()));
      log.debug("aspect spec = {}", this.aspectSpec);

      return new ProposedItem(
          this.urn, this.metadataChangeProposal, this.auditStamp, this.entitySpec, this.aspectSpec);
    }

    private static MetadataChangeProposal validateMcpWithoutAspect(
        @Nonnull EntityRegistry entityRegistry, MetadataChangeProposal mcp) {
      if (mcp == null) {
        throw new UnsupportedOperationException("MetadataChangeProposal is required.");
      }

      final EntitySpec entitySpec;
      final Urn urn;
      if (mcp.getEntityUrn() != null) {
        urn = mcp.getEntityUrn();
        entitySpec = ValidationApiUtils.validateEntity(entityRegistry, urn.getEntityType());
      } else {
        entitySpec = ValidationApiUtils.validateEntity(entityRegistry, mcp.getEntityType());
        urn = EntityKeyUtils.getUrnFromProposal(mcp, entitySpec.getKeyAspectSpec());
        mcp.setEntityUrn(urn);
      }

      if (mcp.getEntityType().equalsIgnoreCase(urn.getEntityType())) {
        mcp.setEntityType(urn.getEntityType());
      } else {
        throw new ValidationException(
            String.format(
                "URN entity type does not match MCP entity type. %s != %s",
                urn.getEntityType(), mcp.getEntityType()));
      }

      ValidationApiUtils.validateUrn(entityRegistry, urn);
      return mcp;
    }
  }
}
