package com.linkedin.metadata.entity.ebean.batch;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.mxe.GenericAspect;
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
  @Nonnull private final ChangeType changeType;
  // urn an urn associated with the new aspect
  @Nonnull private final Urn urn;
  @Nullable private final MetadataChangeProposal metadataChangeProposal;
  @Nonnull private SystemMetadata systemMetadata;
  @Nonnull private final AuditStamp auditStamp;
  @Nonnull private final RecordTemplate recordTemplate;
  // derived
  @Nonnull private EntitySpec entitySpec;
  @Nullable private AspectSpec aspectSpec;

  public static class ProposedItemBuilder {
    @Nonnull
    public MetadataChangeProposal getMetadataChangeProposal() {
      if (this.changeType == null) {
        this.changeType = ChangeType.UPSERT;
      }
      if (metadataChangeProposal != null) {
        return metadataChangeProposal;
      } else {
        final MetadataChangeProposal mcp = new MetadataChangeProposal();
        mcp.setEntityUrn(this.urn);
        mcp.setChangeType(this.changeType);
        mcp.setEntityType(this.entitySpec.getName());
        mcp.setAspectName(GenericAspect.dataSchema().getName());
        mcp.setAspect((GenericAspect) this.recordTemplate);
        mcp.setSystemMetadata(this.systemMetadata);
        mcp.setEntityKeyAspect(
            GenericRecordUtils.serializeAspect(
                EntityKeyUtils.convertUrnToEntityKey(this.urn, entitySpec.getKeyAspectSpec())));
        return mcp;
      }
    }

    public ProposedItem build() {
      StringMap headersMap =
          getMetadataChangeProposal().getHeaders() != null
              ? getMetadataChangeProposal().getHeaders()
              : new StringMap();
      headersMap.put(SKIP_VALIDATION_HEADER, "true");
      getMetadataChangeProposal().setHeaders(headersMap);
      return new ProposedItem(
          this.changeType,
          this.urn,
          getMetadataChangeProposal(),
          SystemMetadataUtils.generateSystemMetadataIfEmpty(this.systemMetadata),
          this.auditStamp,
          this.recordTemplate,
          this.entitySpec,
          this.aspectSpec);
    }

    public static ProposedItem build(
        MetadataChangeProposal mcp, AuditStamp auditStamp, AspectRetriever aspectRetriever) {
      EntitySpec entitySpec =
          aspectRetriever.getEntityRegistry().getEntitySpec(mcp.getEntityType());

      Urn urn = mcp.getEntityUrn();
      if (urn == null) {
        urn = EntityKeyUtils.getUrnFromProposal(mcp, entitySpec.getKeyAspectSpec());
      }

      return ProposedItem.builder()
          .changeType(mcp.getChangeType())
          .urn(urn)
          .systemMetadata(
              SystemMetadataUtils.generateSystemMetadataIfEmpty(mcp.getSystemMetadata()))
          .metadataChangeProposal(mcp)
          .auditStamp(auditStamp)
          .recordTemplate(mcp.getAspect())
          .entitySpec(entitySpec)
          .aspectSpec(null)
          .build();
    }
  }
}
