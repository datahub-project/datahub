package io.datahubproject.iceberg.catalog;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.aspect.validation.ConditionalWriteValidator.HTTP_HEADER_IF_VERSION_MATCH;
import static com.linkedin.metadata.utils.GenericRecordUtils.serializeAspect;
import static io.datahubproject.iceberg.catalog.Utils.*;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

public class IcebergBatch {
  private List<MetadataChangeProposal> mcps = new ArrayList<>();

  @Getter private final AuditStamp auditStamp;

  private final OperationContext operationContext;

  public IcebergBatch(OperationContext operationContext) {
    this.operationContext = operationContext;
    this.auditStamp =
        new AuditStamp()
            .setActor(operationContext.getActorContext().getActorUrn())
            .setTime(System.currentTimeMillis());
  }

  @VisibleForTesting
  IcebergBatch(OperationContext operationContext, AuditStamp auditStamp) {
    this.operationContext = operationContext;
    this.auditStamp = auditStamp;
  }

  @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
  abstract class EntityBatch {
    private final Urn urn;
    private final String entityName;

    public void aspect(String aspectName, RecordTemplate aspectData) {
      mcps.add(newMcp(urn, entityName, aspectName, aspectData, changeType()));
    }

    public void platformInstance(String platformInstanceName) {
      DataPlatformInstance platformInstance =
          new DataPlatformInstance()
              .setPlatform(platformUrn())
              .setInstance(platformInstanceUrn(platformInstanceName));

      aspect(DATA_PLATFORM_INSTANCE_ASPECT_NAME, platformInstance);
    }

    abstract ChangeType changeType();
  }

  class CreateEntityBatch extends EntityBatch {

    private CreateEntityBatch(Urn urn, String entityName) {
      super(urn, entityName);
    }

    @Override
    ChangeType changeType() {
      return ChangeType.CREATE;
    }
  }

  class UpdateEntityBatch extends EntityBatch {

    private UpdateEntityBatch(Urn urn, String entityName) {
      super(urn, entityName);
    }

    @Override
    ChangeType changeType() {
      return ChangeType.UPDATE;
    }

    @Override
    public void platformInstance(String platformInstanceName) {
      // disallow updates of platform instance
      throw new UnsupportedOperationException();
    }
  }

  public EntityBatch createEntity(
      Urn urn, String entityName, String creationAspectName, RecordTemplate aspectData) {
    mcps.add(newMcp(urn, entityName, creationAspectName, aspectData, ChangeType.CREATE_ENTITY));
    mcps.add(
        newMcp(
            urn,
            entityName,
            STATUS_ASPECT_NAME,
            new Status().setRemoved(false),
            ChangeType.CREATE));
    return new CreateEntityBatch(urn, entityName);
  }

  public EntityBatch updateEntity(Urn urn, String entityName) {
    return new UpdateEntityBatch(urn, entityName);
  }

  public EntityBatch conditionalUpdateEntity(
      Urn urn,
      String entityName,
      String aspectName,
      RecordTemplate aspectData,
      String existingVersion) {
    MetadataChangeProposal mcp = newMcp(urn, entityName, aspectName, aspectData, ChangeType.UPDATE);
    mcp.getHeaders().put(HTTP_HEADER_IF_VERSION_MATCH, existingVersion);
    mcps.add(mcp);
    return new UpdateEntityBatch(urn, entityName);
  }

  public void softDeleteEntity(Urn urn, String entityName) {
    // UPSERT instead of UPDATE here, for backward compatibility
    // i.e. if there are existing datasets without Status aspect
    mcps.add(
        newMcp(
            urn, entityName, STATUS_ASPECT_NAME, new Status().setRemoved(true), ChangeType.UPSERT));
  }

  private static MetadataChangeProposal newMcp(
      Urn urn,
      String entityName,
      String aspectName,
      RecordTemplate aspectData,
      ChangeType changeType) {

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(urn);
    mcp.setEntityType(entityName);
    mcp.setAspectName(aspectName);
    mcp.setHeaders(new StringMap(Map.of(SYNC_INDEX_UPDATE_HEADER_NAME, Boolean.toString(true))));
    mcp.setAspect(serializeAspect(aspectData));
    mcp.setChangeType(changeType);
    return mcp;
  }

  public AspectsBatch asAspectsBatch() {
    return AspectsBatchImpl.builder()
        .mcps(mcps, auditStamp, operationContext.getRetrieverContext())
        .build();
  }

  @VisibleForTesting
  List<MetadataChangeProposal> getMcps() {
    return mcps;
  }
}
