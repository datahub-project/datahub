package com.linkedin.metadata.entity;

import com.datahub.util.RecordUtils;
import com.google.common.base.Preconditions;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.PegasusUtils;
import com.linkedin.mxe.SystemMetadata;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;

import static com.linkedin.metadata.entity.EntityService.DEFAULT_RUN_ID;


@Slf4j
public class EntityUtils {

  private EntityUtils() {
  }

  @Nonnull
  public static String toJsonAspect(@Nonnull final RecordTemplate aspectRecord) {
    return RecordUtils.toJsonString(aspectRecord);
  }

  @Nonnull
  public static RecordTemplate toAspectRecord(
      @Nonnull final Urn entityUrn,
      @Nonnull final String aspectName,
      @Nonnull final String jsonAspect,
      @Nonnull final EntityRegistry entityRegistry) {
    return toAspectRecord(PegasusUtils.urnToEntityName(entityUrn), aspectName, jsonAspect, entityRegistry);
  }

  /**
   * @param entityName
   * @param aspectName
   * @param jsonAspect
   * @param entityRegistry
   * @return a RecordTemplate which has been validated, validation errors are logged as warnings
   */
  public static RecordTemplate toAspectRecord(
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      @Nonnull final String jsonAspect,
      @Nonnull final EntityRegistry entityRegistry) {
    final EntitySpec entitySpec = entityRegistry.getEntitySpec(entityName);
    final AspectSpec aspectSpec = entitySpec.getAspectSpec(aspectName);
    //TODO: aspectSpec can be null here
    Preconditions.checkState(aspectSpec != null, String.format("Aspect %s could not be found", aspectName));
    final RecordDataSchema aspectSchema = aspectSpec.getPegasusSchema();
    RecordTemplate aspectRecord = RecordUtils.toRecordTemplate(aspectSpec.getDataTemplateClass(), jsonAspect);
    RecordTemplateValidator.validate(aspectRecord, validationFailure -> {
      log.warn(String.format("Failed to validate record %s against its schema.", aspectRecord));
    });
    return aspectRecord;
  }

  public static SystemMetadata parseSystemMetadata(String jsonSystemMetadata) {
    if (jsonSystemMetadata == null || jsonSystemMetadata.equals("")) {
      SystemMetadata response = new SystemMetadata();
      response.setRunId(DEFAULT_RUN_ID);
      response.setLastObserved(0);
      return response;
    }
    return RecordUtils.toRecordTemplate(SystemMetadata.class, jsonSystemMetadata);
  }

  /**
   * Check if entity is removed (removed=true in Status aspect) and exists 
   */
  public static boolean checkIfRemoved(EntityService entityService, Urn entityUrn) {
    try {
      
      if (!entityService.exists(entityUrn)) {
        return false;
      }
      
      EnvelopedAspect statusAspect =
          entityService.getLatestEnvelopedAspect(entityUrn.getEntityType(), entityUrn, "status");
      if (statusAspect == null) {
        return false;
      }
      Status status = new Status(statusAspect.getValue().data());
      return status.isRemoved();
    } catch (Exception e) {
      log.error("Error while checking if {} is removed", entityUrn, e);
      return false;
    }
  }
}
