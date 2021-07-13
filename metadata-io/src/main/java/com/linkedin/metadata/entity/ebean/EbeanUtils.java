package com.linkedin.metadata.entity.ebean;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.PegasusUtils;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.PegasusUtils.getDataTemplateClassFromSchema;


public class EbeanUtils {
  private EbeanUtils() {
  }

  @Nonnull
  public static String toJsonAspect(@Nonnull final RecordTemplate aspectRecord) {
    return RecordUtils.toJsonString(aspectRecord);
  }

  @Nonnull
  public static RecordTemplate toAspectRecord(@Nonnull final Urn entityUrn, @Nonnull final String aspectName,
      @Nonnull final String jsonAspect, @Nonnull final EntityRegistry entityRegistry) {
    return toAspectRecord(PegasusUtils.urnToEntityName(entityUrn), aspectName, jsonAspect, entityRegistry);
  }
  public static RecordTemplate toAspectRecord(@Nonnull final String entityName, @Nonnull final String aspectName,
      @Nonnull final String jsonAspect, @Nonnull final EntityRegistry entityRegistry) {
    final EntitySpec entitySpec = entityRegistry.getEntitySpec(entityName);
    final AspectSpec aspectSpec = entitySpec.getAspectSpec(aspectName);
    final RecordDataSchema aspectSchema = aspectSpec.getPegasusSchema();
    return RecordUtils.toRecordTemplate(getDataTemplateClassFromSchema(aspectSchema, RecordTemplate.class), jsonAspect);
  }
}
