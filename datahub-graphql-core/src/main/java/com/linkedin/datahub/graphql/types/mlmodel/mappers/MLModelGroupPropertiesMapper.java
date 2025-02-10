package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.MLModelGroupProperties;
import com.linkedin.datahub.graphql.generated.MLModelLineageInfo;
import com.linkedin.datahub.graphql.types.common.mappers.CustomPropertiesMapper;
import com.linkedin.datahub.graphql.types.common.mappers.TimeStampToAuditStampMapper;
import com.linkedin.datahub.graphql.types.mappers.EmbeddedModelMapper;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class MLModelGroupPropertiesMapper
    implements EmbeddedModelMapper<
        com.linkedin.ml.metadata.MLModelGroupProperties, MLModelGroupProperties> {

  public static final MLModelGroupPropertiesMapper INSTANCE = new MLModelGroupPropertiesMapper();

  public static MLModelGroupProperties map(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.ml.metadata.MLModelGroupProperties mlModelGroupProperties,
      @Nonnull Urn entityUrn) {
    return INSTANCE.apply(context, mlModelGroupProperties, entityUrn);
  }

  @Override
  public MLModelGroupProperties apply(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.ml.metadata.MLModelGroupProperties mlModelGroupProperties,
      @Nonnull Urn entityUrn) {
    final MLModelGroupProperties result = new MLModelGroupProperties();

    result.setDescription(mlModelGroupProperties.getDescription());
    if (mlModelGroupProperties.getVersion() != null) {
      result.setVersion(VersionTagMapper.map(context, mlModelGroupProperties.getVersion()));
    }
    result.setCreatedAt(mlModelGroupProperties.getCreatedAt());
    if (mlModelGroupProperties.hasCreated()) {
      result.setCreated(
          TimeStampToAuditStampMapper.map(context, mlModelGroupProperties.getCreated()));
    }
    if (mlModelGroupProperties.getName() != null) {
      result.setName(mlModelGroupProperties.getName());
    } else {
      // backfill name from URN for backwards compatibility
      result.setName(entityUrn.getEntityKey().get(1)); // indexed access is safe here
    }

    if (mlModelGroupProperties.hasLastModified()) {
      result.setLastModified(
          TimeStampToAuditStampMapper.map(context, mlModelGroupProperties.getLastModified()));
    }

    result.setCustomProperties(
        CustomPropertiesMapper.map(mlModelGroupProperties.getCustomProperties(), entityUrn));

    final MLModelLineageInfo lineageInfo = new MLModelLineageInfo();
    if (mlModelGroupProperties.hasTrainingJobs()) {
      lineageInfo.setTrainingJobs(
          mlModelGroupProperties.getTrainingJobs().stream()
              .map(urn -> urn.toString())
              .collect(Collectors.toList()));
    }
    if (mlModelGroupProperties.hasDownstreamJobs()) {
      lineageInfo.setDownstreamJobs(
          mlModelGroupProperties.getDownstreamJobs().stream()
              .map(urn -> urn.toString())
              .collect(Collectors.toList()));
    }
    result.setMlModelLineageInfo(lineageInfo);

    return result;
  }
}
