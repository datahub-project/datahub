package com.linkedin.datahub.graphql.types.file;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canView;
import static com.linkedin.metadata.Constants.DATAHUB_FILE_INFO_ASPECT_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.DataHubFile;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.SchemaFieldEntity;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.mappers.MapperUtils;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.file.DataHubFileInfo;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataHubFileMapper implements ModelMapper<EntityResponse, DataHubFile> {

  public static final DataHubFileMapper INSTANCE = new DataHubFileMapper();

  public static DataHubFile map(
      @Nullable final QueryContext context, @Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(context, entityResponse);
  }

  @Override
  public DataHubFile apply(
      @Nullable final QueryContext context, @Nonnull final EntityResponse entityResponse) {
    final DataHubFile result = new DataHubFile();
    Urn entityUrn = entityResponse.getUrn();

    result.setUrn(entityResponse.getUrn().toString());
    result.setType(EntityType.DATAHUB_FILE);

    EnvelopedAspectMap aspectMap = entityResponse.getAspects();

    // Handle getting deleted file by broken reference (check if required aspect was fetched)
    if (aspectMap.get(DATAHUB_FILE_INFO_ASPECT_NAME) == null) {
      log.warn("DataHubFile {} doesn't have required aspects", entityUrn);
      return null;
    }

    MappingHelper<DataHubFile> mappingHelper = new MappingHelper<>(aspectMap, result);
    mappingHelper.mapToResult(
        DATAHUB_FILE_INFO_ASPECT_NAME, (file, dataMap) -> mapFileInfo(file, dataMap));

    if (context != null && !canView(context.getOperationContext(), entityUrn)) {
      return AuthorizationUtils.restrictEntity(result, DataHubFile.class);
    } else {
      return result;
    }
  }

  private void mapFileInfo(@Nonnull DataHubFile file, @Nonnull DataMap dataMap) {
    DataHubFileInfo gmsFileInfo = new DataHubFileInfo(dataMap);
    com.linkedin.datahub.graphql.generated.DataHubFileInfo graphqlFileInfo =
        new com.linkedin.datahub.graphql.generated.DataHubFileInfo();

    graphqlFileInfo.setStorageBucket(gmsFileInfo.getStorageBucket());
    graphqlFileInfo.setStorageKey(gmsFileInfo.getStorageKey());
    graphqlFileInfo.setOriginalFileName(gmsFileInfo.getOriginalFileName());
    graphqlFileInfo.setMimeType(gmsFileInfo.getMimeType());
    graphqlFileInfo.setSizeInBytes(gmsFileInfo.getSizeInBytes());
    graphqlFileInfo.setScenario(
        com.linkedin.datahub.graphql.generated.UploadDownloadScenario.valueOf(
            gmsFileInfo.getScenario().toString()));

    if (gmsFileInfo.getReferencedByAsset() != null) {
      Urn assetUrn = gmsFileInfo.getReferencedByAsset();
      Entity partialEntity = UrnToEntityMapper.map(null, assetUrn);
      graphqlFileInfo.setReferencedByAsset(partialEntity);
    }

    if (gmsFileInfo.getSchemaField() != null) {
      Urn schemaFieldUrn = gmsFileInfo.getSchemaField();
      SchemaFieldEntity partialSchemaField = new SchemaFieldEntity();
      partialSchemaField.setUrn(schemaFieldUrn.toString());
      partialSchemaField.setType(EntityType.SCHEMA_FIELD);
      graphqlFileInfo.setSchemaField(partialSchemaField);
    }

    if (gmsFileInfo.hasCreated()) {
      graphqlFileInfo.setCreated(MapperUtils.createResolvedAuditStamp(gmsFileInfo.getCreated()));
    }

    file.setInfo(graphqlFileInfo);
  }
}
