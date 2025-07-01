package com.linkedin.datahub.graphql.types.module;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canView;
import static com.linkedin.metadata.Constants.DATAHUB_PAGE_MODULE_PROPERTIES_ASPECT_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.DataHubPageModule;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.mappers.MapperUtils;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.module.DataHubPageModuleProperties;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class PageModuleMapper implements ModelMapper<EntityResponse, DataHubPageModule> {

  public static final PageModuleMapper INSTANCE = new PageModuleMapper();

  public static DataHubPageModule map(
      @Nullable final QueryContext context, @Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(context, entityResponse);
  }

  @Override
  public DataHubPageModule apply(
      @Nullable final QueryContext context, @Nonnull final EntityResponse entityResponse) {
    final DataHubPageModule result = new DataHubPageModule();
    Urn entityUrn = entityResponse.getUrn();

    result.setUrn(entityResponse.getUrn().toString());
    result.setType(EntityType.DATAHUB_PAGE_MODULE);

    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    MappingHelper<DataHubPageModule> mappingHelper = new MappingHelper<>(aspectMap, result);
    mappingHelper.mapToResult(
        DATAHUB_PAGE_MODULE_PROPERTIES_ASPECT_NAME,
        (module, dataMap) -> mapProperties(module, dataMap));

    if (context != null && !canView(context.getOperationContext(), entityUrn)) {
      return AuthorizationUtils.restrictEntity(result, DataHubPageModule.class);
    } else {
      return result;
    }
  }

  private void mapProperties(@Nonnull DataHubPageModule module, @Nonnull DataMap dataMap) {
    DataHubPageModuleProperties gmsModuleProperties = new DataHubPageModuleProperties(dataMap);
    com.linkedin.datahub.graphql.generated.DataHubPageModuleProperties properties =
        new com.linkedin.datahub.graphql.generated.DataHubPageModuleProperties();

    // Map name
    if (gmsModuleProperties.hasName()) {
      properties.setName(gmsModuleProperties.getName());
    }

    // Map type
    if (gmsModuleProperties.hasType()) {
      properties.setType(PageModuleTypeMapper.map(gmsModuleProperties.getType()));
    }

    // Map visibility
    if (gmsModuleProperties.hasVisibility()) {
      properties.setVisibility(PageModuleVisibilityMapper.map(gmsModuleProperties.getVisibility()));
    }

    // Map params
    if (gmsModuleProperties.hasParams()) {
      properties.setParams(PageModuleParamsMapper.map(gmsModuleProperties.getParams()));
    }

    // Map created audit stamp
    if (gmsModuleProperties.hasCreated()) {
      properties.setCreated(MapperUtils.createResolvedAuditStamp(gmsModuleProperties.getCreated()));
    }

    // Map last modified audit stamp
    if (gmsModuleProperties.hasLastModified()) {
      properties.setLastModified(
          MapperUtils.createResolvedAuditStamp(gmsModuleProperties.getLastModified()));
    }

    module.setProperties(properties);
  }
}
