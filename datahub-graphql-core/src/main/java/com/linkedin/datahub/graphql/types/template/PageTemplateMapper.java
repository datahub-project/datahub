package com.linkedin.datahub.graphql.types.template;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canView;
import static com.linkedin.metadata.Constants.DATAHUB_PAGE_TEMPLATE_PROPERTIES_ASPECT_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.DataHubPageModule;
import com.linkedin.datahub.graphql.generated.DataHubPageTemplate;
import com.linkedin.datahub.graphql.generated.DataHubPageTemplateAssetSummary;
import com.linkedin.datahub.graphql.generated.DataHubPageTemplateRow;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.StructuredPropertyEntity;
import com.linkedin.datahub.graphql.generated.SummaryElement;
import com.linkedin.datahub.graphql.generated.SummaryElementType;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.mappers.MapperUtils;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.template.DataHubPageTemplateProperties;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PageTemplateMapper implements ModelMapper<EntityResponse, DataHubPageTemplate> {

  public static final PageTemplateMapper INSTANCE = new PageTemplateMapper();

  public static DataHubPageTemplate map(
      @Nullable final QueryContext context, @Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(context, entityResponse);
  }

  @Override
  public DataHubPageTemplate apply(
      @Nullable final QueryContext context, @Nonnull final EntityResponse entityResponse) {
    final DataHubPageTemplate result = new DataHubPageTemplate();
    Urn entityUrn = entityResponse.getUrn();

    result.setUrn(entityResponse.getUrn().toString());
    result.setType(EntityType.DATAHUB_PAGE_TEMPLATE);

    EnvelopedAspectMap aspectMap = entityResponse.getAspects();

    // Handle getting deleted template by broken reference (check if required aspect was fetched)
    if (aspectMap.get(DATAHUB_PAGE_TEMPLATE_PROPERTIES_ASPECT_NAME) == null) {
      log.warn("Page Template {} doesn't have required aspects", entityUrn);
      return null;
    }

    MappingHelper<DataHubPageTemplate> mappingHelper = new MappingHelper<>(aspectMap, result);
    mappingHelper.mapToResult(
        DATAHUB_PAGE_TEMPLATE_PROPERTIES_ASPECT_NAME,
        (application, dataMap) -> mapProperties(application, dataMap));

    if (context != null && !canView(context.getOperationContext(), entityUrn)) {
      return AuthorizationUtils.restrictEntity(result, DataHubPageTemplate.class);
    } else {
      return result;
    }
  }

  private void mapProperties(@Nonnull DataHubPageTemplate template, @Nonnull DataMap dataMap) {
    DataHubPageTemplateProperties gmsTemplateProperties =
        new DataHubPageTemplateProperties(dataMap);
    com.linkedin.datahub.graphql.generated.DataHubPageTemplateProperties properties =
        new com.linkedin.datahub.graphql.generated.DataHubPageTemplateProperties();

    List<DataHubPageTemplateRow> rows = new ArrayList<>();
    gmsTemplateProperties
        .getRows()
        .forEach(
            row -> {
              DataHubPageTemplateRow templateRow = new DataHubPageTemplateRow();
              List<DataHubPageModule> modules = new ArrayList<>();
              row.getModules()
                  .forEach(
                      moduleUrn -> {
                        DataHubPageModule module = new DataHubPageModule();
                        module.setUrn(moduleUrn.toString());
                        module.setType(EntityType.DATAHUB_PAGE_MODULE);
                        modules.add(module);
                      });
              templateRow.setModules(modules);
              rows.add(templateRow);
            });
    properties.setRows(rows);

    if (gmsTemplateProperties.getAssetSummary() != null) {
      properties.setAssetSummary(mapAssetSummary(gmsTemplateProperties.getAssetSummary()));
    }

    if (gmsTemplateProperties.hasSurface()) {
      properties.setSurface(PageTemplateSurfaceMapper.map(gmsTemplateProperties.getSurface()));
    }

    if (gmsTemplateProperties.hasVisibility()) {
      properties.setVisibility(
          PageTemplateVisibilityMapper.map(gmsTemplateProperties.getVisibility()));
    }

    if (gmsTemplateProperties.hasCreated()) {
      properties.setCreated(
          MapperUtils.createResolvedAuditStamp(gmsTemplateProperties.getCreated()));
    }

    if (gmsTemplateProperties.hasLastModified()) {
      properties.setLastModified(
          MapperUtils.createResolvedAuditStamp(gmsTemplateProperties.getLastModified()));
    }

    template.setProperties(properties);
  }

  private DataHubPageTemplateAssetSummary mapAssetSummary(
      com.linkedin.template.DataHubPageTemplateAssetSummary input) {
    DataHubPageTemplateAssetSummary assetSummary = new DataHubPageTemplateAssetSummary();
    assetSummary.setSummaryElements(new ArrayList<>());
    if (input.getSummaryElements() != null) {

      List<SummaryElement> summaryElements =
          input.getSummaryElements().stream()
              .map(
                  el -> {
                    SummaryElement summaryElement = new SummaryElement();
                    summaryElement.setElementType(
                        SummaryElementType.valueOf(el.getElementType().toString()));
                    if (el.getStructuredPropertyUrn() != null) {
                      StructuredPropertyEntity structuredProperty = new StructuredPropertyEntity();
                      structuredProperty.setUrn(el.getStructuredPropertyUrn().toString());
                      structuredProperty.setType(EntityType.STRUCTURED_PROPERTY);
                      summaryElement.setStructuredProperty(structuredProperty);
                    }
                    return summaryElement;
                  })
              .collect(Collectors.toList());

      assetSummary.setSummaryElements(summaryElements);
    }

    return assetSummary;
  }
}
