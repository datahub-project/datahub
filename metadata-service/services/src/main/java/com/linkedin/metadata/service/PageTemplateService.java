package com.linkedin.metadata.service;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.key.DataHubPageTemplateKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.template.DataHubPageTemplateProperties;
import com.linkedin.template.DataHubPageTemplateRow;
import com.linkedin.template.DataHubPageTemplateRowArray;
import com.linkedin.template.DataHubPageTemplateSurface;
import com.linkedin.template.DataHubPageTemplateVisibility;
import com.linkedin.template.PageTemplateScope;
import com.linkedin.template.PageTemplateSurfaceType;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class PageTemplateService {
  private final EntityClient entityClient;

  public PageTemplateService(@Nonnull EntityClient entityClient) {
    this.entityClient = entityClient;
  }

  /**
   * Upserts a DataHub page template. If the page template with the provided urn already exists,
   * then it will be overwritten.
   *
   * <p>This method assumes that authorization has already been verified at the calling layer.
   *
   * @return the URN of the new page template.
   */
  public Urn upsertPageTemplate(
      @Nonnull OperationContext opContext,
      @Nullable final String urn,
      @Nonnull final List<DataHubPageTemplateRow> rows,
      @Nonnull final PageTemplateScope scope,
      @Nonnull final PageTemplateSurfaceType surfaceType) {
    Objects.requireNonNull(rows, "rows must not be null");
    Objects.requireNonNull(scope, "scope must not be null");
    Objects.requireNonNull(surfaceType, "surfaceType must not be null");

    // 1. Optionally generate new page template urn
    Urn templateUrn = null;
    if (urn != null) {
      templateUrn = UrnUtils.getUrn(urn);
    } else {
      final String templateId = UUID.randomUUID().toString();
      final DataHubPageTemplateKey key = new DataHubPageTemplateKey().setId(templateId);
      templateUrn =
          EntityKeyUtils.convertEntityKeyToUrn(key, Constants.DATAHUB_PAGE_TEMPLATE_ENTITY_NAME);
    }
    final AuditStamp nowAuditStamp = opContext.getAuditStamp();

    // 2. Build Page Template Properties
    DataHubPageTemplateProperties properties = new DataHubPageTemplateProperties();
    DataHubPageTemplateProperties existingProperties =
        getPageTemplateProperties(opContext, templateUrn);
    if (existingProperties != null) {
      properties = existingProperties;
    } else {
      // if creating a new page template, set the created stamp
      properties.setCreated(nowAuditStamp);
    }

    // ensure that all modules referenced in rows exist
    validatePageTemplateRows(opContext, rows);

    properties.setRows(new DataHubPageTemplateRowArray(rows));

    DataHubPageTemplateSurface surface = new DataHubPageTemplateSurface();
    surface.setSurfaceType(surfaceType);
    properties.setSurface(surface);

    DataHubPageTemplateVisibility visibility = new DataHubPageTemplateVisibility();
    visibility.setScope(scope);
    properties.setVisibility(visibility);

    properties.setLastModified(nowAuditStamp);

    // 4. Write changes to GMS
    try {
      final List<MetadataChangeProposal> aspectsToIngest = new ArrayList<>();
      aspectsToIngest.add(
          AspectUtils.buildMetadataChangeProposal(
              templateUrn, Constants.DATAHUB_PAGE_TEMPLATE_PROPERTIES_ASPECT_NAME, properties));
      entityClient.batchIngestProposals(opContext, aspectsToIngest, false);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to upsert PageTemplate with urn %s", templateUrn), e);
    }
    return templateUrn;
  }

  @Nullable
  public DataHubPageTemplateProperties getPageTemplateProperties(
      @Nonnull OperationContext opContext, @Nonnull final Urn templateUrn) {
    Objects.requireNonNull(templateUrn, "templateUrn must not be null");
    final EntityResponse response = getPageTemplateEntityResponse(opContext, templateUrn);
    if (response != null
        && response
            .getAspects()
            .containsKey(Constants.DATAHUB_PAGE_TEMPLATE_PROPERTIES_ASPECT_NAME)) {
      return new DataHubPageTemplateProperties(
          response
              .getAspects()
              .get(Constants.DATAHUB_PAGE_TEMPLATE_PROPERTIES_ASPECT_NAME)
              .getValue()
              .data());
    }
    // No aspect found
    return null;
  }

  @Nullable
  public EntityResponse getPageTemplateEntityResponse(
      @Nonnull OperationContext opContext, @Nonnull final Urn templateUrn) {
    try {
      return entityClient.getV2(
          opContext, Constants.DATAHUB_PAGE_TEMPLATE_ENTITY_NAME, templateUrn, null, false);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to retrieve PageTemplate with urn %s", templateUrn), e);
    }
  }

  private void validatePageTemplateRows(
      @Nonnull OperationContext opContext, @Nonnull final List<DataHubPageTemplateRow> rows) {
    rows.forEach(
        row -> {
          row.getModules()
              .forEach(
                  module -> {
                    try {
                      if (!entityClient.exists(opContext, module)) {
                        throw new RuntimeException(
                            String.format(
                                "Failed to add page template rows as module with urn %s does not exist",
                                module));
                      }
                    } catch (Exception e) {
                      throw new RuntimeException("Failed validating template rows", e);
                    }
                  });
        });
  }
}
