package com.linkedin.metadata.service;

import com.datahub.authorization.AuthUtil;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
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
import io.datahubproject.openapi.exception.UnauthorizedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PageTemplateService {
  private final EntityClient entityClient;
  private static final String DEFAULT_HOME_PAGE_TEMPLATE_URN =
      "urn:li:dataHubPageTemplate:home_default_1";

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

    checkModulesExistInRows(opContext, rows);

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

  private void checkModulesExistInRows(
      @Nonnull OperationContext opContext, @Nonnull final List<DataHubPageTemplateRow> rows) {
    rows.forEach(
        row -> {
          row.getModules()
              .forEach(
                  module -> {
                    try {
                      if (!entityClient.exists(opContext, module)) {
                        throw new RuntimeException(
                            String.format("Module with urn %s does not exist", module));
                      }
                    } catch (Exception e) {
                      throw new RuntimeException("Failed validating template rows", e);
                    }
                  });
        });
  }

  /**
   * Deletes a DataHub page template.
   *
   * @param opContext the operation context
   * @param templateUrn the URN of the page template to delete
   */
  public void deletePageTemplate(
      @Nonnull OperationContext opContext, @Nonnull final Urn templateUrn) {
    Objects.requireNonNull(templateUrn, "templateUrn must not be null");

    try {
      checkDeleteTemplatePermissions(opContext, templateUrn);

      entityClient.deleteEntity(opContext, templateUrn);

      // Asynchronously delete all references to the entity (to return quickly)
      CompletableFuture.runAsync(
          () -> {
            try {
              entityClient.deleteEntityReferences(opContext, templateUrn);
            } catch (Exception e) {
              log.error(
                  String.format(
                      "Caught exception while attempting to clear all entity references for PageTemplate with urn %s",
                      templateUrn),
                  e);
            }
          });

    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to delete PageTemplate with urn %s", templateUrn), e);
    }
  }

  /**
   * Ensures that a page template exists, and uses the page template properties to determine if the
   * user can delete this template. PERSONAL templates can only be deleted by the user that created
   * them. GLOBAL templates can only be deleted by those with the manage privilege.
   */
  public void checkDeleteTemplatePermissions(
      @Nonnull OperationContext opContext, @Nonnull final Urn templateUrn) {

    if (Objects.equals(templateUrn.toString(), DEFAULT_HOME_PAGE_TEMPLATE_URN)) {
      throw new UnauthorizedException("Attempted to delete the default page template");
    }

    DataHubPageTemplateProperties properties = getPageTemplateProperties(opContext, templateUrn);

    if (properties == null) {
      throw new IllegalArgumentException(
          String.format(
              "Attempted to delete a page template that does not exist with urn %s", templateUrn));
    }

    if (properties.getVisibility().getScope().equals(PageTemplateScope.GLOBAL)
        && !AuthUtil.isAuthorized(opContext, PoliciesConfig.MANAGE_HOME_PAGE_TEMPLATES_PRIVILEGE)) {
      throw new UnauthorizedException("User is unauthorized to delete global templates.");
    }

    if (properties.getVisibility().getScope().equals(PageTemplateScope.PERSONAL)
        && !properties
            .getCreated()
            .getActor()
            .toString()
            .equals(opContext.getSessionAuthentication().getActor().toUrnStr())) {
      throw new UnauthorizedException(
          "Attempted to delete personal a page template that was not created by the actor");
    }
  }
}
