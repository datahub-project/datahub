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
import com.linkedin.metadata.key.DataHubPageModuleKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.module.DataHubPageModuleParams;
import com.linkedin.module.DataHubPageModuleProperties;
import com.linkedin.module.DataHubPageModuleType;
import com.linkedin.module.DataHubPageModuleVisibility;
import com.linkedin.module.PageModuleScope;
import com.linkedin.mxe.MetadataChangeProposal;
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
public class PageModuleService {
  private final EntityClient entityClient;

  // Default module URNs that cannot be deleted
  private static final List<String> DEFAULT_MODULE_URNS =
      List.of(
          "urn:li:dataHubPageModule:your_assets",
          "urn:li:dataHubPageModule:your_subscriptions",
          "urn:li:dataHubPageModule:top_domains");

  public PageModuleService(@Nonnull EntityClient entityClient) {
    this.entityClient = entityClient;
  }

  /**
   * Upserts a DataHub page module. If the page module with the provided urn already exists, then it
   * will be overwritten.
   *
   * <p>This method assumes that authorization has already been verified at the calling layer.
   *
   * @return the URN of the new page module.
   */
  public Urn upsertPageModule(
      @Nonnull OperationContext opContext,
      @Nullable final String urn,
      @Nonnull final String name,
      @Nonnull final DataHubPageModuleType type,
      @Nonnull final PageModuleScope scope,
      @Nonnull final DataHubPageModuleParams params) {
    Objects.requireNonNull(name, "name must not be null");
    Objects.requireNonNull(type, "type must not be null");
    Objects.requireNonNull(scope, "scope must not be null");
    Objects.requireNonNull(params, "params must not be null");

    // 1. Optionally generate new page module urn
    Urn moduleUrn = null;
    if (urn != null) {
      moduleUrn = UrnUtils.getUrn(urn);
    } else {
      final String moduleId = UUID.randomUUID().toString();
      final DataHubPageModuleKey key = new DataHubPageModuleKey().setId(moduleId);
      moduleUrn =
          EntityKeyUtils.convertEntityKeyToUrn(key, Constants.DATAHUB_PAGE_MODULE_ENTITY_NAME);
    }
    final AuditStamp nowAuditStamp = opContext.getAuditStamp();

    // 2. Build Page Module Properties
    DataHubPageModuleProperties properties = new DataHubPageModuleProperties();
    DataHubPageModuleProperties existingProperties = getPageModuleProperties(opContext, moduleUrn);
    if (existingProperties != null) {
      properties = existingProperties;
    } else {
      // if creating a new page module, set the created stamp
      properties.setCreated(nowAuditStamp);
    }

    properties.setName(name);
    properties.setType(type);

    DataHubPageModuleVisibility visibility = new DataHubPageModuleVisibility();
    visibility.setScope(scope);
    properties.setVisibility(visibility);

    properties.setParams(params);
    properties.setLastModified(nowAuditStamp);

    // 3. Write changes to GMS
    try {
      final List<MetadataChangeProposal> aspectsToIngest = new ArrayList<>();
      aspectsToIngest.add(
          AspectUtils.buildMetadataChangeProposal(
              moduleUrn, Constants.DATAHUB_PAGE_MODULE_PROPERTIES_ASPECT_NAME, properties));
      entityClient.batchIngestProposals(opContext, aspectsToIngest, false);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to upsert PageModule with urn %s", moduleUrn), e);
    }
    return moduleUrn;
  }

  @Nullable
  public DataHubPageModuleProperties getPageModuleProperties(
      @Nonnull OperationContext opContext, @Nonnull final Urn moduleUrn) {
    Objects.requireNonNull(moduleUrn, "moduleUrn must not be null");
    final EntityResponse response = getPageModuleEntityResponse(opContext, moduleUrn);
    if (response != null
        && response
            .getAspects()
            .containsKey(Constants.DATAHUB_PAGE_MODULE_PROPERTIES_ASPECT_NAME)) {
      return new DataHubPageModuleProperties(
          response
              .getAspects()
              .get(Constants.DATAHUB_PAGE_MODULE_PROPERTIES_ASPECT_NAME)
              .getValue()
              .data());
    }
    // No aspect found
    return null;
  }

  @Nullable
  public EntityResponse getPageModuleEntityResponse(
      @Nonnull OperationContext opContext, @Nonnull final Urn moduleUrn) {
    try {
      return entityClient.getV2(
          opContext, Constants.DATAHUB_PAGE_MODULE_ENTITY_NAME, moduleUrn, null, false);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to retrieve PageModule with urn %s", moduleUrn), e);
    }
  }

  /**
   * Deletes a DataHub page module.
   *
   * @param opContext the operation context
   * @param moduleUrn the URN of the page module to delete
   */
  public void deletePageModule(@Nonnull OperationContext opContext, @Nonnull final Urn moduleUrn) {
    Objects.requireNonNull(moduleUrn, "moduleUrn must not be null");

    try {
      checkDeleteModulePermissions(opContext, moduleUrn);

      entityClient.deleteEntity(opContext, moduleUrn);

      // Asynchronously delete all references to the entity (to return quickly)
      CompletableFuture.runAsync(
          () -> {
            try {
              entityClient.deleteEntityReferences(opContext, moduleUrn);
            } catch (Exception e) {
              log.error(
                  String.format(
                      "Caught exception while attempting to clear all entity references for PageModule with urn %s",
                      moduleUrn),
                  e);
            }
          });

    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to delete PageModule with urn %s", moduleUrn), e);
    }
  }

  /**
   * Ensures that a page module exists, and uses the page module properties to determine if the user
   * can delete this module. PERSONAL modules can only be deleted by the user that created them.
   * GLOBAL modules can only be deleted by those with the manage privilege. Default modules cannot
   * be deleted.
   */
  public void checkDeleteModulePermissions(
      @Nonnull OperationContext opContext, @Nonnull final Urn moduleUrn) {
    // Check if this is a default module that cannot be deleted
    if (DEFAULT_MODULE_URNS.contains(moduleUrn.toString())) {
      throw new IllegalArgumentException(
          String.format("Cannot delete default page module with urn %s", moduleUrn));
    }

    DataHubPageModuleProperties properties = getPageModuleProperties(opContext, moduleUrn);

    if (properties == null) {
      throw new IllegalArgumentException(
          String.format(
              "Attempted to delete a page module that does not exist with urn %s", moduleUrn));
    }

    if (properties.getVisibility().getScope().equals(PageModuleScope.GLOBAL)
        && AuthUtil.isAuthorized(opContext, PoliciesConfig.MANAGE_HOME_PAGE_TEMPLATES_PRIVILEGE)) {
      throw new UnauthorizedException("User is unauthorized to delete global modules.");
    }

    if (properties.getVisibility().getScope().equals(PageModuleScope.PERSONAL)
        && !properties.getCreated().getActor().equals(opContext.getActorContext().getActorUrn())) {
      throw new UnauthorizedException(
          "Attempted to delete personal a page module that was not created by the actor");
    }
    // TODO: check permissions for deleting a GLOBAL module in CH-510
  }
}
