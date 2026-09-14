package com.linkedin.metadata.service;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.key.DataHubColumnViewKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.view.DataHubColumnViewDefinition;
import com.linkedin.view.DataHubColumnViewInfo;
import com.linkedin.view.DataHubColumnViewTarget;
import com.linkedin.view.DataHubViewType;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * CRUD operations on a DataHub Column View. Mirrors {@link ViewService}.
 *
 * <p>No Authorization is performed within the service. The caller is expected to have already
 * verified the permissions of the active Actor.
 */
@Slf4j
public class ColumnViewService extends BaseService {

  public ColumnViewService(@Nonnull SystemEntityClient entityClient) {
    super(entityClient);
  }

  /** Creates a new Column View and returns its urn. */
  public Urn createColumnView(
      @Nonnull OperationContext opContext,
      @Nonnull DataHubViewType type,
      @Nonnull DataHubColumnViewTarget target,
      @Nonnull String name,
      @Nullable String description,
      @Nonnull DataHubColumnViewDefinition definition,
      long currentTimeMs) {
    Objects.requireNonNull(type, "type must not be null");
    Objects.requireNonNull(target, "target must not be null");
    Objects.requireNonNull(name, "name must not be null");
    Objects.requireNonNull(definition, "definition must not be null");
    Objects.requireNonNull(opContext.getSessionAuthentication(), "authentication must not be null");

    final DataHubColumnViewKey key = new DataHubColumnViewKey();
    key.setId(UUID.randomUUID().toString());

    final DataHubColumnViewInfo newView = new DataHubColumnViewInfo();
    newView.setType(type);
    newView.setTarget(target);
    newView.setName(name);
    newView.setDescription(description, SetMode.IGNORE_NULL);
    newView.setDefinition(definition);
    final AuditStamp auditStamp =
        new AuditStamp()
            .setActor(UrnUtils.getUrn(opContext.getSessionAuthentication().getActor().toUrnStr()))
            .setTime(currentTimeMs);
    newView.setCreated(auditStamp);
    newView.setLastModified(auditStamp);

    try {
      return UrnUtils.getUrn(
          this.entityClient.ingestProposal(
              opContext,
              AspectUtils.buildMetadataChangeProposal(
                  EntityKeyUtils.convertEntityKeyToUrn(
                      key, Constants.DATAHUB_COLUMN_VIEW_ENTITY_NAME),
                  Constants.DATAHUB_COLUMN_VIEW_INFO_ASPECT_NAME,
                  newView),
              false));
    } catch (Exception e) {
      throw new RuntimeException("Failed to create Column View", e);
    }
  }

  /**
   * Updates an existing Column View, overwriting only the non-null fields. Read-modify-write, same
   * caveats as {@link ViewService#updateView}. The target is immutable after creation.
   */
  public void updateColumnView(
      @Nonnull OperationContext opContext,
      @Nonnull Urn viewUrn,
      @Nullable String name,
      @Nullable String description,
      @Nullable DataHubColumnViewDefinition definition,
      long currentTimeMs) {
    Objects.requireNonNull(viewUrn, "viewUrn must not be null");
    Objects.requireNonNull(opContext.getSessionAuthentication(), "authentication must not be null");

    DataHubColumnViewInfo existingInfo = getColumnViewInfo(opContext, viewUrn);
    if (existingInfo == null) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to update Column View. Column View with urn %s does not exist.", viewUrn));
    }

    if (name != null) {
      existingInfo.setName(name);
    }
    if (description != null) {
      existingInfo.setDescription(description);
    }
    if (definition != null) {
      existingInfo.setDefinition(definition);
    }
    existingInfo.setLastModified(
        new AuditStamp()
            .setTime(currentTimeMs)
            .setActor(UrnUtils.getUrn(opContext.getSessionAuthentication().getActor().toUrnStr())));

    try {
      this.entityClient.ingestProposal(
          opContext,
          AspectUtils.buildMetadataChangeProposal(
              viewUrn, Constants.DATAHUB_COLUMN_VIEW_INFO_ASPECT_NAME, existingInfo),
          false);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to update Column View with urn %s", viewUrn), e);
    }
  }

  /** Hard-deletes a Column View and, asynchronously, references to it (settings defaults). */
  public void deleteColumnView(@Nonnull OperationContext opContext, @Nonnull Urn viewUrn) {
    try {
      this.entityClient.deleteEntity(
          opContext, Objects.requireNonNull(viewUrn, "viewUrn must not be null"));
      CompletableFuture.runAsync(
          () -> {
            try {
              this.entityClient.deleteEntityReferences(opContext, viewUrn);
            } catch (RemoteInvocationException e) {
              log.error(
                  String.format(
                      "Caught exception while attempting to clear all entity references for column view with urn %s",
                      viewUrn),
                  e);
            }
          });
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to delete Column View with urn %s", viewUrn), e);
    }
  }

  @Nullable
  public DataHubColumnViewInfo getColumnViewInfo(
      @Nonnull OperationContext opContext, @Nonnull final Urn viewUrn) {
    Objects.requireNonNull(viewUrn, "viewUrn must not be null");
    Objects.requireNonNull(opContext.getSessionAuthentication(), "authentication must not be null");
    final EntityResponse response = getColumnViewEntityResponse(opContext, viewUrn);
    if (response != null
        && response.getAspects().containsKey(Constants.DATAHUB_COLUMN_VIEW_INFO_ASPECT_NAME)) {
      return new DataHubColumnViewInfo(
          response
              .getAspects()
              .get(Constants.DATAHUB_COLUMN_VIEW_INFO_ASPECT_NAME)
              .getValue()
              .data());
    }
    return null;
  }

  @Nullable
  public EntityResponse getColumnViewEntityResponse(
      @Nonnull OperationContext opContext, @Nonnull final Urn viewUrn) {
    Objects.requireNonNull(viewUrn, "viewUrn must not be null");
    Objects.requireNonNull(opContext.getSessionAuthentication(), "authentication must not be null");
    try {
      return this.entityClient.getV2(
          opContext,
          Constants.DATAHUB_COLUMN_VIEW_ENTITY_NAME,
          viewUrn,
          ImmutableSet.of(Constants.DATAHUB_COLUMN_VIEW_INFO_ASPECT_NAME));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to retrieve Column View with urn %s", viewUrn), e);
    }
  }
}
