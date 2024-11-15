package com.linkedin.metadata.service;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.dataquality.DimensionTypeInfo;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.key.DimensionTypeKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * This class is used to permit easy CRUD operations on a DataHub Dimension Name. Currently it
 * supports creating, updating, and removing a Dimension Name.
 */
@Slf4j
public class DimensionTypeService extends BaseService {

  public DimensionTypeService(@Nonnull SystemEntityClient entityClient) {
    super(entityClient);
  }

  /**
   * Creates a new Dimension Name.
   *
   * <p>Note that this method does not do authorization validation. It is assumed that users of this
   * class have already authorized the operation.
   *
   * @param opContext operational context
   * @param name optional name of the Dimension
   * @param description optional description of the Dimension
   * @param currentTimeMs the current time in millis
   * @return the urn of the newly created Ownership Type
   */
  @Nullable
  public Urn createDimensionName(
      @Nonnull final OperationContext opContext,
      String name,
      @Nullable String description,
      long currentTimeMs) {
    Objects.requireNonNull(name, "name must not be null");
    Objects.requireNonNull(
        opContext.getSessionAuthentication(), "operation context must not be null");

    // 1. Generate a unique id for the new Ownership Type.
    final DimensionTypeKey key = new DimensionTypeKey();
    key.setId(UUID.randomUUID().toString());

    // 2. Create a new instance of Ownership TypeProperties
    final DimensionTypeInfo dimensionTypeInfo = new DimensionTypeInfo();
    dimensionTypeInfo.setName(name);
    dimensionTypeInfo.setDescription(description, SetMode.IGNORE_NULL);
    final AuditStamp auditStamp =
        new AuditStamp()
            .setActor(UrnUtils.getUrn(opContext.getSessionAuthentication().getActor().toUrnStr()))
            .setTime(currentTimeMs);
    dimensionTypeInfo.setCreated(auditStamp);
    dimensionTypeInfo.setLastModified(auditStamp);

    // 3. Write the new Ownership Type to GMS, return the new URN.
    try {
      final Urn entityUrn =
          EntityKeyUtils.convertEntityKeyToUrn(key, Constants.DIMENSION_TYPE_ENTITY_NAME);
      return UrnUtils.getUrn(
          this.entityClient.ingestProposal(
              opContext,
              AspectUtils.buildMetadataChangeProposal(
                  entityUrn, Constants.DIMENSION_NAME_INFO_ASPECT_NAME, dimensionTypeInfo),
              false));
    } catch (Exception e) {
      throw new RuntimeException("Failed to create Dimension Type", e);
    }
  }

  /**
   * Updates an existing DimensionName. If a provided field is null, the previous value will be
   * kept.
   *
   * <p>Note that this method does not do authorization validation. It is assumed that users of this
   * class have already authorized the operation.
   *
   * @param urn the urn of the dimension name
   * @param name optional name of the dimension name
   * @param description optional description of the dimension name
   * @param authentication the current authentication
   * @param currentTimeMs the current time in millis
   */
  public void updateDimensionName(
      @Nonnull final OperationContext opContext,
      @Nonnull Urn urn,
      @Nullable String name,
      @Nullable String description,
      long currentTimeMs) {
    Objects.requireNonNull(urn, "urn must not be null");
    Objects.requireNonNull(
        opContext.getSessionAuthentication(), "operational context must not be null");

    DimensionTypeInfo info = getDimensionTypeInfo(opContext, urn);

    if (info == null) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to update Dimension Type. Dimension Type with urn %s does not exist.", urn));
    }

    // 2. Apply changes to existing Dimension Type
    if (name != null) {
      info.setName(name);
    }
    if (description != null) {
      info.setDescription(description);
    }

    info.setLastModified(
        new AuditStamp()
            .setTime(currentTimeMs)
            .setActor(UrnUtils.getUrn(opContext.getSessionAuthentication().getActor().toUrnStr())));

    // 3. Write changes to GMS
    try {
      this.entityClient.ingestProposal(
          opContext,
          AspectUtils.buildMetadataChangeProposal(
              urn, Constants.DIMENSION_NAME_INFO_ASPECT_NAME, info),
          false);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to update View with urn %s", urn), e);
    }
  }

  /**
   * Deletes an existing Dimension Type with a specific urn.
   *
   * <p>Note that this method does not do authorization validation. It is assumed that users of this
   * class have already authorized the operation
   *
   * <p>If the Dimension Type does not exist, no exception will be thrown.
   *
   * @param opContext operational context
   * @param urn the urn of the Dimension Type
   */
  public void deleteDimensionType(
      @Nonnull OperationContext opContext, @Nonnull Urn urn, boolean deleteReferences) {
    Objects.requireNonNull(urn, "Dimension TypeUrn must not be null");
    Objects.requireNonNull(
        opContext.getSessionAuthentication(), "operational context must not be null");
    try {
      this.entityClient.deleteEntity(opContext, urn);
      if (deleteReferences) {
        this.entityClient.deleteEntityReferences(opContext, urn);
      }
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to delete Dimension Type with urn %s", urn), e);
    }
  }

  /**
   * Returns an instance of {@link OwnershipTypeInfo} for the specified Ownership Type urn, or null
   * if one cannot be found.
   *
   * @param opContext operational context
   * @param ownershipTypeUrn the urn of the Ownership Type
   * @return an instance of {@link OwnershipTypeInfo} for the Ownership Type, null if it does not
   *     exist.
   */
  @Nullable
  public DimensionTypeInfo getDimensionTypeInfo(
      @Nonnull OperationContext opContext, @Nonnull final Urn dimensionTypeUrn) {
    Objects.requireNonNull(dimensionTypeUrn, "dimensionTypeUrn must not be null");
    Objects.requireNonNull(
        opContext.getSessionAuthentication(), "operational context must not be null");
    final EntityResponse response = getDimensionTypeEntityResponse(opContext, dimensionTypeUrn);
    if (response != null
        && response.getAspects().containsKey(Constants.DIMENSION_NAME_INFO_ASPECT_NAME)) {
      return new DimensionTypeInfo(
          response.getAspects().get(Constants.DIMENSION_NAME_INFO_ASPECT_NAME).getValue().data());
    }
    // No aspect found
    return null;
  }

  /**
   * Returns an instance of {@link EntityResponse} for the specified Dimension Type urn, or null if
   * one cannot be found.
   *
   * @param opContext operational context
   * @param dimensionTypeUrn the urn of the Dimension Type.
   * @return an instance of {@link EntityResponse} for the Dimension Type, null if it does not
   *     exist.
   */
  @Nullable
  public EntityResponse getDimensionTypeEntityResponse(
      @Nonnull OperationContext opContext, @Nonnull final Urn dimensionTypeUrn) {
    Objects.requireNonNull(dimensionTypeUrn, "dimensionTypeUrn must not be null");
    Objects.requireNonNull(
        opContext.getSessionAuthentication(), "Operational context must not be null");
    try {
      return this.entityClient.getV2(
          opContext,
          Constants.DIMENSION_TYPE_ENTITY_NAME,
          dimensionTypeUrn,
          ImmutableSet.of(Constants.DIMENSION_NAME_INFO_ASPECT_NAME, Constants.STATUS_ASPECT_NAME));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to retrieve Ownership Type with urn %s", dimensionTypeUrn), e);
    }
  }
}
