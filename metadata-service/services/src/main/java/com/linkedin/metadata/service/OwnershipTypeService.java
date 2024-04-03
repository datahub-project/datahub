package com.linkedin.metadata.service;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.key.OwnershipTypeKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.ownership.OwnershipTypeInfo;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * This class is used to permit easy CRUD operations on a DataHub Ownership Type. Currently it
 * supports creating, updating, and removing a Ownership Type.
 *
 * <p>Note that no Authorization is performed within the service. The expectation is that the caller
 * has already verified the permissions of the active Actor.
 *
 * <p>TODO: Ideally we have some basic caching of the view information inside of this class.
 */
@Slf4j
public class OwnershipTypeService extends BaseService {

  public static final String SYSTEM_ID = "__system__";

  public OwnershipTypeService(
      @Nonnull EntityClient entityClient, @Nonnull Authentication systemAuthentication) {
    super(entityClient, systemAuthentication);
  }

  /**
   * Creates a new Ownership Type.
   *
   * <p>Note that this method does not do authorization validation. It is assumed that users of this
   * class have already authorized the operation.
   *
   * @param name optional name of the Ownership Type
   * @param description optional description of the Ownership Type
   * @param authentication the current authentication
   * @param currentTimeMs the current time in millis
   * @return the urn of the newly created Ownership Type
   */
  public Urn createOwnershipType(
      String name,
      @Nullable String description,
      @Nonnull Authentication authentication,
      long currentTimeMs) {
    Objects.requireNonNull(name, "name must not be null");
    Objects.requireNonNull(authentication, "authentication must not be null");

    // 1. Generate a unique id for the new Ownership Type.
    final OwnershipTypeKey key = new OwnershipTypeKey();
    key.setId(UUID.randomUUID().toString());

    // 2. Create a new instance of Ownership TypeProperties
    final OwnershipTypeInfo ownershipTypeInfo = new OwnershipTypeInfo();
    ownershipTypeInfo.setName(name);
    ownershipTypeInfo.setDescription(description, SetMode.IGNORE_NULL);
    final AuditStamp auditStamp =
        new AuditStamp()
            .setActor(UrnUtils.getUrn(authentication.getActor().toUrnStr()))
            .setTime(currentTimeMs);
    ownershipTypeInfo.setCreated(auditStamp);
    ownershipTypeInfo.setLastModified(auditStamp);

    // 3. Write the new Ownership Type to GMS, return the new URN.
    try {
      final Urn entityUrn =
          EntityKeyUtils.convertEntityKeyToUrn(key, Constants.OWNERSHIP_TYPE_ENTITY_NAME);
      return UrnUtils.getUrn(
          this.entityClient.ingestProposal(
              AspectUtils.buildMetadataChangeProposal(
                  entityUrn, Constants.OWNERSHIP_TYPE_INFO_ASPECT_NAME, ownershipTypeInfo),
              authentication,
              false));
    } catch (Exception e) {
      throw new RuntimeException("Failed to create Ownership Type", e);
    }
  }

  /**
   * Updates an existing Ownership Type. If a provided field is null, the previous value will be
   * kept.
   *
   * <p>Note that this method does not do authorization validation. It is assumed that users of this
   * class have already authorized the operation.
   *
   * @param urn the urn of the Ownership Type
   * @param name optional name of the Ownership Type
   * @param description optional description of the Ownership Type
   * @param authentication the current authentication
   * @param currentTimeMs the current time in millis
   */
  public void updateOwnershipType(
      @Nonnull Urn urn,
      @Nullable String name,
      @Nullable String description,
      @Nonnull Authentication authentication,
      long currentTimeMs) {
    Objects.requireNonNull(urn, "urn must not be null");
    Objects.requireNonNull(authentication, "authentication must not be null");

    // 1. Check whether the Ownership Type exists
    OwnershipTypeInfo info = getOwnershipTypeInfo(urn, authentication);

    if (info == null) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to update Ownership Type. Ownership Type with urn %s does not exist.", urn));
    }

    // 2. Apply changes to existing Ownership Type
    if (name != null) {
      info.setName(name);
    }
    if (description != null) {
      info.setDescription(description);
    }

    info.setLastModified(
        new AuditStamp()
            .setTime(currentTimeMs)
            .setActor(UrnUtils.getUrn(authentication.getActor().toUrnStr())));

    // 3. Write changes to GMS
    try {
      this.entityClient.ingestProposal(
          AspectUtils.buildMetadataChangeProposal(
              urn, Constants.OWNERSHIP_TYPE_INFO_ASPECT_NAME, info),
          authentication,
          false);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to update View with urn %s", urn), e);
    }
  }

  /**
   * Deletes an existing Ownership Type with a specific urn.
   *
   * <p>Note that this method does not do authorization validation. It is assumed that users of this
   * class have already authorized the operation
   *
   * <p>If the Ownership Type does not exist, no exception will be thrown.
   *
   * @param urn the urn of the Ownership Type
   * @param authentication the current authentication
   */
  public void deleteOwnershipType(
      @Nonnull Urn urn, boolean deleteReferences, @Nonnull Authentication authentication) {
    Objects.requireNonNull(urn, "Ownership TypeUrn must not be null");
    Objects.requireNonNull(authentication, "authentication must not be null");
    try {
      if (isSystemOwnershipType(urn)) {
        log.info("Soft deleting ownership type: {}", urn);
        final Status statusAspect = new Status();
        statusAspect.setRemoved(true);
        this.entityClient.ingestProposal(
            AspectUtils.buildMetadataChangeProposal(
                urn, Constants.STATUS_ASPECT_NAME, statusAspect),
            authentication,
            false);
      } else {
        this.entityClient.deleteEntity(urn, authentication);
        if (deleteReferences) {
          this.entityClient.deleteEntityReferences(urn, authentication);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to delete Ownership Type with urn %s", urn), e);
    }
  }

  /**
   * Return whether the provided urn is for a system provided ownership type.
   *
   * @param urn the urn of the Ownership Type
   * @return true is the ownership type is a system default.
   */
  private boolean isSystemOwnershipType(Urn urn) {
    return urn.getId().startsWith(SYSTEM_ID);
  }

  /**
   * Returns an instance of {@link OwnershipTypeInfo} for the specified Ownership Type urn, or null
   * if one cannot be found.
   *
   * @param ownershipTypeUrn the urn of the Ownership Type
   * @param authentication the authentication to use
   * @return an instance of {@link OwnershipTypeInfo} for the Ownership Type, null if it does not
   *     exist.
   */
  @Nullable
  public OwnershipTypeInfo getOwnershipTypeInfo(
      @Nonnull final Urn ownershipTypeUrn, @Nonnull final Authentication authentication) {
    Objects.requireNonNull(ownershipTypeUrn, "ownershipTypeUrn must not be null");
    Objects.requireNonNull(authentication, "authentication must not be null");
    final EntityResponse response =
        getOwnershipTypeEntityResponse(ownershipTypeUrn, authentication);
    if (response != null
        && response.getAspects().containsKey(Constants.OWNERSHIP_TYPE_INFO_ASPECT_NAME)) {
      return new OwnershipTypeInfo(
          response.getAspects().get(Constants.OWNERSHIP_TYPE_INFO_ASPECT_NAME).getValue().data());
    }
    // No aspect found
    return null;
  }

  /**
   * Returns an instance of {@link EntityResponse} for the specified Ownership Type urn, or null if
   * one cannot be found.
   *
   * @param ownershipTypeUrn the urn of the Ownership Type.
   * @param authentication the authentication to use
   * @return an instance of {@link EntityResponse} for the Ownership Type, null if it does not
   *     exist.
   */
  @Nullable
  public EntityResponse getOwnershipTypeEntityResponse(
      @Nonnull final Urn ownershipTypeUrn, @Nonnull final Authentication authentication) {
    Objects.requireNonNull(ownershipTypeUrn, "viewUrn must not be null");
    Objects.requireNonNull(authentication, "authentication must not be null");
    try {
      return this.entityClient.getV2(
          Constants.OWNERSHIP_TYPE_ENTITY_NAME,
          ownershipTypeUrn,
          ImmutableSet.of(Constants.OWNERSHIP_TYPE_INFO_ASPECT_NAME, Constants.STATUS_ASPECT_NAME),
          authentication);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to retrieve Ownership Type with urn %s", ownershipTypeUrn), e);
    }
  }
}
