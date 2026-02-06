package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.SubTypes;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.key.CorpUserKey;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Service for managing Service Accounts in DataHub.
 *
 * <p>Service accounts are special CorpUser entities with:
 *
 * <ul>
 *   <li>A username prefix of "service:"
 *   <li>A SubTypes aspect with typeNames containing "SERVICE_ACCOUNT"
 * </ul>
 *
 * <p>Note that no Authorization is performed within the service. The expectation is that the caller
 * has already verified the permissions of the active Actor.
 */
@Slf4j
public class ServiceAccountService extends BaseService {

  public static final String SERVICE_ACCOUNT_SUB_TYPE = "SERVICE_ACCOUNT";
  public static final String SERVICE_ACCOUNT_PREFIX = "service:";

  public ServiceAccountService(
      @Nonnull final SystemEntityClient entityClient,
      @Nonnull final OpenApiClient openApiClient,
      @Nonnull final ObjectMapper objectMapper) {
    super(entityClient, openApiClient, objectMapper);
  }

  /**
   * Creates a new service account.
   *
   * @param opContext the operation context
   * @param name the name of the service account (without the "service:" prefix)
   * @param displayName optional display name
   * @param description optional description
   * @param createdBy the URN of the user creating the service account
   * @return the URN of the created service account
   * @throws IllegalArgumentException if a service account with this name already exists
   * @throws Exception if creation fails
   */
  @Nonnull
  public Urn createServiceAccount(
      @Nonnull OperationContext opContext,
      @Nonnull String name,
      @Nullable String displayName,
      @Nullable String description,
      @Nonnull String createdBy)
      throws Exception {
    Objects.requireNonNull(opContext, "opContext must not be null");
    Objects.requireNonNull(name, "name must not be null");
    Objects.requireNonNull(createdBy, "createdBy must not be null");

    // Build identifiers
    final String serviceAccountId = SERVICE_ACCOUNT_PREFIX + name;
    final Urn serviceAccountUrn =
        UrnUtils.getUrn(String.format("urn:li:corpuser:%s", serviceAccountId));

    // Check if service account already exists
    if (this.entityClient.exists(opContext, serviceAccountUrn)) {
      throw new IllegalArgumentException("Service account with name '" + name + "' already exists");
    }

    log.info("Creating service account {} for user {}", serviceAccountUrn.toString(), createdBy);

    // Build all proposals - ingest synchronously one by one for atomicity
    final List<MetadataChangeProposal> proposals = new ArrayList<>();

    // Create the CorpUserKey aspect
    final CorpUserKey key = new CorpUserKey();
    key.setUsername(serviceAccountId);
    proposals.add(
        AspectUtils.buildMetadataChangeProposal(serviceAccountUrn, CORP_USER_KEY_ASPECT_NAME, key));

    // Create the CorpUserInfo aspect
    final CorpUserInfo info = new CorpUserInfo();
    info.setActive(true);
    info.setDisplayName(displayName != null ? displayName : name);
    if (description != null) {
      info.setTitle(description);
    }
    proposals.add(
        AspectUtils.buildMetadataChangeProposal(
            serviceAccountUrn, CORP_USER_INFO_ASPECT_NAME, info));

    // Create the SubTypes aspect - this is critical for identifying service accounts
    final SubTypes subTypes = new SubTypes();
    subTypes.setTypeNames(new StringArray(SERVICE_ACCOUNT_SUB_TYPE));
    proposals.add(
        AspectUtils.buildMetadataChangeProposal(
            serviceAccountUrn, SUB_TYPES_ASPECT_NAME, subTypes));

    log.info(
        "Ingesting {} proposals for service account {} (SubTypes typeNames: {})",
        proposals.size(),
        serviceAccountUrn.toString(),
        subTypes.getTypeNames());

    // Ingest all proposals synchronously
    ingestChangeProposals(opContext, proposals, false);

    log.info(
        "Successfully created service account {} for user {}",
        serviceAccountUrn.toString(),
        createdBy);

    return serviceAccountUrn;
  }

  /**
   * Gets a service account by URN.
   *
   * @param opContext the operation context
   * @param urn the URN of the service account
   * @return the EntityResponse containing the service account data, or null if not found
   * @throws IllegalArgumentException if the URN is not a service account
   * @throws Exception if retrieval fails
   */
  @Nullable
  public EntityResponse getServiceAccount(@Nonnull OperationContext opContext, @Nonnull Urn urn)
      throws Exception {
    Objects.requireNonNull(opContext, "opContext must not be null");
    Objects.requireNonNull(urn, "urn must not be null");

    EntityResponse response =
        this.entityClient.getV2(
            opContext, CORP_USER_ENTITY_NAME, urn, Collections.singleton(SUB_TYPES_ASPECT_NAME));

    if (response == null) {
      return null;
    }

    // Verify this is a service account
    if (!isServiceAccount(response)) {
      throw new IllegalArgumentException(
          "The specified URN is not a service account: " + urn.toString());
    }

    // Now fetch all aspects
    return this.entityClient.getV2(opContext, CORP_USER_ENTITY_NAME, urn, null);
  }

  /**
   * Deletes a service account.
   *
   * @param opContext the operation context
   * @param urn the URN of the service account to delete
   * @throws IllegalArgumentException if the URN is not a service account or doesn't exist
   * @throws Exception if deletion fails
   */
  public void deleteServiceAccount(@Nonnull OperationContext opContext, @Nonnull Urn urn)
      throws Exception {
    Objects.requireNonNull(opContext, "opContext must not be null");
    Objects.requireNonNull(urn, "urn must not be null");

    // Fetch the entity to verify it's a service account
    EntityResponse response =
        this.entityClient.getV2(
            opContext, CORP_USER_ENTITY_NAME, urn, Collections.singleton(SUB_TYPES_ASPECT_NAME));

    if (response == null) {
      throw new IllegalArgumentException("Service account not found: " + urn.toString());
    }

    log.info("Fetched entity {} with aspects: {}", urn.toString(), response.getAspects().keySet());

    // Verify this is a service account by checking SubTypes aspect
    if (!isServiceAccount(response)) {
      log.error(
          "Entity {} is not a service account. Available aspects: {}",
          urn.toString(),
          response.getAspects().keySet());
      throw new IllegalArgumentException(
          "The specified URN is not a service account: " + urn.toString());
    }

    log.info("Deleting service account {}", urn.toString());

    // Delete the entity
    this.entityClient.deleteEntity(opContext, urn);
  }

  /**
   * Checks if an entity response represents a service account.
   *
   * @param response the entity response
   * @return true if the entity is a service account, false otherwise
   */
  public static boolean isServiceAccount(@Nullable EntityResponse response) {
    if (response == null) {
      return false;
    }

    if (!response.getAspects().containsKey(SUB_TYPES_ASPECT_NAME)) {
      return false;
    }

    try {
      final SubTypes subTypes =
          new SubTypes(response.getAspects().get(SUB_TYPES_ASPECT_NAME).getValue().data());
      return subTypes.hasTypeNames() && subTypes.getTypeNames().contains(SERVICE_ACCOUNT_SUB_TYPE);
    } catch (Exception e) {
      log.warn("Failed to parse subTypes for entity: {}", response.getUrn().toString(), e);
      return false;
    }
  }
}
