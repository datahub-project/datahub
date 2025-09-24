package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Origin;
import com.linkedin.common.OriginType;
import com.linkedin.common.SubTypes;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Service for managing DataHub service accounts, particularly those created from OAuth/OIDC tokens.
 * Handles the creation and management of service account users with proper aspects and metadata.
 *
 * <p>Service accounts created by this service are marked with:
 *
 * <ul>
 *   <li>SubTypes aspect with "SERVICE" type
 *   <li>Origin aspect with external type information
 *   <li>CorpUserInfo aspect with basic user information
 * </ul>
 */
@Slf4j
public class ServiceAccountService {

  static final String USER_ID_PREFIX = "__oauth_";
  static final String DEFAULT_USER_CLAIM = "sub";

  /**
   * Creates a unique service account user ID from issuer and subject information. The structure
   * ensures uniqueness across IdPs in case migrations happen.
   *
   * @param issuer The issuer URL from the JWT token
   * @param subject The subject (user identifier) from the JWT token
   * @return Unique service account user ID
   */
  public String buildServiceUserUrn(@Nonnull String issuer, @Nonnull String subject) {
    String sanitizedIssuer = issuer.replaceAll("https?://", "").replaceAll("[^a-zA-Z0-9]", "_");
    return String.format("%s%s_%s", USER_ID_PREFIX, sanitizedIssuer, subject);
  }

  /**
   * Creates a service account with the specified name and origin information.
   *
   * @param userId The unique service account user ID
   * @param displayName The display name for the service account
   * @param originType The origin type (e.g., EXTERNAL, NATIVE)
   * @param externalType Additional origin information (e.g., issuer URL)
   * @param entityService The entity service for persistence
   * @param operationContext The operation context for the request
   * @return true if the service account was created successfully, false if it already exists
   */
  public boolean createServiceAccount(
      @Nonnull String userId,
      @Nonnull String displayName,
      @Nonnull OriginType originType,
      @Nonnull String externalType,
      @Nonnull EntityService<?> entityService,
      @Nonnull OperationContext operationContext) {

    try {
      CorpuserUrn userUrn = new CorpuserUrn(userId);

      // Check if user already exists
      boolean userExists = entityService.exists(operationContext, userUrn, false);
      if (userExists) {
        log.debug("Service account user already exists: {}", userUrn);
        return false;
      }

      log.info("Creating new service account user: {}", userUrn);

      // Create the aspects for the new service account
      List<MetadataChangeProposal> aspectsToIngest =
          createServiceAccountAspects(userUrn, displayName, originType, externalType);

      // Ingest synchronously to ensure user is immediately available
      AspectsBatch aspectsBatch =
          AspectsBatchImpl.builder()
              .mcps(
                  aspectsToIngest, createSystemAuditStamp(), operationContext.getRetrieverContext())
              .build(operationContext);

      entityService.ingestAspects(operationContext, aspectsBatch, false, true);

      log.info("Successfully created service account user: {}", userId);
      return true;

    } catch (Exception e) {
      log.error("Failed to create service account user: {}. Error: {}", userId, e.getMessage());
      throw new RuntimeException("Failed to create service account: " + e.getMessage(), e);
    }
  }

  /**
   * Creates a service account from OAuth/OIDC token information. Ensures that a service account
   * user exists in DataHub. If the user doesn't exist, creates a new user with CorpUserInfo,
   * SubTypes, and Origin aspects.
   *
   * @param userId The unique service account user ID
   * @param issuer The issuer URL from the JWT token
   * @param subject The subject (user identifier) from the JWT token
   * @param entityService The entity service for persistence
   * @param operationContext The operation context for the request
   * @return true if the service account was created, false if it already exists
   */
  public boolean ensureServiceAccountExists(
      @Nonnull String userId,
      @Nonnull String issuer,
      @Nonnull String subject,
      @Nonnull EntityService<?> entityService,
      @Nonnull OperationContext operationContext) {

    try {
      CorpuserUrn userUrn = new CorpuserUrn(userId);

      // Check if user already exists
      boolean userExists = entityService.exists(operationContext, userUrn, false);
      if (userExists) {
        log.debug("Service account user already exists: {}", userUrn);
        return false;
      }

      log.info("Creating new service account user: {}", userUrn);

      String displayName = String.format("Service Account: %s @ %s", subject, issuer);

      // Create the aspects for the new service account
      List<MetadataChangeProposal> aspectsToIngest =
          createServiceAccountAspects(userUrn, displayName, OriginType.EXTERNAL, issuer);

      // Ingest synchronously to ensure user is immediately available
      AspectsBatch aspectsBatch =
          AspectsBatchImpl.builder()
              .mcps(
                  aspectsToIngest, createSystemAuditStamp(), operationContext.getRetrieverContext())
              .build(operationContext);

      entityService.ingestAspects(operationContext, aspectsBatch, false, true);

      log.info("Successfully created service account user: {} from issuer: {}", userId, issuer);
      return true;

    } catch (Exception e) {
      // Don't fail authentication if user creation fails - treat as side-effect
      log.error(
          "Failed to create service account user: {} from issuer: {}. Error: {}",
          userId,
          issuer,
          e.getMessage());
      return false;
    }
  }

  /**
   * Creates the required aspects for a new service account user.
   *
   * @param userUrn The URN of the user to create
   * @param displayName The display name for the service account
   * @param originType The origin type (e.g., EXTERNAL, NATIVE)
   * @param externalType Additional origin information (e.g., issuer URL)
   * @return List of MetadataChangeProposal objects representing the aspects to ingest
   */
  public List<MetadataChangeProposal> createServiceAccountAspects(
      @Nonnull CorpuserUrn userUrn,
      @Nonnull String displayName,
      @Nonnull OriginType originType,
      @Nonnull String externalType) {

    List<MetadataChangeProposal> aspects = new ArrayList<>();

    // 1. CorpUserInfo aspect - basic user information
    CorpUserInfo corpUserInfo = new CorpUserInfo();
    corpUserInfo.setActive(true);
    corpUserInfo.setDisplayName(displayName);
    corpUserInfo.setTitle("OAuth Service Account");

    aspects.add(createMetadataChangeProposal(userUrn, CORP_USER_INFO_ASPECT_NAME, corpUserInfo));

    // 2. SubTypes aspect - mark as SERVICE
    SubTypes subTypes = new SubTypes();
    StringArray typeNames = new StringArray();
    typeNames.add("SERVICE");
    subTypes.setTypeNames(typeNames);

    aspects.add(createMetadataChangeProposal(userUrn, SUB_TYPES_ASPECT_NAME, subTypes));

    // 3. Origin aspect - mark with origin information
    Origin origin = new Origin();
    origin.setType(originType);
    origin.setExternalType(externalType);

    aspects.add(createMetadataChangeProposal(userUrn, ORIGIN_ASPECT_NAME, origin));

    return aspects;
  }

  /**
   * Helper method to create a MetadataChangeProposal for an aspect.
   *
   * @param userUrn The URN of the user
   * @param aspectName The name of the aspect
   * @param aspect The aspect data
   * @return MetadataChangeProposal for the aspect
   */
  public MetadataChangeProposal createMetadataChangeProposal(
      @Nonnull CorpuserUrn userUrn, @Nonnull String aspectName, @Nonnull RecordTemplate aspect) {

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(userUrn);
    mcp.setEntityType(userUrn.getEntityType());
    mcp.setAspectName(aspectName);
    mcp.setAspect(GenericRecordUtils.serializeAspect(aspect));
    mcp.setChangeType(ChangeType.UPSERT);

    return mcp;
  }

  /**
   * Creates an AuditStamp for system-level operations.
   *
   * @return AuditStamp with system context
   */
  public AuditStamp createSystemAuditStamp() {
    return new AuditStamp()
        .setTime(System.currentTimeMillis())
        .setActor(UrnUtils.getUrn(SYSTEM_ACTOR));
  }
}
