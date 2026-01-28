package com.linkedin.datahub.graphql.resolvers.auth;

import com.linkedin.common.SubTypes;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ServiceAccount;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.Constants;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/** Utility class for service account operations. */
@Slf4j
public final class ServiceAccountUtils {

  /** The sub-type name used to identify service accounts in the SubTypes aspect. */
  public static final String SERVICE_ACCOUNT_SUB_TYPE = "SERVICE_ACCOUNT";

  /** The prefix used for service account usernames. */
  public static final String SERVICE_ACCOUNT_PREFIX = "service_";

  private ServiceAccountUtils() {
    // Utility class - prevent instantiation
  }

  /**
   * Checks if the entity response represents a service account by examining the SubTypes aspect.
   *
   * @param response The entity response to check
   * @return true if the entity is a service account, false otherwise
   */
  public static boolean isServiceAccount(@Nullable EntityResponse response) {
    if (response == null) {
      return false;
    }

    final EnvelopedAspect subTypesAspect =
        response.getAspects().get(Constants.SUB_TYPES_ASPECT_NAME);
    if (subTypesAspect == null) {
      return false;
    }

    try {
      final SubTypes subTypes = new SubTypes(subTypesAspect.getValue().data());
      return subTypes.hasTypeNames() && subTypes.getTypeNames().contains(SERVICE_ACCOUNT_SUB_TYPE);
    } catch (Exception e) {
      log.warn("Failed to parse subTypes for entity: {}", response.getUrn().toString(), e);
      return false;
    }
  }

  /**
   * Extracts the service account name from a corpuser URN.
   *
   * @param urn The corpuser URN string (e.g., "urn:li:corpuser:service_my-service")
   * @return The service account name without the prefix (e.g., "my-service")
   */
  @Nonnull
  public static String extractNameFromUrn(@Nonnull String urn) {
    final String username = urn.replace("urn:li:corpuser:", "");
    if (username.startsWith(SERVICE_ACCOUNT_PREFIX)) {
      return username.substring(SERVICE_ACCOUNT_PREFIX.length());
    }
    return username;
  }

  /**
   * Builds a service account URN from a name.
   *
   * @param name The service account name (without prefix)
   * @return The full corpuser URN string
   */
  @Nonnull
  public static String buildServiceAccountUrn(@Nonnull String name) {
    return "urn:li:corpuser:" + SERVICE_ACCOUNT_PREFIX + name;
  }

  /**
   * Builds a service account ID (username) from a name.
   *
   * @param name The service account name (without prefix)
   * @return The service account ID with prefix
   */
  @Nonnull
  public static String buildServiceAccountId(@Nonnull String name) {
    return SERVICE_ACCOUNT_PREFIX + name;
  }

  /**
   * Generates a new unique service account ID using UUID.
   *
   * @return A new service account ID in the format "service_UUID"
   */
  @Nonnull
  public static String generateServiceAccountId() {
    return SERVICE_ACCOUNT_PREFIX + UUID.randomUUID().toString();
  }

  /**
   * Generates a new unique service account URN using UUID.
   *
   * @return A new service account URN in the format "urn:li:corpuser:service_UUID"
   */
  @Nonnull
  public static String generateServiceAccountUrn() {
    return "urn:li:corpuser:" + generateServiceAccountId();
  }

  /**
   * Maps an EntityResponse to a ServiceAccount GraphQL object.
   *
   * @param response The entity response to map
   * @return The mapped ServiceAccount, or null if response is null
   */
  @Nullable
  public static ServiceAccount mapToServiceAccount(@Nullable EntityResponse response) {
    if (response == null) {
      return null;
    }

    final ServiceAccount serviceAccount = new ServiceAccount();
    serviceAccount.setUrn(response.getUrn().toString());
    serviceAccount.setType(EntityType.CORP_USER);

    // Extract name from URN
    serviceAccount.setName(extractNameFromUrn(response.getUrn().toString()));

    // Extract info from the corpUserInfo aspect
    final EnvelopedAspect infoAspect =
        response.getAspects().get(Constants.CORP_USER_INFO_ASPECT_NAME);
    if (infoAspect != null) {
      try {
        final CorpUserInfo info = new CorpUserInfo(infoAspect.getValue().data());
        if (info.hasDisplayName()) {
          serviceAccount.setDisplayName(info.getDisplayName());
        }
        if (info.hasTitle()) {
          serviceAccount.setDescription(info.getTitle());
        }
      } catch (Exception e) {
        log.warn(
            "Failed to parse corpUserInfo for service account: {}",
            response.getUrn().toString(),
            e);
      }
    }

    return serviceAccount;
  }
}
