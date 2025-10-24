package io.datahubproject.openapi.scim.util;

import com.datahub.util.RecordUtils;
import com.linkedin.common.Origin;
import com.linkedin.common.OriginType;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.Constants;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility class for filtering and processing SCIM entities. Provides methods to identify
 * SCIM-created entities and safely extract external IDs.
 */
@Slf4j
public class ScimUtil {

  public static final String SCIM_CLIENT_PREFIX = "SCIM_client_";

  /**
   * Checks if an entity was created via SCIM. Only SCIM-created entities should be processed by
   * SCIM endpoints.
   *
   * @param origin the Origin aspect from the entity
   * @return true if the entity was created via SCIM, false otherwise
   */
  public static boolean isScimCreatedEntity(Origin origin) {
    if (origin == null || !origin.hasType() || !origin.getType().equals(OriginType.EXTERNAL)) {
      return false;
    }
    if (origin.getExternalType() == null) {
      return false;
    }
    return origin.getExternalType().startsWith(SCIM_CLIENT_PREFIX);
  }

  /**
   * Utility method to extract external ID from SCIM-created entities only. This method safely
   * handles the external type extraction without throwing exceptions.
   *
   * @param origin the Origin aspect from the entity
   * @return the external ID if the entity was created via SCIM, null otherwise
   */
  public static String extractScimExternalId(Origin origin) {
    if (!isScimCreatedEntity(origin)) {
      return null;
    }

    String externalType = origin.getExternalType();
    String externalId = externalType.substring(SCIM_CLIENT_PREFIX.length());
    // If externalType is just the prefix (no externalId) or contains 'null' string, return null
    return externalId.isEmpty() || externalId.equals("null") ? null : externalId;
  }

  /**
   * Helper method to extract Origin aspect from a list of EnvelopedAspect objects.
   *
   * @param aspects list of EnvelopedAspect objects
   * @return the Origin aspect if found, null otherwise
   */
  public static Origin extractOriginFromAspects(List<EnvelopedAspect> aspects) {
    for (EnvelopedAspect envelopedAspect : aspects) {
      if (Constants.ORIGIN_ASPECT_NAME.equals(envelopedAspect.getName())) {
        return RecordUtils.toRecordTemplate(Origin.class, envelopedAspect.getValue().data());
      }
    }
    return null;
  }

  /**
   * Logs a warning about a non-SCIM entity with unexpected external type. Only logs when there's an
   * unexpected external type (not null/empty).
   *
   * @param urn the URN of the entity
   * @param origin the Origin aspect of the entity
   * @param action the action being performed (e.g., "Skipping", "Attempted to access")
   */
  public static void logNonScimEntityWithUnexpectedType(Object urn, Origin origin, String action) {
    if (origin != null && origin.getExternalType() != null && !origin.getExternalType().isEmpty()) {
      log.warn(
          "{} non-SCIM entity {} with unexpected external type: {}",
          action,
          urn,
          origin.getExternalType());
    }
  }
}
