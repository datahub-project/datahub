package com.linkedin.metadata.entity;

import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EnvelopedAspect;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class EntityUtils {
  private EntityUtils() {
  }

  /**
   * Check if entity is removed (removed=true in Status aspect)
   */
  public static boolean checkIfRemoved(EntityService entityService, Urn entityUrn) {
    try {
      EnvelopedAspect statusAspect =
          entityService.getLatestEnvelopedAspect(entityUrn.getEntityType(), entityUrn, "status");
      if (statusAspect == null) {
        return false;
      }
      Status status = new Status(statusAspect.getValue().data());
      return status.isRemoved();
    } catch (Exception e) {
      log.error("Error while checking if {} is removed", entityUrn, e);
      return false;
    }
  }
}
