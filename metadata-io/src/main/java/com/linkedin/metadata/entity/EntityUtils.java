package com.linkedin.metadata.entity;

import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EnvelopedAspect;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;


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

  public static Map<String, Long> convertVersionStamp(String versionStamp) {
    Map<String, Long> aspectVersionMap = new HashMap<>();
    if (StringUtils.isBlank(versionStamp)) {
      return aspectVersionMap;
    }
    String[] aspectNameVersionPairs = versionStamp.split(";");
    for (String pair : aspectNameVersionPairs) {
      String[] tokens = pair.split(":");
      if (tokens.length != 2) {
        throw new IllegalArgumentException("Invalid version stamp cannot be parsed: " + versionStamp);
      }
      try {
        aspectVersionMap.put(tokens[0], Long.valueOf(tokens[1]));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Invalid value for aspect version: " + tokens[1]);
      }
    }

    return aspectVersionMap;
  }

  public static String constructVersionStamp(SortedMap<String, Long> versionStampMap) {
    StringBuilder versionStamp = versionStampMap.entrySet().stream()
        .collect(StringBuilder::new, (builder, entry) -> builder.append(entry.getKey())
            .append(":")
            .append(entry.getValue()).append(";"), StringBuilder::append);
    // trim off last ;
    return versionStamp.substring(0, versionStamp.length() - 1);
  }
}
