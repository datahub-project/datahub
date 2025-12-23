package com.linkedin.metadata.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.MonitorKey;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Utility methods for working with Monitor URNs. */
public class MonitorUrnUtils {

  private MonitorUrnUtils() {}

  /**
   * Generate a monitor URN from entity URN and monitor ID.
   *
   * @param entityUrn the entity being monitored
   * @param monitorId unique identifier for the monitor
   * @return the generated monitor URN
   */
  @Nonnull
  public static Urn generateMonitorUrn(@Nonnull Urn entityUrn, @Nonnull String monitorId) {
    MonitorKey key = new MonitorKey();
    key.setEntity(entityUrn);
    key.setId(monitorId);
    return EntityKeyUtils.convertEntityKeyToUrn(key, Constants.MONITOR_ENTITY_NAME);
  }

  /**
   * Extract the entity URN from a monitor URN.
   *
   * <p>Monitor URN format: urn:li:monitor:(entity_urn,id)
   *
   * @param monitorUrn the monitor URN
   * @return the entity URN, or null if extraction fails
   */
  @Nullable
  public static Urn getEntityUrn(@Nonnull Urn monitorUrn) {
    try {
      return UrnUtils.getUrn(monitorUrn.getEntityKey().get(0));
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Extract the monitor ID from a monitor URN.
   *
   * <p>Monitor URN format: urn:li:monitor:(entity_urn,id)
   *
   * @param monitorUrn the monitor URN
   * @return the monitor ID, or null if extraction fails
   */
  @Nullable
  public static String getMonitorId(@Nonnull Urn monitorUrn) {
    try {
      return monitorUrn.getEntityKey().get(1);
    } catch (Exception e) {
      return null;
    }
  }
}
