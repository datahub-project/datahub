package com.linkedin.datahub.upgrade.system.restoreindices;

import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.upgrade.DataHubUpgradeRequest;
import java.util.List;
import java.util.Map;

/** Shared fixtures for the restore-indices step tests. */
public final class RestoreIndicesTestHelpers {

  private RestoreIndicesTestHelpers() {}

  /** An EntityResponse carrying a dataHubUpgradeRequest aspect at the given version. */
  public static EntityResponse upgradeRequestResponse(String version) {
    DataHubUpgradeRequest request =
        new DataHubUpgradeRequest().setVersion(version).setTimestampMs(0L);
    EnvelopedAspect aspect = new EnvelopedAspect().setValue(new Aspect(request.data()));
    return new EntityResponse()
        .setAspects(
            new EnvelopedAspectMap(Map.of(Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME, aspect)));
  }

  /** True if any captured scan targets the given aspect + urn prefix. */
  public static boolean scanContains(
      List<RestoreIndicesArgs> scans, String aspectName, String urnLike) {
    return scans.stream()
        .anyMatch(a -> aspectName.equals(a.aspectName()) && urnLike.equals(a.urnLike()));
  }
}
