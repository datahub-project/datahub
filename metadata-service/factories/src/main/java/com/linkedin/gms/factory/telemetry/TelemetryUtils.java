/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.gms.factory.telemetry;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.telemetry.TelemetryClientId;
import io.datahubproject.metadata.context.OperationContext;
import jakarta.annotation.Nonnull;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class TelemetryUtils {

  public static final String CLIENT_ID_URN = "urn:li:telemetry:clientId";
  public static final String CLIENT_ID_ASPECT = "telemetryClientId";

  private static String _clientId;

  public static String getClientId(
      @Nonnull OperationContext opContext, EntityService<?> entityService) {
    if (_clientId == null) {
      createClientIdIfNotPresent(opContext, entityService);
      RecordTemplate clientIdTemplate =
          entityService.getLatestAspect(
              opContext, UrnUtils.getUrn(CLIENT_ID_URN), CLIENT_ID_ASPECT);
      // Should always be present here from above, so no need for null check
      _clientId = ((TelemetryClientId) clientIdTemplate).getClientId();
    }
    return _clientId;
  }

  private static void createClientIdIfNotPresent(
      @Nonnull OperationContext opContext, EntityService<?> entityService) {
    String uuid = UUID.randomUUID().toString();
    TelemetryClientId clientId = new TelemetryClientId().setClientId(uuid);
    final AuditStamp clientIdStamp = new AuditStamp();
    clientIdStamp.setActor(UrnUtils.getUrn(Constants.SYSTEM_ACTOR));
    clientIdStamp.setTime(System.currentTimeMillis());
    entityService.ingestAspectIfNotPresent(
        opContext, UrnUtils.getUrn(CLIENT_ID_URN), CLIENT_ID_ASPECT, clientId, clientIdStamp, null);
  }

  private TelemetryUtils() {
    throw new UnsupportedOperationException();
  }
}
