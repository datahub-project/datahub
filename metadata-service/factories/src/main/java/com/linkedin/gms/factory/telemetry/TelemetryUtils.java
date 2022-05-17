package com.linkedin.gms.factory.telemetry;

import com.linkedin.common.AuditStamp;
import com.linkedin.telemetry.TelemetryClientId;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import java.util.UUID;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public final class TelemetryUtils {

    public static final String CLIENT_ID_URN = "urn:li:telemetry:clientId";
    public static final String CLIENT_ID_ASPECT = "clientId";

    private static String _clientId;

    public static String getClientId(EntityService entityService) {
        if (_clientId == null) {
            createClientIdIfNotPresent(entityService);
            RecordTemplate clientIdTemplate = entityService.getLatestAspect(UrnUtils.getUrn(CLIENT_ID_URN), CLIENT_ID_ASPECT);
            // Should always be present here from above, so no need for null check
            _clientId = ((TelemetryClientId) clientIdTemplate).getClientId();
        }
        return _clientId;
    }

    private static void createClientIdIfNotPresent(EntityService entityService) {
        String uuid = UUID.randomUUID().toString();
        TelemetryClientId clientId = new TelemetryClientId().setClientId(uuid);
        final AuditStamp clientIdStamp = new AuditStamp();
        clientIdStamp.setActor(UrnUtils.getUrn(Constants.SYSTEM_ACTOR));
        clientIdStamp.setTime(System.currentTimeMillis());
        entityService.ingestAspectIfNotPresent(UrnUtils.getUrn(CLIENT_ID_URN), CLIENT_ID_ASPECT, clientId, clientIdStamp, null);
    }

    private TelemetryUtils() {
        throw new UnsupportedOperationException();
    }

}
