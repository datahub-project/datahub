package com.linkedin.gms.factory.telemetry;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.ClientId;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;

import java.io.*;
import java.util.UUID;

import lombok.extern.slf4j.Slf4j;


@Slf4j
final class TelemetryUtils {

    public static final String CONFIG_FILE_NAME = "telemetry-config.json";
    public static final String CLIENT_ID_URN = "urn:li:telemetry:clientId";
    public static final String CLIENT_ID_ASPECT = "clientId";

    private static String _clientId;

//    public static void createConfig(String folderPath) {
//        try {
//            _clientId = UUID.randomUUID().toString();
//            final File datahubFolder = new File(folderPath);
//            if (!datahubFolder.exists()) {
//                boolean created = datahubFolder.mkdirs();
//                if (!created) {
//                    log.warn("Unable to create telemetry configuration folder.");
//                    return;
//                }
//            }
//
//            String configFilePath = folderPath + File.pathSeparator + CONFIG_FILE_NAME;
//            final File configFile = new File(folderPath + File.pathSeparator + CONFIG_FILE_NAME);
//            if (!configFile.exists()) {
//
//                JSONObject config = new JSONObject();
//
//                config.put("client_id", _clientId);
//
//                FileWriter file = new FileWriter(configFilePath);
//                file.write(config.toString());
//                file.close();
//            }
//        } catch (FileNotFoundException e) {
//            log.error("Could not open telemetry config:", e);
//        } catch (IOException e) {
//            log.error("Error writing telemetry config:", e);
//        }
//
//    }
//
//    public static void loadConfig(String configFilePath) {
//        try {
//            final File configFile = new File(configFilePath);
//            if (configFile.exists()) {
//
//                InputStream inputFile = new FileInputStream(configFilePath);
//                String jsonTxt = IOUtils.toString(inputFile, StandardCharsets.UTF_8);
//                inputFile.close();
//
//                JSONObject config = new JSONObject(jsonTxt);
//
//                _clientId = config.get("client_id").toString();
//            }
//        } catch (FileNotFoundException e) {
//            log.error("Could not open telemetry config:", e);
//        } catch (IOException e) {
//            log.error("Error reading telemetry config:", e);
//        }
//    }

    public static String getClientId(EntityService entityService) {
        if (_clientId == null) {
            createClientIdIfNotPresent(entityService);
            RecordTemplate clientIdTemplate = entityService.getLatestAspect(UrnUtils.getUrn(CLIENT_ID_URN), CLIENT_ID_ASPECT);
            // Should always be present here from above, so no need for null check
            _clientId = ((ClientId) clientIdTemplate).getClientId();
        }
        return _clientId;
    }

    private static void createClientIdIfNotPresent(EntityService entityService) {
        String uuid = UUID.randomUUID().toString();
        ClientId clientId = new ClientId().setClientId(uuid);
        final AuditStamp clientIdStamp = new AuditStamp();
        clientIdStamp.setActor(UrnUtils.getUrn(Constants.SYSTEM_ACTOR));
        clientIdStamp.setTime(System.currentTimeMillis());
        entityService.ingestAspectIfNotPresent(UrnUtils.getUrn(CLIENT_ID_URN), CLIENT_ID_ASPECT, clientId, clientIdStamp, null);
    }

    private TelemetryUtils() {
        throw new UnsupportedOperationException();
    }

}
