package com.linkedin.gms.factory.telemetry;

import com.mixpanel.mixpanelapi.MessageBuilder;
import com.mixpanel.mixpanelapi.MixpanelAPI;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.json.JSONObject;

import java.io.*;
import java.util.UUID;

import lombok.extern.slf4j.Slf4j;

import static java.lang.Boolean.parseBoolean;

@Slf4j
final class Telemetry {
    static final String MIXPANEL_TOKEN = "5ee83d940754d63cacbf7d34daa6f44a";
    static String DATAHUB_FOLDER_PATH;
    static String CONFIG_FILE_PATH;

    static boolean enabled = true;
    static String clientId;

    private static MixpanelAPI mixpanel;
    private static MessageBuilder mixpanelBuilder;

    static {
        DATAHUB_FOLDER_PATH = System.getenv("DATAHUB_HOME_FOLDER");
        if (DATAHUB_FOLDER_PATH == null || DATAHUB_FOLDER_PATH.isEmpty()) {
            DATAHUB_FOLDER_PATH = System.getProperty("user.home") + "/.datahub";
        } CONFIG_FILE_PATH = DATAHUB_FOLDER_PATH + "/telemetry-config.json";

        File configFile = new File(CONFIG_FILE_PATH);

        if (!configFile.exists()) {
            clientId = UUID.randomUUID().toString();
            updateConfig();
        } else {
            loadConfig();
        }

        if (enabled) {

            // initialize MixPanel instance and message builder
            mixpanel = new MixpanelAPI();
            mixpanelBuilder = new MessageBuilder(MIXPANEL_TOKEN);

            // set user-level properties
            JSONObject props = new JSONObject();
            props.put("java_version", System.getProperty("java.version"));
            props.put("os", System.getProperty("os.name"));
            JSONObject update = mixpanelBuilder.set(clientId, props);
            try {
                mixpanel.sendMessage(update);
            } catch (IOException e) {
                log.error("Error sending telemetry profile:", e);
            }
        }


    }

    public static void updateConfig() {
        try {
            final File datahubFolder = new File(DATAHUB_FOLDER_PATH);
            if (!datahubFolder.exists()) {
                datahubFolder.mkdirs();
            }

            final File configFile = new File(CONFIG_FILE_PATH);
            if (!configFile.exists()) {

                JSONObject config = new JSONObject();

                config.put("client_id", clientId);
                config.put("enabled", enabled);

                FileWriter file = new FileWriter(CONFIG_FILE_PATH);
                file.write(config.toString());
                file.close();
            }
        } catch (FileNotFoundException e) {
            log.error("Could not open telemetry config:", e);
        } catch (IOException e) {
            log.error("Error writing telemetry config:", e);
        }

    }

    public static void enable() {
        enabled = true;
    }

    public static void disable() {
        enabled = false;
    }

    public static void loadConfig() {
        try {
            final File configFile = new File(CONFIG_FILE_PATH);
            if (configFile.exists()) {

                InputStream inputFile = new FileInputStream(CONFIG_FILE_PATH);
                String jsonTxt = IOUtils.toString(inputFile, "UTF-8");
                inputFile.close();

                JSONObject config = new JSONObject(jsonTxt);

                String envEnabledRaw = System.getenv("DATAHUB_TELEMETRY_ENABLED");
                if (envEnabledRaw == null || envEnabledRaw.isEmpty()) {
                    envEnabledRaw = "true";
                }
                boolean envEnabled = envEnabledRaw.equals("true");

                clientId = config.get("client_id").toString();
                enabled = parseBoolean(config.get("enabled").toString()) && envEnabled;

            }
        } catch (FileNotFoundException e) {
            log.error("Could not open telemetry config:", e);
        } catch (IOException e) {
            log.error("Error reading telemetry config:", e);
        }
    }

    public static void ping(String eventName, JSONObject properties) {

        if (!enabled || mixpanelBuilder == null || mixpanel == null) {
            return;
        }

        try {
            JSONObject event = mixpanelBuilder.event(clientId, eventName, properties);
            mixpanel.sendMessage(event);
        } catch (IOException e) {
            log.error("Error reporting telemetry:", e);
        }
    }

    private Telemetry() {
        throw new UnsupportedOperationException();
    }

}
