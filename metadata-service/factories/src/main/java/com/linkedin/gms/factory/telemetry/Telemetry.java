package com.linkedin.gms.factory.telemetry;

import com.mixpanel.mixpanelapi.MessageBuilder;
import com.mixpanel.mixpanelapi.MixpanelAPI;
import org.json.simple.parser.JSONParser;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.json.JSONObject;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.UUID;

import lombok.extern.slf4j.Slf4j;

import static java.lang.Boolean.parseBoolean;

@Slf4j
public class Telemetry {
    static final String MIXPANEL_TOKEN = "5ee83d940754d63cacbf7d34daa6f44a";
    static final String DATAHUB_FOLDER_PATH = System.getProperty("user.home") + "./datahub";
    static final String CONFIG_FILE_PATH = DATAHUB_FOLDER_PATH + "/telemetry-config.json";

    static boolean enabled;
    static String client_id;

    private static MixpanelAPI mixpanel;
    private static MessageBuilder mixpanelBuilder;

    public static void main(String args[]) {

        try{
            File CONFIG_FILE = new File(CONFIG_FILE_PATH);

            if(!CONFIG_FILE.exists()){
                client_id = UUID.randomUUID().toString();
                update_config();
            }else{
                load_config();
            }

            mixpanel = new MixpanelAPI();
            mixpanelBuilder = new MessageBuilder(MIXPANEL_TOKEN);

            JSONObject props = new JSONObject();
            props.put("java_version", System.getProperty("java.version"));
            props.put("os", System.getProperty("os.name"));
            JSONObject update = mixpanelBuilder.set(client_id, props);
            mixpanel.sendMessage(update);

        }catch (Exception e) {
            log.debug("Error initializing telemetry:\n" + ExceptionUtils.getStackTrace(e));
        }

    }

    public static void update_config() {
        try {
            File DATAHUB_FOLDER = new File(DATAHUB_FOLDER_PATH);
            if (!DATAHUB_FOLDER.exists()) {
                DATAHUB_FOLDER.mkdirs();
            }

            File CONFIG_FILE = new File(CONFIG_FILE_PATH);
            if (!CONFIG_FILE.exists()) {

                JSONObject config = new JSONObject();

                UUID client_id = UUID.randomUUID();
                config.put("client_id", client_id);
                config.put("enabled", enabled);

                FileWriter file = new FileWriter(CONFIG_FILE_PATH);
                file.write(config.toJSONString());

            }
        } catch (Exception e) {
            log.debug("Error configuring telemetry:\n" + ExceptionUtils.getStackTrace(e));
        }

    }

    public static void enable() {
        enabled = true;
    }

    public static void disable() {
        enabled = false;
    }

    public static void load_config() {
        try {
            File CONFIG_FILE = new File(CONFIG_FILE_PATH);
            if (CONFIG_FILE.exists()) {

                JSONObject config = new JSONObject(new FileReader(CONFIG_FILE));

                String env_enabled_raw = System.getenv("DATAHUB_TELEMETRY_ENABLED");
                if (env_enabled_raw == null || env_enabled_raw.isEmpty()) {
                    env_enabled_raw = "true";
                }
                boolean env_enabled = env_enabled_raw.equals("true");

                client_id = config.get("client_id").toString();

                enabled = parseBoolean(config.get("enabled").toString()) && env_enabled;

            }
        } catch (Exception e) {
            log.debug("Error configuring telemetry:\n" + ExceptionUtils.getStackTrace(e));
        }
    }

    public static void ping(String eventName, JSONObject properties) {
        if (!enabled || mixpanelBuilder == null || mixpanel == null) {
            return;
        }

        try {
            JSONObject planEvent =
                    mixpanelBuilder.event(client_id, eventName, properties);
            mixpanel.sendMessage(planEvent);
        } catch (Exception e) {
            log.debug("Error reporting telemetry:\n" + ExceptionUtils.getStackTrace(e));
        }
    }
}
