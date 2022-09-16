package utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

import java.util.Map;

public class GsonUtils {
    public static final Gson GSON = new GsonBuilder().create();

    public static JsonObject objectToJson(Object object) {
        return GSON.toJsonTree(object).getAsJsonObject();
    }

    public static String stringify(Object object) {
        return GSON.toJson(object);
    }

    public static JsonObject parse(String json) {
        return GSON.fromJson(json, JsonObject.class);
    }

    public static <T> T parse(String json, Class<T> clazz) {
        return GSON.fromJson(json, clazz);
    }

    public static JsonObject mapToJsonObject(Map<String, Object> map) {
        JsonObject result = new JsonObject();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            result.add(entry.getKey(), GSON.toJsonTree(entry.getValue()));
        }
        return result;
    }

    public static JsonObject stringifyMap(Map<String, Object> map) {
        JsonObject result = new JsonObject();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            Object value = entry.getValue();
            result.addProperty(entry.getKey(), value == null ? "" : value.toString());
        }
        return result;
    }
}
