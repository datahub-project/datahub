package utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.util.RestliUtil;
import com.linkedin.restli.client.RestLiResponseException;
import java.io.IOException;
import javax.annotation.Nonnull;
import play.Logger;
import play.libs.Json;


/**
 * Helper class for controller APIs
 */
public class ControllerUtil {

  private ControllerUtil() {
    //utility class
  }

  @Nonnull
  public static <E extends Throwable> JsonNode errorResponse(@Nonnull E e) {
    return errorResponse(e.toString());
  }

  @Nonnull
  public static JsonNode errorResponse(@Nonnull String msg) {
    return Json.newObject().put("msg", msg);
  }

  public static boolean checkErrorCode(Exception e, int statusCode) {
    return (e instanceof RestLiResponseException) && (((RestLiResponseException) e).getStatus() == statusCode);
  }

  /**
   * Creates a generic jsonNode out of a key and value which is a very common use case
   * @param key
   * @param value
   * @return
   */
  @Nonnull
  public static JsonNode jsonNode(@Nonnull final String key, @Nonnull final Object value) {
    JsonNode node = null;
    if (value instanceof RecordTemplate) {
      try {
        node = RestliUtil.toJsonNode((RecordTemplate) value);
      } catch (final IOException e) {
        Logger.error("Could not create a json", e);
      }
    } else if (value instanceof JsonNode) {
      node = (JsonNode) value;
    } else {
      node = Json.toJson(value);
    }
    return Json.newObject().set(key, node);
  }
}