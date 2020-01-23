package utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.restli.client.RestLiResponseException;
import javax.annotation.Nonnull;
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

}