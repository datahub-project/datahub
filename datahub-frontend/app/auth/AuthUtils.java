package auth;

import com.linkedin.common.urn.CorpuserUrn;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import play.mvc.Http;

@Slf4j
public class AuthUtils {

  /**
   * The config path that determines whether Metadata Service Authentication is enabled.
   *
   * <p>When enabled, the frontend server will proxy requests to the Metadata Service without
   * requiring them to have a valid frontend-issued Session Cookie. This effectively means
   * delegating the act of authentication to the Metadata Service. It is critical that if Metadata
   * Service authentication is enabled at the frontend service layer, it is also enabled in the
   * Metadata Service itself. Otherwise, unauthenticated traffic may reach the Metadata itself.
   *
   * <p>When disabled, the frontend server will require that all requests have a valid Session
   * Cookie associated with them. Otherwise, requests will be denied with an Unauthorized error.
   */
  public static final String METADATA_SERVICE_AUTH_ENABLED_CONFIG_PATH =
      "metadataService.auth.enabled";

  /** The attribute inside session cookie representing a GMS-issued access token */
  public static final String SESSION_COOKIE_GMS_TOKEN_NAME = "token";

  /**
   * An ID used to identify system callers that are internal to DataHub. Provided via configuration.
   */
  public static final String SYSTEM_CLIENT_ID_CONFIG_PATH = "systemClientId";

  /**
   * An Secret used to authenticate system callers that are internal to DataHub. Provided via
   * configuration.
   */
  public static final String SYSTEM_CLIENT_SECRET_CONFIG_PATH = "systemClientSecret";

  /** Cookie name for redirect url that is manually separated from the session to reduce size */
  public static final String REDIRECT_URL_COOKIE_NAME = "REDIRECT_URL";

  public static final CorpuserUrn DEFAULT_ACTOR_URN = new CorpuserUrn("datahub");

  public static final String LOGIN_ROUTE = "/login";
  public static final String USER_NAME = "username";
  public static final String PASSWORD = "password";
  public static final String ACTOR = "actor";
  public static final String ACCESS_TOKEN = "token";
  public static final String FULL_NAME = "fullName";
  public static final String EMAIL = "email";
  public static final String TITLE = "title";
  public static final String INVITE_TOKEN = "inviteToken";
  public static final String RESET_TOKEN = "resetToken";

  /**
   * Determines whether the inbound request should be forward to downstream Metadata Service. Today,
   * this simply checks for the presence of an "Authorization" header or the presence of a valid
   * session cookie issued by the frontend.
   *
   * <p>Note that this method DOES NOT actually verify the authentication token of an inbound
   * request. That will be handled by the downstream Metadata Service. Until then, the request
   * should be treated as UNAUTHENTICATED.
   *
   * <p>Returns true if the request is eligible to be forwarded to GMS, false otherwise.
   */
  public static boolean isEligibleForForwarding(Http.Request req) {
    return hasValidSessionCookie(req) || hasAuthHeader(req);
  }

  /**
   * Returns true if a request has a valid session cookie issued by the frontend server. Note that
   * this DOES NOT verify whether the token within the session cookie will be accepted by the
   * downstream GMS service.
   *
   * <p>Note that we depend on the presence of 2 cookies, one accessible to the browser and one not,
   * as well as their agreement to determine authentication status.
   */
  public static boolean hasValidSessionCookie(final Http.Request req) {
    Map<String, String> sessionCookie = req.session().data();
    return sessionCookie.containsKey(ACCESS_TOKEN)
        && sessionCookie.containsKey(ACTOR)
        && req.getCookie(ACTOR).isPresent()
        && req.session().data().get(ACTOR).equals(req.getCookie(ACTOR).get().value());
  }

  /** Returns true if a request includes the Authorization header, false otherwise */
  public static boolean hasAuthHeader(final Http.Request req) {
    return req.getHeaders().contains(Http.HeaderNames.AUTHORIZATION);
  }

  /**
   * Creates a client authentication cookie (actor cookie) with a specified TTL in hours.
   *
   * @param actorUrn the urn of the authenticated actor, e.g. "urn:li:corpuser:datahub"
   * @param ttlInHours the number of hours until the actor cookie expires after being set
   */
  public static Http.Cookie createActorCookie(
      @Nonnull final String actorUrn,
      @Nonnull final Integer ttlInHours,
      @Nonnull final String sameSite,
      final boolean isSecure) {
    return Http.Cookie.builder(ACTOR, actorUrn)
        .withHttpOnly(false)
        .withMaxAge(Duration.of(ttlInHours, ChronoUnit.HOURS))
        .withSameSite(convertSameSiteValue(sameSite))
        .withSecure(isSecure)
        .build();
  }

  public static Map<String, String> createSessionMap(
      final String userUrnStr, final String accessToken) {
    final Map<String, String> sessionAttributes = new HashMap<>();
    sessionAttributes.put(ACTOR, userUrnStr);
    sessionAttributes.put(ACCESS_TOKEN, accessToken);
    return sessionAttributes;
  }

  private AuthUtils() {}

  private static Http.Cookie.SameSite convertSameSiteValue(@Nonnull final String sameSiteValue) {
    try {
      return Http.Cookie.SameSite.valueOf(sameSiteValue);
    } catch (IllegalArgumentException e) {
      log.warn(
          String.format(
              "Invalid AUTH_COOKIE_SAME_SITE value: %s. Using LAX instead.", sameSiteValue),
          e);
      return Http.Cookie.SameSite.LAX;
    }
  }
}
