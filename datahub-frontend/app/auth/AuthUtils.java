package auth;

import com.linkedin.common.urn.CorpuserUrn;
import lombok.extern.slf4j.Slf4j;
import play.mvc.Http;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

@Slf4j
public class AuthUtils {

    /**
     * The config path that determines whether Metadata Service Authentication is enabled.
     *
     * When enabled, the frontend server will proxy requests to the Metadata Service without requiring them to have a valid
     * frontend-issued Session Cookie. This effectively means delegating the act of authentication to the Metadata Service. It
     * is critical that if Metadata Service authentication is enabled at the frontend service layer, it is also enabled in the
     * Metadata Service itself. Otherwise, unauthenticated traffic may reach the Metadata itself.
     *
     * When disabled, the frontend server will require that all requests have a valid Session Cookie associated with them. Otherwise,
     * requests will be denied with an Unauthorized error.
     */
    public static final String METADATA_SERVICE_AUTH_ENABLED_CONFIG_PATH = "metadataService.auth.enabled";

    /**
     * The attribute inside session cookie representing a GMS-issued access token
     */
    public static final String SESSION_COOKIE_GMS_TOKEN_NAME = "token";

    /**
     * An ID used to identify system callers that are internal to DataHub. Provided via configuration.
     */
    public static final String SYSTEM_CLIENT_ID_CONFIG_PATH = "systemClientId";

    /**
     * An Secret used to authenticate system callers that are internal to DataHub. Provided via configuration.
     */
    public static final String SYSTEM_CLIENT_SECRET_CONFIG_PATH = "systemClientSecret";

    public static final String SESSION_TTL_CONFIG_PATH = "auth.session.ttlInHours";
    public static final Integer DEFAULT_SESSION_TTL_HOURS = 720;
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
     * Determines whether the inbound request should be forward to downstream Metadata Service. Today, this simply
     * checks for the presence of an "Authorization" header or the presence of a valid session cookie issued
     * by the frontend.
     *
     * Note that this method DOES NOT actually verify the authentication token of an inbound request. That will
     * be handled by the downstream Metadata Service. Until then, the request should be treated as UNAUTHENTICATED.
     *
     * Returns true if the request is eligible to be forwarded to GMS, false otherwise.
     */
    public static boolean isEligibleForForwarding(Http.Request req) {
        return hasValidSessionCookie(req) || hasAuthHeader(req);
    }

    /**
     * Returns true if a request has a valid session cookie issued by the frontend server.
     * Note that this DOES NOT verify whether the token within the session cookie will be accepted
     * by the downstream GMS service.
     *
     * Note that we depend on the presence of 2 cookies, one accessible to the browser and one not,
     * as well as their agreement to determine authentication status.
     */
    public static boolean hasValidSessionCookie(final Http.Request req) {
        return req.session().data().containsKey(ACTOR)
                && req.cookie(ACTOR) != null
                && req.session().data().get(ACTOR).equals(req.cookie(ACTOR).value());
    }

    /**
     * Returns true if a request includes the Authorization header, false otherwise
     */
    public static boolean hasAuthHeader(final Http.Request req) {
        return req.getHeaders().contains(Http.HeaderNames.AUTHORIZATION);
    }

    /**
     * Creates a client authentication cookie (actor cookie) with a specified TTL in hours.
     *
     * @param actorUrn the urn of the authenticated actor, e.g. "urn:li:corpuser:datahub"
     * @param ttlInHours the number of hours until the actor cookie expires after being set
     */
    public static Http.Cookie createActorCookie(final String actorUrn, final Integer ttlInHours) {
        return Http.Cookie.builder(ACTOR, actorUrn)
                .withHttpOnly(false)
                .withMaxAge(Duration.of(ttlInHours, ChronoUnit.HOURS))
                .build();
    }

    private AuthUtils() { }

}
