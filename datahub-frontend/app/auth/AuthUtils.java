package auth;

import com.linkedin.common.urn.CorpuserUrn;
import lombok.extern.slf4j.Slf4j;
import play.mvc.Http;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

@Slf4j
public class AuthUtils {

    public static final String SESSION_TTL_CONFIG_PATH = "auth.session.ttlInHours";
    public static final Integer DEFAULT_SESSION_TTL_HOURS = 720;
    public static final CorpuserUrn DEFAULT_ACTOR_URN = new CorpuserUrn("datahub");

    public static final String LOGIN_ROUTE = "/login";
    public static final String USER_NAME = "username";
    public static final String PASSWORD = "password";
    public static final String ACTOR = "actor";

    /**
     * Determines whether the inbound request should be forward to downstream GMS. Today, this simply
     * checks for the presence of an "Authorization" header or the presence of a valid session cookie issued
     * by the frontend.
     *
     * Note that this method DOES NOT actually verify the authentication token of an inbound request. That will
     * be handled by the downstream GMS service. Until then, the request should be treated as UNAUTHENTICATED.
     *
     * returns true if the request is eligible to be forwarded to GMS, false otherwise.
     */
    public static boolean isEligibleForForwarding(Http.Context ctx) {
        return hasValidSessionCookie(ctx) || hasAuthHeader(ctx);
    }

    /**
     * Returns true if a request has a valid session cookie issued by the frontend server.
     * Note that this DOES NOT verify whether the token within the session cookie will be accepted
     * by the downstream GMS service.
     *
     * Note that we depend on the presence of 2 cookies, one accessible to the browser and one not,
     * as well as their agreement to determine authentication status.
     */
    public static boolean hasValidSessionCookie(final Http.Context ctx) {
        return ctx.session().containsKey(ACTOR)
                && ctx.request().cookie(ACTOR) != null
                && ctx.session().get(ACTOR).equals(ctx.request().cookie(ACTOR).value());
    }

    /**
     * Returns true if a request is authenticated, false otherwise.
     *
     * Note that we depend on the presence of 2 cookies, one accessible to the browser and one not,
     * as well as their agreement to determine authentication status.
     */
    public static boolean hasAuthHeader(final Http.Context ctx) {
        return ctx.request().getHeaders().contains("Authorization");
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

    /**
     * Attempt to generate a GMS session token for a particular user after they've been successfully logged in.
     *
     * Notice that the method that is
     */
    public static String generateGmsSessionToken(final AuthClient authClient, final String userUrn) {
        try {
            return authClient.generateSessionTokenForUser(userUrn);
        } catch (Exception e) {
            return null;
        }
    }

    private AuthUtils() { }

}
