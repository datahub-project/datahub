package auth;

import com.linkedin.common.urn.CorpuserUrn;
import play.mvc.Http;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class AuthUtils {

    public static final String SESSION_TTL_CONFIG_PATH = "auth.session.ttlInHours";
    public static final Integer DEFAULT_SESSION_TTL_HOURS = 720;
    public static final CorpuserUrn DEFAULT_ACTOR_URN = new CorpuserUrn("datahub");

    public static final String LOGIN_ROUTE = "/login";
    public static final String USER_NAME = "username";
    public static final String PASSWORD = "password";
    public static final String ACTOR = "actor";

    /**
     * Returns true if a request is authenticated, false otherwise.
     *
     * Note that we depend on the presence of 2 cookies, one accessible to the browser and one not,
     * as well as their agreement to determine authentication status.
     */
    public static boolean isAuthenticated(final Http.Context ctx) {
        return ctx.session().containsKey(ACTOR)
                && ctx.request().cookie(ACTOR) != null
                && ctx.session().get(ACTOR).equals(ctx.request().cookie(ACTOR).value());
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
