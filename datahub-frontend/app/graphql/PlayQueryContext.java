package graphql;

import com.typesafe.config.Config;
import play.mvc.Http;
import com.linkedin.datahub.graphql.QueryContext;

import static security.AuthConstants.AUTH_TOKEN;
import static security.AuthConstants.USER;


/**
 * Provides session context to components of the GraphQL Engine at runtime.
 */
public class PlayQueryContext implements QueryContext {

    private final Http.Session _session;
    private final Config _appConfig;

    public PlayQueryContext(Http.Session session) {
        this(session, null);
    }

    public PlayQueryContext(Http.Session session, Config appConfig) {
        _session = session;
        _appConfig = appConfig;
    }

    /**
     * Returns true if the current user is authenticated, false otherwise.
     */
    @Override
    public boolean isAuthenticated() {
        return getSession().containsKey(AUTH_TOKEN); // TODO: Compute this once by validating the signed auth token.
    }

    /**
     * Returns the currently logged in user string
     */
    @Override
    public String getActor() {
        return _session.get(USER);
    }

    /**
     * Retrieves the {@link Http.Session} object associated with the current user.
     */
    public Http.Session getSession() {
        return _session;
    }

    /**
     * Retrieves the {@link Config} object associated with the play application.
     */
    public Config getAppConfig() {
        return _appConfig;
    }

    /**
     * Retrieves the user name associated with the current user.
     */
    public String getUserName() {
        return _session.get(USER);
    }

    /**
     * Retrieves the hashed auth token associated with the current user.
     */
    public String getAuthToken() {
        return _session.get(AUTH_TOKEN);
    }

}
