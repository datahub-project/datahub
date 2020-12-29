package graphql;

import com.typesafe.config.Config;
import play.mvc.Http;

/**
 * Provides session context to components of the GraphQL Engine at runtime.
 */
public class QueryContext {

    private final Http.Session _session;
    private final Config _appConfig;

    public QueryContext(Http.Session session) {
        this(session, null);
    }

    public QueryContext(Http.Session session, Config appConfig) {
        _session = session;
        _appConfig = appConfig;
    }

    /**
     * Returns true if the current user is authenticated, false otherwise.
     */
    public boolean isAuthenticated() {
        return getSession().containsKey("auth_token"); // TODO: Compute this once by validating the signed auth token.
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
        return _session.get("user");
    }

    /**
     * Retrieves the hashed auth token associated with the current user.
     */
    public String getAuthToken() {
        return _session.get("auth_token");
    }

    /*
        TODO:
        public Role getRole()
     */
}
