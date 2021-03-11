package react.graphql;

import com.typesafe.config.Config;
import play.mvc.Http;
import com.linkedin.datahub.graphql.QueryContext;

import static react.auth.AuthUtils.*;

/**
 * Provides session context to components of the GraphQL Engine at runtime.
 */
public class PlayQueryContext implements QueryContext {

    private final Http.Context _context;
    private final Config _appConfig;

    public PlayQueryContext(Http.Context context) {
        this(context, null);
    }

    public PlayQueryContext(Http.Context context, Config appConfig) {
        _context = context;
        _appConfig = appConfig;
    }

    /**
     * Returns true if the current user is authenticated, false otherwise.
     */
    @Override
    public boolean isAuthenticated() {
        return _context.session().containsKey(ACTOR);
    }

    /**
     * Returns the currently logged in user string
     */
    @Override
    public String getActor() {
        return _context.session().get(ACTOR);
    }

    /**
     * Retrieves the {@link Http.Session} object associated with the current user.
     */
    public Http.Session getSession() {
        return _context.session();
    }

    /**
     * Retrieves the {@link Http.Context} object associated with the current request.
     */
    public Http.Context getPlayContext() {
        return _context;
    }

    /**
     * Retrieves the {@link Config} object associated with the play application.
     */
    public Config getAppConfig() {
        return _appConfig;
    }
}
