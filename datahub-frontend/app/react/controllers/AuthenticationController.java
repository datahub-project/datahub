package react.controllers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.CorpuserUrn;
import com.typesafe.config.Config;
import org.apache.commons.lang3.StringUtils;
import org.pac4j.core.client.Client;
import org.pac4j.core.context.session.SessionStore;
import org.pac4j.core.exception.HttpAction;
import org.pac4j.play.PlayWebContext;
import org.pac4j.play.http.PlayHttpActionAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Http;
import play.mvc.Result;
import react.auth.AuthUtils;
import react.auth.JAASConfigs;
import react.auth.OidcConfigs;
import security.AuthenticationManager;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.naming.NamingException;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import static react.auth.AuthUtils.*;

public class AuthenticationController extends Controller {

    private final Logger _logger = LoggerFactory.getLogger(AuthenticationController.class.getName());
    private final Config _configs;
    private final OidcConfigs _oidcConfigs;
    private final JAASConfigs _jaasConfigs;

    @Inject
    private org.pac4j.core.config.Config _ssoConfig;

    @Inject
    private SessionStore _playSessionStore;

    @Inject
    public AuthenticationController(@Nonnull Config configs) {
        _configs = configs;
        _oidcConfigs = new OidcConfigs(configs);
        _jaasConfigs = new JAASConfigs(configs);
    }

    /**
     * Route used to perform authentication, or redirect to log in if authentication fails.
     *
     * If indirect SSO (eg. oidc) is configured, this route will redirect to the identity provider (Indirect auth).
     * If not, we will fallback to the default username / password login experience (Direct auth).
     */
    @Nonnull
    public Result authenticate() {
        if (AuthUtils.isAuthenticated(ctx())) {
            return redirect("/");
        }

        // 1. If indirect auth is enabled, redirect to IdP
        if (_oidcConfigs.isOidcEnabled()) {
            final PlayWebContext playWebContext = new PlayWebContext(ctx(), _playSessionStore);
            final Client client = _ssoConfig.getClients().findClient(_oidcConfigs.getClientName());
            final HttpAction action = client.redirect(playWebContext);
            return new PlayHttpActionAdapter().adapt(action.getCode(), playWebContext);
        }

        // 2. If JAAS auth is enabled, fallback to it
        if (_jaasConfigs.isJAASEnabled()) {
            return redirect(LOGIN_ROUTE);
        }

        // 3. If no auth enabled, fallback to using default user account & redirect.
        session().put(ACTOR, DEFAULT_ACTOR_URN.toString());
        return redirect("/").withCookies(createActorCookie(DEFAULT_ACTOR_URN.toString(), _configs.hasPath(SESSION_TTL_CONFIG_PATH)
                ? _configs.getInt(SESSION_TTL_CONFIG_PATH)
                : DEFAULT_SESSION_TTL_HOURS));
    }

    /**
     * Log in a user based on a username + password.
     *
     * TODO: Implement built-in support for LDAP auth. Currently dummy jaas authentication is the default.
     */
    @Nonnull
    public Result logIn() {
        if (!_jaasConfigs.isJAASEnabled()) {
            final ObjectNode error = Json.newObject();
            error.put("message", "JAAS authentication is not enabled on the server.");
            return badRequest(error);
        }

        final JsonNode json = request().body().asJson();
        final String username = json.findPath(USER_NAME).textValue();
        final String password = json.findPath(PASSWORD).textValue();

        if (StringUtils.isBlank(username)) {
            JsonNode invalidCredsJson = Json.newObject()
                .put("message", "User name must not be empty.");
            return badRequest(invalidCredsJson);
        }

        ctx().session().clear();

        try {
            AuthenticationManager.authenticateUser(username, password);
        } catch (NamingException e) {
            _logger.error("Authentication error", e);
            JsonNode invalidCredsJson = Json.newObject()
                .put("message", "Invalid Credentials");
            return badRequest(invalidCredsJson);
        }

        final String actorUrn = new CorpuserUrn(username).toString();
        ctx().session().put(ACTOR, actorUrn);

        return ok().withCookies(Http.Cookie.builder(ACTOR, actorUrn)
            .withHttpOnly(false)
            .withMaxAge(Duration.of(30, ChronoUnit.DAYS))
            .build());
    }
}