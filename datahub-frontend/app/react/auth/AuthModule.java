package react.auth;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.linkedin.common.urn.CorpuserUrn;
import org.pac4j.core.client.Client;
import org.pac4j.core.client.Clients;
import org.pac4j.core.config.Config;
import org.pac4j.core.context.session.SessionStore;
import org.pac4j.core.engine.DefaultCallbackLogic;
import org.pac4j.core.http.adapter.HttpActionAdapter;
import org.pac4j.core.http.callback.PathParameterCallbackUrlResolver;
import org.pac4j.core.profile.CommonProfile;
import org.pac4j.core.profile.ProfileManager;
import org.pac4j.oidc.client.OidcClient;
import org.pac4j.oidc.config.OidcConfiguration;
import org.pac4j.play.CallbackController;
import org.pac4j.play.PlayWebContext;
import org.pac4j.play.http.PlayHttpActionAdapter;
import org.pac4j.play.store.PlayCookieSessionStore;
import org.pac4j.play.store.PlaySessionStore;
import play.Environment;
import play.mvc.Result;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static react.auth.AuthUtils.*;

/**
 * Responsible for configuring, validating, and providing authentication related components.
 */
public class AuthModule extends AbstractModule {

    private static final String AUTH_BASE_URL_CONFIG_PATH = "auth.baseUrl";
    private static final String AUTH_BASE_CALLBACK_PATH_CONFIG_PATH = "auth.baseCallbackPath";
    private static final String AUTH_SUCCESS_REDIRECT_PATH_CONFIG_PATH = "auth.successRedirectPath";

    private static final String DEFAULT_BASE_CALLBACK_PATH = "/callback";
    private static final String DEFAULT_SUCCESS_REDIRECT_PATH = "/";

    private String _authBaseUrl;
    private String _authBaseCallbackPath;
    private String _authSuccessRedirectPath;

    private final com.typesafe.config.Config _configs;

    /**
     * OIDC-specific configurations.
     */
    private final OidcConfigs _oidcConfigs;

    public AuthModule(final Environment environment, final com.typesafe.config.Config configs) {
        _configs = configs;
        _oidcConfigs = new OidcConfigs(configs);

        if (isIndirectAuthEnabled()) {
            _authBaseUrl = configs.getString(AUTH_BASE_URL_CONFIG_PATH);
            _authBaseCallbackPath = configs.hasPath(AUTH_BASE_CALLBACK_PATH_CONFIG_PATH)
                    ? configs.getString(AUTH_BASE_CALLBACK_PATH_CONFIG_PATH)
                    : DEFAULT_BASE_CALLBACK_PATH;
            _authSuccessRedirectPath = configs.hasPath(AUTH_SUCCESS_REDIRECT_PATH_CONFIG_PATH)
                    ? configs.getString(AUTH_SUCCESS_REDIRECT_PATH_CONFIG_PATH)
                    : DEFAULT_SUCCESS_REDIRECT_PATH;
        }
    }

    @Override
    protected void configure() {
        final PlayCookieSessionStore playCacheCookieStore = new PlayCookieSessionStore();
        bind(SessionStore.class).toInstance(playCacheCookieStore);
        bind(PlaySessionStore.class).toInstance(playCacheCookieStore);

        final CallbackController callbackController = new CallbackController() {};
        callbackController.setDefaultUrl(_authSuccessRedirectPath);
        callbackController.setCallbackLogic(new DefaultCallbackLogic<Result, PlayWebContext>() {
            @Override
            public Result perform(final PlayWebContext context, final Config config, final HttpActionAdapter<Result, PlayWebContext> httpActionAdapter,
                                  final String inputDefaultUrl, final Boolean inputSaveInSession, final Boolean inputMultiProfile,
                                  final Boolean inputRenewSession, final String client) {
                final Result result = super.perform(context, config, httpActionAdapter, inputDefaultUrl, inputSaveInSession, inputMultiProfile, inputRenewSession, client);
                if (_oidcConfigs.getClientName().equals(client)) {
                    return handleOidcCallback(result, context, getProfileManager(context, config));
                }
                throw new RuntimeException(String.format("Unrecognized client with name %s provided to callback URL.", client));
            }
        });
        // Make OIDC the default SSO client.
        if (_oidcConfigs.isOidcEnabled()) {
            callbackController.setDefaultClient(_oidcConfigs.getClientName());
        }
        bind(CallbackController.class).toInstance(callbackController);
    }

    @Provides @Singleton
    protected Config provideConfig() {
        if (isIndirectAuthEnabled()) {

            final Clients clients = new Clients(_authBaseUrl + _authBaseCallbackPath);
            final List<Client> clientList = new ArrayList<>();

            if (_oidcConfigs.isOidcEnabled()) {
                final OidcConfiguration oidcConfiguration = new OidcConfiguration();
                oidcConfiguration.setClientId(_oidcConfigs.getClientId());
                oidcConfiguration.setSecret(_oidcConfigs.getClientSecret());
                oidcConfiguration.setDiscoveryURI(_oidcConfigs.getDiscoveryUri());
                oidcConfiguration.setClientAuthenticationMethodAsString(_oidcConfigs.getClientAuthenticationMethod());
                oidcConfiguration.setScope(_oidcConfigs.getScope());

                final OidcClient oidcClient = new OidcClient(oidcConfiguration);
                oidcClient.setName(_oidcConfigs.getClientName());
                oidcClient.setCallbackUrlResolver(new PathParameterCallbackUrlResolver());
                clientList.add(oidcClient);
            }
            clients.setClients(clientList);

            final Config config = new Config(clients);
            config.setHttpActionAdapter(new PlayHttpActionAdapter());
            return config;
        }
        return new Config();
    }

    private Result handleOidcCallback(final Result result, final PlayWebContext context, ProfileManager<CommonProfile> profileManager) {
        if (OidcResponseErrorHandler.isError(context)) {
            return OidcResponseErrorHandler.handleError(context);
        }
        else if (profileManager.isAuthenticated() && profileManager.get(true).isPresent()) {
            final CommonProfile profile = profileManager.get(true).get();
            if (!profile.containsAttribute(_oidcConfigs.getUserNameClaim())) {
                throw new RuntimeException(
                        String.format(
                            "Failed to resolve user name claim from profile provided by Identity Provider. Missing attribute '%s'",
                            _oidcConfigs.getUserNameClaim()
                        ));
            }

            final String userNameClaim = (String) profile.getAttribute(_oidcConfigs.getUserNameClaim());
            final Pattern pattern = Pattern.compile(_oidcConfigs.getUserNameClaimRegex());
            final Matcher matcher = pattern.matcher(userNameClaim);
            if (matcher.find()) {
                final String userName = matcher.group();
                final String actorUrn = new CorpuserUrn(userName).toString();
                context.getJavaSession().put(ACTOR, actorUrn);
                return result.withCookies(createActorCookie(actorUrn, _configs.hasPath(SESSION_TTL_CONFIG_PATH)
                        ? _configs.getInt(SESSION_TTL_CONFIG_PATH)
                        : DEFAULT_SESSION_TTL_HOURS));
            } else {
                throw new RuntimeException(
                        String.format("Failed to extract DataHub username from username claim %s using regex %s",
                                userNameClaim,
                                _oidcConfigs.getUserNameClaimRegex()));
            }
        }
        throw new RuntimeException(String.format("Failed to authenticate current user. Cannot find valid identity provider profile in session"));
    }

    /**
     * Returns true if indirect authentication is enabled (callback-based SSO), false otherwise.
     * Currently, only OIDC is supported.
     */
    private boolean isIndirectAuthEnabled() {
        return _oidcConfigs.isOidcEnabled();
    }
}

