package react.auth;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.pac4j.core.client.Client;
import org.pac4j.core.client.Clients;
import org.pac4j.core.config.Config;
import org.pac4j.core.context.session.SessionStore;
import org.pac4j.play.CallbackController;
import org.pac4j.play.http.PlayHttpActionAdapter;
import org.pac4j.play.store.PlayCookieSessionStore;
import org.pac4j.play.store.PlaySessionStore;
import play.Environment;

import java.util.ArrayList;
import java.util.List;

import static react.auth.OidcConfigs.*;


/**
 * Responsible for configuring, validating, and providing authentication related components.
 */
public class AuthModule extends AbstractModule {

    private final SsoProvider _ssoProvider;
    private final Config _pac4jConfig;

    public AuthModule(final Environment environment, final com.typesafe.config.Config configs) {
        _ssoProvider = initSsoClient(configs);
        _pac4jConfig = buildPac4jConfig(configs);
    }

    private SsoProvider initSsoClient(com.typesafe.config.Config configs) {
        if (isSsoEnabled(configs)) {
            SsoConfigs ssoConfigs = new SsoConfigs(configs);
            if (ssoConfigs.isOidcEnabled()) {
                // Register OIDC Manager, add to list of managers.
                OidcConfigs oidcConfigs = new OidcConfigs(configs);
                return new OidcProvider(oidcConfigs);
            }
        }
        return null;
    }

    private Config buildPac4jConfig(com.typesafe.config.Config configs) {
        if (isSsoEnabled(configs)) {
            SsoConfigs ssoConfigs = new SsoConfigs(configs);
            final Clients clients = new Clients(ssoConfigs.getAuthBaseUrl() + ssoConfigs.getAuthBaseCallbackPath());
            final List<Client> clientList = new ArrayList<>();
            clientList.add(_ssoProvider.getClient());
            clients.setClients(clientList);
            final Config config = new Config(clients);
            config.setHttpActionAdapter(new PlayHttpActionAdapter());
            return config;
        }
        return new Config();
    }

    @Override
    protected void configure() {
        final PlayCookieSessionStore playCacheCookieStore = new PlayCookieSessionStore();
        bind(SessionStore.class).toInstance(playCacheCookieStore);
        bind(PlaySessionStore.class).toInstance(playCacheCookieStore);

        final CallbackController callbackController = new CallbackController() {};
        callbackController.setDefaultUrl("/"); // Redirect to HomePage on Authentication.
        callbackController.setCallbackLogic(new SsoCallbackHandler(_ssoProvider));
        bind(CallbackController.class).toInstance(callbackController);
    }

    @Provides @Singleton
    protected Config provideConfig() {
        return _pac4jConfig;
    }

    protected boolean isSsoEnabled(com.typesafe.config.Config configs) {
        // If OIDC is enabled, we infer SSO to be enabled.
        return configs.hasPath(OIDC_ENABLED_CONFIG_PATH)
            && Boolean.TRUE.equals(
            Boolean.parseBoolean(configs.getString(OIDC_ENABLED_CONFIG_PATH)));
    }
}

