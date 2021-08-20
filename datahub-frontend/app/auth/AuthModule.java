package auth;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.pac4j.core.client.Client;
import org.pac4j.core.client.Clients;
import org.pac4j.core.config.Config;
import org.pac4j.core.context.session.SessionStore;
import org.pac4j.play.LogoutController;
import org.pac4j.play.http.PlayHttpActionAdapter;
import org.pac4j.play.store.PlayCookieSessionStore;
import org.pac4j.play.store.PlaySessionStore;
import play.Environment;

import java.util.ArrayList;
import java.util.List;
import auth.sso.oidc.OidcProvider;
import auth.sso.oidc.OidcConfigs;
import auth.sso.SsoConfigs;
import auth.sso.SsoManager;
import controllers.SsoCallbackController;

import static auth.sso.oidc.OidcConfigs.*;

/**
 * Responsible for configuring, validating, and providing authentication related components.
 */
public class AuthModule extends AbstractModule {

    private final com.typesafe.config.Config _configs;

    public AuthModule(final Environment environment, final com.typesafe.config.Config configs) {
        _configs = configs;
    }

    @Override
    protected void configure() {
        final PlayCookieSessionStore playCacheCookieStore = new PlayCookieSessionStore();
        bind(SessionStore.class).toInstance(playCacheCookieStore);
        bind(PlaySessionStore.class).toInstance(playCacheCookieStore);

        try {
            bind(SsoCallbackController.class).toConstructor(SsoCallbackController.class.getConstructor(
                SsoManager.class));
        } catch (NoSuchMethodException | SecurityException e) {
            System.out.println("Required constructor missing");
        }
        // logout
        final LogoutController logoutController = new LogoutController();
        logoutController.setDefaultUrl("/");
        bind(LogoutController.class).toInstance(logoutController);
    }

    @Provides @Singleton
    protected Config provideConfig(SsoManager ssoManager) {
        if (ssoManager.isSsoEnabled()) {
            final Clients clients = new Clients();
            final List<Client> clientList = new ArrayList<>();
            clientList.add(ssoManager.getSsoProvider().client());
            clients.setClients(clientList);
            final Config config = new Config(clients);
            config.setHttpActionAdapter(new PlayHttpActionAdapter());
            return config;
        }
        return new Config();
    }

    @Provides @Singleton
    protected SsoManager provideSsoManager() {
        SsoManager manager = new SsoManager();
        // Seed the SSO manager with a default SSO provider.
        if (isSsoEnabled(_configs)) {
            SsoConfigs ssoConfigs = new SsoConfigs(_configs);
            if (ssoConfigs.isOidcEnabled()) {
                // Register OIDC Provider, add to list of managers.
                OidcConfigs oidcConfigs = new OidcConfigs(_configs);
                OidcProvider oidcProvider = new OidcProvider(oidcConfigs);
                // Set the default SSO provider to this OIDC client.
                manager.setSsoProvider(oidcProvider);
            }
        }
        return manager;
    }

    protected boolean isSsoEnabled(com.typesafe.config.Config configs) {
        // If OIDC is enabled, we infer SSO to be enabled.
        return configs.hasPath(OIDC_ENABLED_CONFIG_PATH)
            && Boolean.TRUE.equals(
            Boolean.parseBoolean(configs.getString(OIDC_ENABLED_CONFIG_PATH)));
    }
}

