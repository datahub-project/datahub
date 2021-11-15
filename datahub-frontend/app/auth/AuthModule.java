package auth;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.RestliEntityClient;
import com.linkedin.metadata.restli.DefaultRestliClientFactory;
import com.linkedin.util.Configuration;
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

import static auth.AuthUtils.*;
import static auth.sso.oidc.OidcConfigs.*;

/**
 * Responsible for configuring, validating, and providing authentication related components.
 */
public class AuthModule extends AbstractModule {

    /**
     * The following environment variables are expected to be provided.
     * They are used in establishing the connection to the downstream GMS.
     * Currently, only 1 downstream GMS is supported.
     */
    private static final String GMS_HOST_ENV_VAR = "DATAHUB_GMS_HOST";
    private static final String GMS_PORT_ENV_VAR = "DATAHUB_GMS_PORT";
    private static final String GMS_USE_SSL_ENV_VAR = "DATAHUB_GMS_USE_SSL";
    private static final String GMS_SSL_PROTOCOL_VAR = "DATAHUB_GMS_SSL_PROTOCOL";

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
                SsoManager.class, EntityClient.class, AuthClient.class));
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

    @Provides @Singleton
    protected EntityClient provideEntityClient() {

        String systemClientId = "";
        String systemSecret = "";

        if (isMetadataServiceAuthEnabled(_configs)) {
            systemClientId = _configs.getString(SYSTEM_CLIENT_ID_CONFIG_PATH);
            systemSecret = _configs.getString(SYSTEM_CLIENT_SECRET_CONFIG_PATH);
        }

        return new RestliEntityClient(buildRestliClient(), systemClientId, systemSecret);
    }

    @Provides @Singleton
    protected AuthClient provideAuthClient() {
        // Init a GMS auth client
        final String metadataServiceHost = _configs.hasPath("metadataService.host")
            ? _configs.getString("metadataService.host")
            : Configuration.getEnvironmentVariable(GMS_HOST_ENV_VAR, "localhost");

        final int metadataServicePort = _configs.hasPath("metadataService.host")
            ? _configs.getInt("metadataService.host")
            : Integer.parseInt(Configuration.getEnvironmentVariable(GMS_PORT_ENV_VAR, "8080"));

        final Boolean metadataServiceUseSsl = _configs.hasPath("metadataService.useSsl")
            ? _configs.getBoolean("metadataService.useSsl")
            : Boolean.parseBoolean(Configuration.getEnvironmentVariable(GMS_USE_SSL_ENV_VAR, "False"));

        String systemClientId = "";
        String systemSecret = "";

        if (isMetadataServiceAuthEnabled(_configs)) {
            systemClientId = _configs.getString(SYSTEM_CLIENT_ID_CONFIG_PATH);
            systemSecret = _configs.getString(SYSTEM_CLIENT_SECRET_CONFIG_PATH);
        }
        return new AuthClient(metadataServiceHost, metadataServicePort, metadataServiceUseSsl, systemClientId, systemSecret);
    }

    private com.linkedin.restli.client.Client buildRestliClient() {
        final String metadataServiceHost = _configs.hasPath("metadataService.host")
            ? _configs.getString("metadataService.host")
            : Configuration.getEnvironmentVariable(GMS_HOST_ENV_VAR, "localhost");

        final int metadataServicePort = _configs.hasPath("metadataService.host")
            ? _configs.getInt("metadataService.host")
            : Integer.parseInt(Configuration.getEnvironmentVariable(GMS_PORT_ENV_VAR, "8080"));

        final Boolean metadataServiceUseSsl = _configs.hasPath("metadataService.useSsl")
            ? _configs.getBoolean("metadataService.useSsl")
            : Boolean.parseBoolean(Configuration.getEnvironmentVariable(GMS_USE_SSL_ENV_VAR, "False"));

        final String metadataServiceSslProtocol = _configs.hasPath("metadataService.sslProtocol")
            ? _configs.getString("metadataService.sslProtocol")
            : Configuration.getEnvironmentVariable(GMS_SSL_PROTOCOL_VAR);

        return DefaultRestliClientFactory.getRestLiClient(metadataServiceHost, metadataServicePort, metadataServiceUseSsl, metadataServiceSslProtocol);
    }

    protected boolean isSsoEnabled(com.typesafe.config.Config configs) {
        // If OIDC is enabled, we infer SSO to be enabled.
        return configs.hasPath(OIDC_ENABLED_CONFIG_PATH)
            && Boolean.TRUE.equals(
            Boolean.parseBoolean(configs.getString(OIDC_ENABLED_CONFIG_PATH)));
    }

    protected boolean isMetadataServiceAuthEnabled(com.typesafe.config.Config configs) {
        // If OIDC is enabled, we infer SSO to be enabled.
        return configs.hasPath(METADATA_SERVICE_AUTH_ENABLED_CONFIG_PATH)
            && Boolean.TRUE.equals(
            Boolean.parseBoolean(configs.getString(METADATA_SERVICE_AUTH_ENABLED_CONFIG_PATH)));
    }
}

