package auth;

import auth.sso.SsoConfigs;
import auth.sso.SsoManager;
import auth.sso.oidc.OidcConfigs;
import auth.sso.oidc.OidcProvider;
import client.AuthServiceClient;
import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.RestliEntityClient;
import com.linkedin.metadata.restli.DefaultRestliClientFactory;
import com.linkedin.util.Configuration;
import controllers.SsoCallbackController;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.pac4j.core.client.Client;
import org.pac4j.core.client.Clients;
import org.pac4j.core.config.Config;
import org.pac4j.core.context.session.SessionStore;
import org.pac4j.play.LogoutController;
import org.pac4j.play.http.PlayHttpActionAdapter;
import org.pac4j.play.store.PlayCacheSessionStore;
import org.pac4j.play.store.PlayCookieSessionStore;
import org.pac4j.play.store.PlaySessionStore;
import org.pac4j.play.store.ShiroAesDataEncrypter;
import play.Environment;
import play.cache.SyncCacheApi;
import utils.ConfigUtil;

import static auth.AuthUtils.*;
import static auth.sso.oidc.OidcConfigs.*;
import static utils.ConfigUtil.*;


/**
 * Responsible for configuring, validating, and providing authentication related components.
 */
public class AuthModule extends AbstractModule {

    /**
     * Pac4j Stores Session State in a browser-side cookie in encrypted fashion. This configuration
     * value provides a stable encryption base from which to derive the encryption key.
     *
     * We hash this value (SHA1), then take the first 16 bytes as the AES key.
     */
    private static final String PAC4J_AES_KEY_BASE_CONF = "play.http.secret.key";
    private static final String PAC4J_SESSIONSTORE_PROVIDER_CONF = "pac4j.sessionStore.provider";

    private final com.typesafe.config.Config _configs;

    public AuthModule(final Environment environment, final com.typesafe.config.Config configs) {
        _configs = configs;
    }

    @Override
    protected void configure() {
        /**
         * In Pac4J, you are given the option to store the profiles of authenticated users in either
         * (i) PlayCacheSessionStore - saves your data in the Play cache or
         * (ii) PlayCookieSessionStore saves your data in the Play session cookie
         * However there is problem (https://github.com/datahub-project/datahub/issues/4448) observed when storing the Pac4j profile in cookie.
         * Whenever the profile returned by Pac4j is greater than 4096 characters, the response will be rejected by the browser.
         * Default to PlayCacheCookieStore so that datahub-frontend container remains as a stateless service
         */
        String sessionStoreProvider = _configs.getString(PAC4J_SESSIONSTORE_PROVIDER_CONF);

        if (sessionStoreProvider.equals("PlayCacheSessionStore")) {
            final PlayCacheSessionStore playCacheSessionStore = new PlayCacheSessionStore(getProvider(SyncCacheApi.class));
            bind(SessionStore.class).toInstance(playCacheSessionStore);
            bind(PlaySessionStore.class).toInstance(playCacheSessionStore);
        } else {
            PlayCookieSessionStore playCacheCookieStore;
            try {
                // To generate a valid encryption key from an input value, we first
                // hash the input to generate a fixed-length string. Then, we convert
                // it to hex and slice the first 16 bytes, because AES key length must strictly
                // have a specific length.
                final String aesKeyBase = _configs.getString(PAC4J_AES_KEY_BASE_CONF);
                final String aesKeyHash = DigestUtils.sha1Hex(aesKeyBase.getBytes(StandardCharsets.UTF_8));
                final String aesEncryptionKey = aesKeyHash.substring(0, 16);
                playCacheCookieStore = new PlayCookieSessionStore(
                        new ShiroAesDataEncrypter(aesEncryptionKey.getBytes()));
            } catch (Exception e) {
                throw new RuntimeException("Failed to instantiate Pac4j cookie session store!", e);
            }
            bind(SessionStore.class).toInstance(playCacheCookieStore);
            bind(PlaySessionStore.class).toInstance(playCacheCookieStore);
        }

        try {
            bind(SsoCallbackController.class).toConstructor(SsoCallbackController.class.getConstructor(
                SsoManager.class,
                Authentication.class,
                EntityClient.class,
                AuthServiceClient.class));
        } catch (NoSuchMethodException | SecurityException e) {
            throw new RuntimeException("Failed to bind to SsoCallbackController. Cannot find constructor, e");
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

    @Provides
    @Singleton
    protected Authentication provideSystemAuthentication() {
        // Returns an instance of Authentication used to authenticate system initiated calls to Metadata Service.
        String systemClientId = _configs.getString(SYSTEM_CLIENT_ID_CONFIG_PATH);
        String systemSecret = _configs.getString(SYSTEM_CLIENT_SECRET_CONFIG_PATH);
        final Actor systemActor =
            new Actor(ActorType.USER, systemClientId); // TODO: Change to service actor once supported.
        return new Authentication(systemActor, String.format("Basic %s:%s", systemClientId, systemSecret),
            Collections.emptyMap());
    }

    @Provides
    @Singleton
    protected EntityClient provideEntityClient() {
        return new RestliEntityClient(buildRestliClient());
    }

    @Provides
    @Singleton
    protected CloseableHttpClient provideHttpClient() {
        return HttpClients.createDefault();
    }

    @Provides
    @Singleton
    protected AuthServiceClient provideAuthClient(Authentication systemAuthentication, CloseableHttpClient httpClient) {
        // Init a GMS auth client
        final String metadataServiceHost =
            _configs.hasPath(METADATA_SERVICE_HOST_CONFIG_PATH) ? _configs.getString(METADATA_SERVICE_HOST_CONFIG_PATH)
                : Configuration.getEnvironmentVariable(GMS_HOST_ENV_VAR, DEFAULT_GMS_HOST);

        final int metadataServicePort =
            _configs.hasPath(METADATA_SERVICE_PORT_CONFIG_PATH) ? _configs.getInt(METADATA_SERVICE_PORT_CONFIG_PATH)
                : Integer.parseInt(Configuration.getEnvironmentVariable(GMS_PORT_ENV_VAR, DEFAULT_GMS_PORT));

        final Boolean metadataServiceUseSsl =
            _configs.hasPath(METADATA_SERVICE_USE_SSL_CONFIG_PATH) ? _configs.getBoolean(
                METADATA_SERVICE_USE_SSL_CONFIG_PATH)
                : Boolean.parseBoolean(Configuration.getEnvironmentVariable(GMS_USE_SSL_ENV_VAR, DEFAULT_GMS_USE_SSL));

        return new AuthServiceClient(metadataServiceHost, metadataServicePort, metadataServiceUseSsl,
            systemAuthentication, httpClient);
    }

    private com.linkedin.restli.client.Client buildRestliClient() {
        final String metadataServiceHost = utils.ConfigUtil.getString(
            _configs,
            METADATA_SERVICE_HOST_CONFIG_PATH,
            utils.ConfigUtil.DEFAULT_METADATA_SERVICE_HOST);
        final int metadataServicePort = utils.ConfigUtil.getInt(
            _configs,
            utils.ConfigUtil.METADATA_SERVICE_PORT_CONFIG_PATH,
            utils.ConfigUtil.DEFAULT_METADATA_SERVICE_PORT);
        final boolean metadataServiceUseSsl = utils.ConfigUtil.getBoolean(
            _configs,
            utils.ConfigUtil.METADATA_SERVICE_USE_SSL_CONFIG_PATH,
            ConfigUtil.DEFAULT_METADATA_SERVICE_USE_SSL
        );
        final String metadataServiceSslProtocol = utils.ConfigUtil.getString(
            _configs,
            utils.ConfigUtil.METADATA_SERVICE_SSL_PROTOCOL_CONFIG_PATH,
            ConfigUtil.DEFAULT_METADATA_SERVICE_SSL_PROTOCOL
        );
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

