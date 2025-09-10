package auth.sso.oidc.custom;

import auth.sso.oidc.OidcConfigs;
import org.pac4j.core.util.CommonHelper;
import org.pac4j.oidc.client.OidcClient;
import org.pac4j.oidc.config.OidcConfiguration;
import org.pac4j.oidc.credentials.extractor.OidcCredentialsExtractor;
import org.pac4j.oidc.logout.OidcLogoutActionBuilder;
import org.pac4j.oidc.logout.processor.OidcLogoutProcessor;

public class CustomOidcClient extends OidcClient {

  private final OidcConfigs oidcConfigs;

  public CustomOidcClient(OidcConfiguration configuration, OidcConfigs oidcConfigs) {
    super(configuration);
    this.oidcConfigs = oidcConfigs;
  }

  @Override
  protected void internalInit(final boolean forceReinit) {
    // Validate configuration
    CommonHelper.assertNotNull("configuration", getConfiguration());

    // Initialize configuration
    getConfiguration().init(forceReinit);

    // Initialize client components
    setRedirectionActionBuilderIfUndefined(
        new CustomOidcRedirectionActionBuilder(getConfiguration(), this));
    setCredentialsExtractorIfUndefined(new OidcCredentialsExtractor(getConfiguration(), this));

    // Initialize default authenticator if not set
    if (getAuthenticator() == null || forceReinit) {
      if (forceReinit) {
        setAuthenticator(new CustomOidcAuthenticator(this, oidcConfigs));
      } else {
        setAuthenticatorIfUndefined(new CustomOidcAuthenticator(this, oidcConfigs));
      }
    }

    setProfileCreatorIfUndefined(
        new CustomOidcProfileCreator(getConfiguration(), this, oidcConfigs));
    setLogoutProcessorIfUndefined(
        new OidcLogoutProcessor(getConfiguration(), findSessionLogoutHandler()));
    setLogoutActionBuilderIfUndefined(new OidcLogoutActionBuilder(getConfiguration()));
  }
}
