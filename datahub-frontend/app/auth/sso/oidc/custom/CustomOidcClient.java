package auth.sso.oidc.custom;

import org.pac4j.core.util.CommonHelper;
import org.pac4j.oidc.client.OidcClient;
import org.pac4j.oidc.config.OidcConfiguration;
import org.pac4j.oidc.credentials.extractor.OidcCredentialsExtractor;
import org.pac4j.oidc.logout.OidcLogoutActionBuilder;
import org.pac4j.oidc.logout.processor.OidcLogoutProcessor;
import org.pac4j.oidc.profile.creator.OidcProfileCreator;

public class CustomOidcClient extends OidcClient {

  public CustomOidcClient(OidcConfiguration configuration) {
    super(configuration);
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
      setAuthenticatorIfUndefined(new CustomOidcAuthenticator(this));
    }

    setProfileCreatorIfUndefined(new OidcProfileCreator(getConfiguration(), this));
    setLogoutProcessorIfUndefined(
        new OidcLogoutProcessor(getConfiguration(), findSessionLogoutHandler()));
    setLogoutActionBuilderIfUndefined(new OidcLogoutActionBuilder(getConfiguration()));
  }
}
