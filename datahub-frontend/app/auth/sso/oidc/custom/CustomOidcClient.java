package auth.sso.oidc.custom;

import org.pac4j.core.util.CommonHelper;
import org.pac4j.oidc.client.OidcClient;
import org.pac4j.oidc.config.OidcConfiguration;
import org.pac4j.oidc.credentials.extractor.OidcExtractor;
import org.pac4j.oidc.logout.OidcLogoutActionBuilder;
import org.pac4j.oidc.profile.OidcProfile;
import org.pac4j.oidc.profile.creator.OidcProfileCreator;
import org.pac4j.oidc.redirect.OidcRedirectActionBuilder;


public class CustomOidcClient extends OidcClient<OidcProfile, OidcConfiguration> {

  public CustomOidcClient(final OidcConfiguration configuration) {
    setConfiguration(configuration);
  }

  @Override
  protected void clientInit() {
    CommonHelper.assertNotNull("configuration", getConfiguration());
    getConfiguration().init();
    defaultRedirectActionBuilder(new OidcRedirectActionBuilder(getConfiguration(), this));
    defaultCredentialsExtractor(new OidcExtractor(getConfiguration(), this));
    defaultAuthenticator(new CustomOidcAuthenticator(getConfiguration(), this));
    defaultProfileCreator(new OidcProfileCreator<>(getConfiguration()));
    defaultLogoutActionBuilder(new OidcLogoutActionBuilder<>(getConfiguration()));
  }
}
