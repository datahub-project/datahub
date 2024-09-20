package auth.sso.oidc.custom;

import java.util.Map;
import java.util.Optional;
import org.pac4j.core.context.WebContext;
import org.pac4j.core.exception.http.RedirectionAction;
import org.pac4j.core.exception.http.RedirectionActionHelper;
import org.pac4j.oidc.client.OidcClient;
import org.pac4j.oidc.config.OidcConfiguration;
import org.pac4j.oidc.redirect.OidcRedirectionActionBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CustomOidcRedirectionActionBuilder extends OidcRedirectionActionBuilder {

  private static final Logger logger = LoggerFactory.getLogger(OidcRedirectionActionBuilder.class);
  public CustomOidcRedirectionActionBuilder(OidcConfiguration configuration, OidcClient client) {
    super(configuration, client);
  }

  @Override
  public Optional<RedirectionAction> getRedirectionAction(WebContext context) {
    Map<String, String> params = this.buildParams();
    String computedCallbackUrl = this.client.computeFinalCallbackUrl(context);
    params.put("redirect_uri", computedCallbackUrl);
    this.addStateAndNonceParameters(context, params);
    if (this.configuration.getMaxAge() != null) {
      params.put("max_age", this.configuration.getMaxAge().toString());
    }

    String location = this.buildAuthenticationRequestUrl(params);

    logger.debug("Custom parameters: {}", this.configuration.getCustomParams());

    String acrValues = this.configuration.getCustomParam("acr_values");

    if (acrValues != null && !location.contains("acr_values=")) {
      location += (location.contains("?") ? "&" : "?") + "acr_values=" + acrValues;
    }

    logger.debug("Authentication request url: {}", location);
    return Optional.of(RedirectionActionHelper.buildRedirectUrlAction(context, location));
  }

}
