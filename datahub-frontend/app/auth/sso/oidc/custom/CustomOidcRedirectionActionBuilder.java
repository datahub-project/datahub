package auth.sso.oidc.custom;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.pac4j.core.context.CallContext;
import org.pac4j.core.context.WebContext;
import org.pac4j.core.exception.http.RedirectionAction;
import org.pac4j.core.util.HttpActionHelper;
import org.pac4j.oidc.client.OidcClient;
import org.pac4j.oidc.config.OidcConfiguration;
import org.pac4j.oidc.redirect.OidcRedirectionActionBuilder;
import org.pac4j.oidc.redirect.Params;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomOidcRedirectionActionBuilder extends OidcRedirectionActionBuilder {

  private static final Logger logger = LoggerFactory.getLogger(OidcRedirectionActionBuilder.class);

  private final OidcConfiguration configuration;

  public CustomOidcRedirectionActionBuilder(OidcConfiguration configuration, OidcClient client) {
    super(client);
    this.configuration = configuration;
  }

  @Override
  public Optional<RedirectionAction> getRedirectionAction(CallContext ctx) {
    WebContext context = ctx.webContext();

    Params params = this.buildParams(context);
    String computedCallbackUrl = this.client.computeFinalCallbackUrl(context);
    params.requestObject().put(OidcConfiguration.REDIRECT_URI, computedCallbackUrl);
    this.addStateAndNonceParameters(ctx, params);
    if (this.configuration.getMaxAge() != null) {
      params
          .requestObject()
          .put(OidcConfiguration.MAX_AGE, this.configuration.getMaxAge().toString());
    }

    Map<String, String> authParams = new HashMap<>();
    authParams.putAll(params.url());
    authParams.putAll(params.requestObject());
    String location = this.buildAuthenticationRequestUrl(authParams);

    logger.debug("Custom parameters: {}", this.configuration.getCustomParams());

    String acrValues = this.configuration.getCustomParam("acr_values");

    if (acrValues != null && !location.contains("acr_values=")) {
      location += (location.contains("?") ? "&" : "?") + "acr_values=" + acrValues;
    }

    logger.debug("Authentication request url: {}", location);
    return Optional.of(HttpActionHelper.buildRedirectUrlAction(context, location));
  }
}
