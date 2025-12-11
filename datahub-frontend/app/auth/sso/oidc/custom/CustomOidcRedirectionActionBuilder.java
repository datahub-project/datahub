/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package auth.sso.oidc.custom;

import java.util.Map;
import java.util.Optional;
import org.pac4j.core.context.CallContext;
import org.pac4j.core.context.WebContext;
import org.pac4j.core.exception.http.RedirectionAction;
import org.pac4j.core.util.HttpActionHelper;
import org.pac4j.oidc.client.OidcClient;
import org.pac4j.oidc.config.OidcConfiguration;
import org.pac4j.oidc.redirect.OidcRedirectionActionBuilder;
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

    Map<String, String> params = this.buildParams(context);
    String computedCallbackUrl = this.client.computeFinalCallbackUrl(context);
    params.put("redirect_uri", computedCallbackUrl);
    this.addStateAndNonceParameters(ctx, params);
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
    return Optional.of(HttpActionHelper.buildRedirectUrlAction(context, location));
  }
}
