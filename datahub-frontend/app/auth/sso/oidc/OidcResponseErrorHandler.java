/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package auth.sso.oidc;

import static play.mvc.Results.internalServerError;
import static play.mvc.Results.unauthorized;

import java.util.Optional;
import org.pac4j.core.context.CallContext;
import org.pac4j.core.context.WebContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import play.mvc.Result;

public class OidcResponseErrorHandler {

  private OidcResponseErrorHandler() {}

  private static final Logger logger = LoggerFactory.getLogger("OidcResponseErrorHandler");

  private static final String ERROR_FIELD_NAME = "error";
  private static final String ERROR_DESCRIPTION_FIELD_NAME = "error_description";

  public static Result handleError(final CallContext ctx) {
    WebContext context = ctx.webContext();
    logger.warn(
        "OIDC responded with an error: '{}'. Error description: '{}'",
        getError(context),
        getErrorDescription(context));

    if (getError(context).isPresent() && getError(context).get().equals("access_denied")) {
      return unauthorized(
          String.format(
              "Access denied. "
                  + "The OIDC service responded with 'Access denied'. "
                  + "It seems that you don't have access to this application yet. Please apply for access. \n\n"
                  + "If you already have been assigned this application, it may be so that your OIDC request is still in action. "
                  + "Error details: '%s':'%s'",
              context.getRequestParameter("error"),
              context.getRequestParameter("error_description")));
    }

    return internalServerError(
        String.format(
            "Internal server error. The OIDC service responded with an error: '%s'.\n"
                + "Error description: '%s'",
            getError(context).orElse(""), getErrorDescription(context).orElse("")));
  }

  public static boolean isError(final CallContext ctx) {
    return getError(ctx.webContext()).isPresent() && !getError(ctx.webContext()).get().isEmpty();
  }

  public static Optional<String> getError(final WebContext context) {
    return context.getRequestParameter(ERROR_FIELD_NAME);
  }

  public static Optional<String> getErrorDescription(final WebContext context) {
    return context.getRequestParameter(ERROR_DESCRIPTION_FIELD_NAME);
  }
}
