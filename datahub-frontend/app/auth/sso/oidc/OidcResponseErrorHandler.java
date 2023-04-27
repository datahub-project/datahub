package auth.sso.oidc;

import org.pac4j.play.PlayWebContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import play.mvc.Result;

import java.util.Optional;

import static play.mvc.Results.internalServerError;
import static play.mvc.Results.unauthorized;


public class OidcResponseErrorHandler {

    private OidcResponseErrorHandler() {

    }

    private static final Logger _logger = LoggerFactory.getLogger("OidcResponseErrorHandler");

    private static final String ERROR_FIELD_NAME = "error";
    private static final String ERROR_DESCRIPTION_FIELD_NAME = "error_description";

    public static Result handleError(final PlayWebContext context) {

        _logger.warn("OIDC responded with an error: '{}'. Error description: '{}'",
                getError(context),
                getErrorDescription(context));

        if (getError(context).isPresent() && getError(context).get().equals("access_denied")) {
            return unauthorized(String.format("Access denied. "
                    + "The OIDC service responded with 'Access denied'. "
                    + "It seems that you don't have access to this application yet. Please apply for access. \n\n"
                    + "If you already have been assigned this application, it may be so that your OIDC request is still in action. "
                    + "Error details: '%s':'%s'",
                    context.getRequestParameter("error"),
                    context.getRequestParameter("error_description")));
        }

        return internalServerError(
                String.format("Internal server error. The OIDC service responded with an error: '%s'.\n"
                        + "Error description: '%s'", getError(context).orElse(""), getErrorDescription(context).orElse("")));
    }

    public static boolean isError(final PlayWebContext context) {
        return getError(context).isPresent() && !getError(context).get().isEmpty();
    }

    public static Optional<String> getError(final PlayWebContext context) {
        return context.getRequestParameter(ERROR_FIELD_NAME);
    }

    public static Optional<String> getErrorDescription(final PlayWebContext context) {
        return context.getRequestParameter(ERROR_DESCRIPTION_FIELD_NAME);
    }
}
