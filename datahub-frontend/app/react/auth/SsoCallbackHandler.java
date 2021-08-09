package react.auth;

import org.pac4j.core.config.Config;
import org.pac4j.core.engine.DefaultCallbackLogic;
import org.pac4j.core.http.adapter.HttpActionAdapter;
import org.pac4j.play.PlayWebContext;
import play.mvc.Result;


/**
 * An extension of the Pac4j default Callback Logic invoked by the
 * CallbackController when the Identity Provider redirects back to DataHub
 * post-authentication.
 *
 * This implementation leverages the {@link SsoManager} to delegate handling of a
 * callback to an {@link SsoProvider}.
 */
public class SsoCallbackHandler extends DefaultCallbackLogic<Result, PlayWebContext> {

    private final SsoManager _ssoManager;

    public SsoCallbackHandler(final SsoManager ssoManager) {
      _ssoManager = ssoManager;
    }

    @Override
    public Result perform(
        final PlayWebContext context,
        final Config config,
        final HttpActionAdapter<Result, PlayWebContext> httpActionAdapter,
        final String inputDefaultUrl,
        final Boolean inputSaveInSession,
        final Boolean inputMultiProfile,
        final Boolean inputRenewSession,
        final String client) {
      if (_ssoManager.isSsoEnabled()) {
        return _ssoManager.getSsoProvider().getCallbackLogic().perform(
            context, config, httpActionAdapter, inputDefaultUrl, inputSaveInSession, inputMultiProfile, inputRenewSession, client
        );
      }
      throw new RuntimeException("Failed to perform Callback Logic: SSO is not enabled.");
    }
}
