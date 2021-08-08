package react.auth;

import org.pac4j.core.config.Config;
import org.pac4j.core.engine.DefaultCallbackLogic;
import org.pac4j.core.http.adapter.HttpActionAdapter;
import org.pac4j.play.PlayWebContext;
import play.mvc.Result;


public class SsoCallbackHandler extends DefaultCallbackLogic<Result, PlayWebContext> {

    private final SsoClient _ssoClient;

    public SsoCallbackHandler(final SsoClient ssoClient) {
      _ssoClient = ssoClient;
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
      final Result result = super.perform(context, config, httpActionAdapter, inputDefaultUrl, inputSaveInSession, inputMultiProfile, inputRenewSession, client);
        // NPE WARNING
        if (client.equals(_ssoClient.getClient().getName())) {
          return _ssoClient.handleCallback(result, context, getProfileManager(context, config));
        }
      // Should never occur.
        throw new RuntimeException(String.format("Unrecognized client with name %s provided to callback URL.", client));
    }
}
