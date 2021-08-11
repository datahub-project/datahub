package react.controllers;

import com.linkedin.common.urn.CorpuserUrn;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.inject.Inject;
import org.pac4j.core.config.Config;
import org.pac4j.core.engine.DefaultCallbackLogic;
import org.pac4j.core.http.adapter.HttpActionAdapter;
import org.pac4j.core.profile.CommonProfile;
import org.pac4j.core.profile.ProfileManager;
import org.pac4j.play.CallbackController;
import org.pac4j.play.PlayWebContext;
import play.mvc.Controller;
import play.mvc.Result;
import react.auth.OidcConfigs;
import react.auth.OidcClientProvider;
import react.auth.SsoManager;

import static react.auth.AuthUtils.*;


public class OidcCallbackController extends Controller {

  private final CallbackController _delegate; // Delegates to the default Pac4j Callback Controller.

  @Inject
  private SsoManager _ssoManager;

  public OidcCallbackController() {
    _delegate = new CallbackController();
    _delegate.setDefaultUrl("/"); // By default, redirects to Home Page on log in.
    _delegate.setCallbackLogic(new DefaultCallbackLogic<Result, PlayWebContext>() {
      @Override
      public Result perform(final PlayWebContext context, final Config config, final HttpActionAdapter<Result, PlayWebContext> httpActionAdapter,
          final String inputDefaultUrl, final Boolean inputSaveInSession, final Boolean inputMultiProfile,
          final Boolean inputRenewSession, final String client) {
        final Result result = super.perform(context, config, httpActionAdapter, inputDefaultUrl, inputSaveInSession, inputMultiProfile, inputRenewSession, client);
        final OidcConfigs oidcConfigs = (OidcConfigs) _ssoManager.getSsoProvider().configs();
        return handleOidcCallback(oidcConfigs, result, context, getProfileManager(context, config));
      }
    });
  }

  public CompletionStage<Result> callback() {
    if (isOidcEnabled()) {
      // Ideally, we drop this. _delegate.setDefaultClient(_ssoManager.getSsoProvider().getClient().getName());
      return _delegate.callback();
    }
    throw new RuntimeException("Failed to perform OIDC callback: OIDC SSO is not configured.");
  }

  private Result handleOidcCallback(final OidcConfigs oidcConfigs, final Result result, final PlayWebContext context, ProfileManager<CommonProfile> profileManager) {
    if (profileManager.isAuthenticated()) {
      final CommonProfile profile = profileManager.get(true).get();

      // Extract the User name required to log into DataHub.
      final String userName = extractUserNameOrThrow(oidcConfigs, profile);
      final String corpUserUrn = new CorpuserUrn(userName).toString();

      // If just-in-time User Provisioning is enabled, try to create the DataHub user if it does not exist.
      if (oidcConfigs.isJitUserProvisioningEnabled()) {
        provisionUser(corpUserUrn, profile);
      }

      // If extraction of groups is enabled, create DataHub Groups and GroupMembership
      if (oidcConfigs.isExtractGroupsEnabled()) {
        extractGroups(corpUserUrn, profile);
      }
      context.getJavaSession().put(ACTOR, corpUserUrn);
      return result.withCookies(createActorCookie(corpUserUrn, oidcConfigs.getSessionTtlInHours()));
    }
    throw new RuntimeException(
        "Failed to authenticate current user. Cannot find valid identity provider profile in session");
  }

  private String extractUserNameOrThrow(final OidcConfigs oidcConfigs, final CommonProfile profile) {
    final String userNameClaim = (String) profile.getAttribute(oidcConfigs.getUserNameClaim());

    // Ensure that the attribute exists (was returned by IdP)
    if (!profile.containsAttribute(userNameClaim)) {
      throw new RuntimeException(
          String.format(
              "Failed to resolve user name claim from profile provided by Identity Provider. Missing attribute '%s'",
              userNameClaim
          ));
    }

    // Ensure that we can extract a valid username mapping.
    final Optional<String> mappedUserName = extractRegexGroup(
        oidcConfigs.getUserNameClaimRegex(),
        (String) profile.getAttribute(oidcConfigs.getUserNameClaim())
    );

    return mappedUserName.orElseThrow(() ->
        new RuntimeException(String.format("Failed to extract DataHub username from username claim %s using regex %s",
            userNameClaim,
            oidcConfigs.getUserNameClaimRegex())));
  }

  private void provisionUser(String urn, CommonProfile profile) {
    // nothing yet.
    // We want to provision a user using a restli client.

  }

  private void extractGroups(String urn, CommonProfile profile) {
    // nothing yet.
  }

  private Optional<String> extractRegexGroup(final String patternStr, final String target) {
    final Pattern pattern = Pattern.compile(patternStr);
    final Matcher matcher = pattern.matcher(target);
    if (matcher.find()) {
      final String extractedValue = matcher.group();
      return Optional.of(extractedValue);
    }
    return Optional.empty();
  }

  private boolean isOidcEnabled() {
    return _ssoManager.isSsoEnabled() && _ssoManager.getSsoProvider() instanceof OidcClientProvider;
  }
}
