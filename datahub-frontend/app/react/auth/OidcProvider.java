package react.auth;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.datahub.graphql.GmsClientFactory;
import com.linkedin.entity.client.EntityClient;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.pac4j.core.client.Client;
import org.pac4j.core.engine.CallbackLogic;
import org.pac4j.core.engine.DefaultCallbackLogic;
import org.pac4j.core.http.callback.PathParameterCallbackUrlResolver;
import org.pac4j.core.profile.CommonProfile;
import org.pac4j.core.profile.ProfileManager;
import org.pac4j.oidc.config.OidcConfiguration;
import org.pac4j.oidc.credentials.OidcCredentials;
import org.pac4j.oidc.profile.OidcProfile;
import org.pac4j.play.PlayWebContext;
import play.mvc.Result;

import static react.auth.AuthUtils.*;


public class OidcProvider implements SsoProvider {

  private final EntityClient _entityClient = GmsClientFactory.getEntitiesClient();
  private final OidcConfigs _oidcConfigs;
  private final Client<OidcCredentials, OidcProfile> _oidcClient;

  public OidcProvider(OidcConfigs configs) {
    _oidcConfigs = configs;
    _oidcClient = createOidcClient();
  }

  @Override
  public Client<OidcCredentials, OidcProfile> getClient() {
    return _oidcClient;
  }

  @Override
  public CallbackLogic<Result, PlayWebContext> getCallback() {

    return new DefaultCallbackLogic<>() {

    }

    final Result result = super.perform(context, config, httpActionAdapter, inputDefaultUrl, inputSaveInSession, inputMultiProfile, inputRenewSession, client);
    if (profileManager.isAuthenticated()) {
      final CommonProfile profile = profileManager.get(true).get();

      // Extract the User name required to log into DataHub.
      final String userName = extractUserNameOrThrow(profile);
      final String corpUserUrn = new CorpuserUrn(userName).toString();

      // If just-in-time User Provisioning is enabled, try to create the DataHub user if it does not exist.
      if (_oidcConfigs.isJitUserProvisioningEnabled()) {
        provisionUser(corpUserUrn, profile);
      }

      // If extraction of groups is enabled, create DataHub Groups and GroupMembership
      if (_oidcConfigs.isExtractGroupsEnabled()) {
        extractGroups(corpUserUrn, profile);
      }

      context.getJavaSession().put(ACTOR, corpUserUrn);
      return result.withCookies(createActorCookie(corpUserUrn, _oidcConfigs.getSessionTtlInHours()));
    }
    throw new RuntimeException(
        "Failed to authenticate current user. Cannot find valid identity provider profile in session");
  }


  private String extractUserNameOrThrow(final CommonProfile profile) {
    final String userNameClaim = (String) profile.getAttribute(_oidcConfigs.getUserNameClaim());

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
        _oidcConfigs.getUserNameClaimRegex(),
        (String) profile.getAttribute(_oidcConfigs.getUserNameClaim())
    );

    return mappedUserName.orElseThrow(() ->
        new RuntimeException(String.format("Failed to extract DataHub username from username claim %s using regex %s",
            userNameClaim,
            _oidcConfigs.getUserNameClaimRegex())));
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

  private void provisionUser(String urn, CommonProfile profile) {
    // nothing yet.
    // We want to provision a user using a restli client.



  }

  private void extractGroups(String urn, CommonProfile profile) {
    // nothing yet.
  }

  private Client<OidcCredentials, OidcProfile> createOidcClient() {
    final OidcConfiguration oidcConfiguration = new OidcConfiguration();
    oidcConfiguration.setClientId(_oidcConfigs.getClientId());
    oidcConfiguration.setSecret(_oidcConfigs.getClientSecret());
    oidcConfiguration.setDiscoveryURI(_oidcConfigs.getDiscoveryUri());
    oidcConfiguration.setClientAuthenticationMethodAsString(_oidcConfigs.getClientAuthenticationMethod());
    oidcConfiguration.setScope(_oidcConfigs.getScope());

    final org.pac4j.oidc.client.OidcClient<OidcProfile, OidcConfiguration>  oidcClient = new org.pac4j.oidc.client.OidcClient<>(oidcConfiguration);
    oidcClient.setName(_oidcConfigs.getClientName());
    oidcClient.setCallbackUrlResolver(new PathParameterCallbackUrlResolver());
    return oidcClient;
  }
}
