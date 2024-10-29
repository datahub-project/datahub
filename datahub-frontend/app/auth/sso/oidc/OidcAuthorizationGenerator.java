package auth.sso.oidc;

import com.nimbusds.jwt.JWT;
import com.nimbusds.jwt.JWTParser;
import java.util.Map.Entry;
import java.util.Optional;
import org.pac4j.core.authorization.generator.AuthorizationGenerator;
import org.pac4j.core.context.CallContext;
import org.pac4j.core.profile.AttributeLocation;
import org.pac4j.core.profile.CommonProfile;
import org.pac4j.core.profile.UserProfile;
import org.pac4j.core.profile.definition.ProfileDefinition;
import org.pac4j.oidc.profile.OidcProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OidcAuthorizationGenerator implements AuthorizationGenerator {

  private static final Logger logger = LoggerFactory.getLogger(OidcAuthorizationGenerator.class);

  private final ProfileDefinition profileDef;
  private final OidcConfigs oidcConfigs;

  public OidcAuthorizationGenerator(
      final ProfileDefinition profileDef, final OidcConfigs oidcConfigs) {
    this.profileDef = profileDef;
    this.oidcConfigs = oidcConfigs;
  }

  @Override
  public Optional<UserProfile> generate(final CallContext context, final UserProfile profile) {
    if (!(profile instanceof OidcProfile oidcProfile)) {
      return Optional.of(profile);
    }

    if (oidcConfigs.getExtractJwtAccessTokenClaims().orElse(false)) {
      try {
        final JWT jwt = JWTParser.parse(oidcProfile.getAccessToken().getValue());

        CommonProfile commonProfile = new CommonProfile();

        // Copy existing attributes
        profile.getAttributes().forEach(commonProfile::addAttribute);

        // Add JWT claims
        for (final Entry<String, Object> entry : jwt.getJWTClaimsSet().getClaims().entrySet()) {
          final String claimName = entry.getKey();

          if (profile.getAttribute(claimName) == null) {
            profileDef.convertAndAdd(
                commonProfile, AttributeLocation.PROFILE_ATTRIBUTE, claimName, entry.getValue());
          }
        }

        return Optional.of(commonProfile);
      } catch (Exception e) {
        logger.warn("Cannot parse access token claims", e);
      }
    }

    return Optional.of(profile);
  }
}
