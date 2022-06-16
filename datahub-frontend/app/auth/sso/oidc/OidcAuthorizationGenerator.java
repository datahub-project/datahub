package auth.sso.oidc;

import java.util.Map.Entry;

import org.pac4j.core.authorization.generator.AuthorizationGenerator;
import org.pac4j.core.context.WebContext;
import org.pac4j.core.profile.AttributeLocation;
import org.pac4j.core.profile.CommonProfile;
import org.pac4j.core.profile.definition.ProfileDefinition;
import org.pac4j.oidc.profile.OidcProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nimbusds.jwt.JWT;
import com.nimbusds.jwt.JWTParser;

public class OidcAuthorizationGenerator implements AuthorizationGenerator {

    private static final Logger logger = LoggerFactory.getLogger(OidcAuthorizationGenerator.class);
    
    private final ProfileDefinition profileDef;

    private final OidcConfigs oidcConfigs;

    public OidcAuthorizationGenerator(final ProfileDefinition profileDef, final OidcConfigs oidcConfigs) {
        this.profileDef = profileDef;
        this.oidcConfigs = oidcConfigs;
    }

    @Override
    public CommonProfile generate(WebContext context, CommonProfile profile) {
        if (oidcConfigs.getExtractJwtAccessTokenClaims().orElse(false)) {
            try {
                final JWT jwt = JWTParser.parse(((OidcProfile) profile).getAccessToken().getValue());
    
                for (final Entry<String, Object> entry : jwt.getJWTClaimsSet().getClaims().entrySet()) {
                    final String claimName = entry.getKey();
                    if (profile.getAttribute(claimName) == null) {
                        profileDef.convertAndAdd(profile, AttributeLocation.PROFILE_ATTRIBUTE, claimName, entry.getValue());
                    }
                }
            } catch (Exception e) {
                logger.warn("Cannot parse access token claims", e);
            }
        }
        
        return profile;
    }
    
}
