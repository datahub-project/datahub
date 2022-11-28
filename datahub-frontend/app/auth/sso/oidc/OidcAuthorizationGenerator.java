package auth.sso.oidc;

import java.text.ParseException;
import java.util.Map.Entry;

import com.nimbusds.jose.Algorithm;
import com.nimbusds.jose.Header;
import com.nimbusds.jose.JWEAlgorithm;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.util.Base64URL;
import com.nimbusds.jose.util.JSONObjectUtils;
import com.nimbusds.jwt.EncryptedJWT;
import com.nimbusds.jwt.JWT;
import com.nimbusds.jwt.SignedJWT;
import net.minidev.json.JSONObject;
import org.pac4j.core.authorization.generator.AuthorizationGenerator;
import org.pac4j.core.context.WebContext;
import org.pac4j.core.profile.AttributeLocation;
import org.pac4j.core.profile.CommonProfile;
import org.pac4j.core.profile.definition.ProfileDefinition;
import org.pac4j.oidc.profile.OidcProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
                final JWT jwt = parse(((OidcProfile) profile).getAccessToken().getValue());
    
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

    public static JWT parse(final String s) throws ParseException {
        final int firstDotPos = s.indexOf(".");

        if (firstDotPos == -1) {
            throw new ParseException("Invalid JWT serialization: Missing dot delimiter(s)", 0);
        }

        Base64URL header = new Base64URL(s.substring(0, firstDotPos));
        JSONObject jsonObject;

        try {
            jsonObject = JSONObjectUtils.parse(header.decodeToString());
        } catch (ParseException e) {
            throw new ParseException("Invalid unsecured/JWS/JWE header: " + e.getMessage(), 0);
        }

        Algorithm alg = Header.parseAlgorithm(jsonObject);

        if (alg instanceof JWSAlgorithm) {
            return SignedJWT.parse(s);
        } else if (alg instanceof JWEAlgorithm) {
            return EncryptedJWT.parse(s);
        } else {
            throw new AssertionError("Unexpected algorithm type: " + alg);
        }
    }
    
}
