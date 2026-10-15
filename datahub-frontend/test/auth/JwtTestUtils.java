package auth;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

/**
 * Builds HS256-signed JWTs shaped like the tokens GMS issues, for tests that only read claims and
 * never verify the signature.
 */
public final class JwtTestUtils {
  private static final byte[] SIGNING_KEY = new byte[32];

  private JwtTestUtils() {}

  public static String signedJwt(JWTClaimsSet claims) throws JOSEException {
    SignedJWT jwt = new SignedJWT(new JWSHeader(JWSAlgorithm.HS256), claims);
    jwt.sign(new MACSigner(SIGNING_KEY));
    return jwt.serialize();
  }

  public static String signedJwtWithId(String jti) throws JOSEException {
    return signedJwt(new JWTClaimsSet.Builder().jwtID(jti).subject("datahub").build());
  }
}
