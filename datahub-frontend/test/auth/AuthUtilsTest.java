package auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nimbusds.jwt.JWTClaimsSet;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class AuthUtilsTest {

  @Test
  void extractTokenId_signedJwtWithJti_returnsJti() throws Exception {
    String token = JwtTestUtils.signedJwtWithId("0f8fad5b-d9cb-469f-a165-70867728950e");

    assertEquals(
        Optional.of("0f8fad5b-d9cb-469f-a165-70867728950e"),
        AuthUtils.extractTokenId("Bearer " + token));
    assertEquals(
        Optional.of("0f8fad5b-d9cb-469f-a165-70867728950e"),
        AuthUtils.extractTokenId("bearer " + token),
        "Bearer scheme is case-insensitive");
  }

  @Test
  void extractTokenId_jwtWithoutJti_returnsEmpty() throws Exception {
    String token = JwtTestUtils.signedJwt(new JWTClaimsSet.Builder().subject("datahub").build());

    assertTrue(AuthUtils.extractTokenId("Bearer " + token).isEmpty());
  }

  @Test
  void extractTokenId_missingOrNonBearerValue_returnsEmpty() {
    assertTrue(AuthUtils.extractTokenId(null).isEmpty());
    assertTrue(AuthUtils.extractTokenId("").isEmpty());
    assertTrue(AuthUtils.extractTokenId("Basic ZGF0YWh1YjpkYXRhaHVi").isEmpty());
  }

  @Test
  void extractTokenId_opaqueBearerToken_returnsEmpty() {
    assertTrue(AuthUtils.extractTokenId("Bearer not-a-jwt").isEmpty());
  }

  @Test
  void extractTokenId_jtiUnsafeForHeader_returnsEmpty() throws Exception {
    String token = JwtTestUtils.signedJwtWithId("evil\r\nX-Injected: 1");

    assertTrue(AuthUtils.extractTokenId("Bearer " + token).isEmpty());
  }
}
