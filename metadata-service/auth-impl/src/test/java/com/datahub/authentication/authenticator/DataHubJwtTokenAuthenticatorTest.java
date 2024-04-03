package com.datahub.authentication.authenticator;

import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationException;
import com.datahub.authentication.AuthenticationRequest;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import org.testng.annotations.Test;

public class DataHubJwtTokenAuthenticatorTest {

  @Test
  void testPublicAuthentication() throws Exception {
    String token =
        "bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwidXNlcm5hbWUiOiJqb2huX3Nub3ciLCJhZG1pbiI6dH"
            + "J1ZSwiaWF0IjoxNTE2MjM5MDIyLCJpc3MiOiJodHRwczovL3Rlc3QuY29tL3JlYWxtL2RvbWFpbiJ9.Hz6Zpt-b5gpw3YvJ4fk8xfhLQL9Rmwqj2hPh"
            + "VcvpyDw5IHoiMLIxGZsiC80lxfU8a02f-2Tmek5bNKaXbgSNzYWITL5lrwEO-rTXYNamy8gJOBoM8n7gHDOo6JDd25go4MsLbjHbQ-WNq5SErgaNOMfZ"
            + "dkg2jqKVldZvjW33v8aupx08fzONnuzaYIJBQpONhGzDkYZKkkrewdrYYVl_naNRWsKt8uSVu83G3mLhMPazkxNT5CWfNR7sdXfladz8U6ruLFOGUJJ5K"
            + "DjEVAReRpEbxaKOIY6oFio1TeUQsi6vppLXB0RupTBmE5dr7rxdL4j9eDY94M2uowBDuOsEGA";

    HashSet<String> set = new HashSet<>();
    set.add("https://test.com/realm/domain");
    final AuthenticationRequest context =
        new AuthenticationRequest(ImmutableMap.of("Authorization", token));
    DataHubJwtTokenAuthenticator mock = mock(DataHubJwtTokenAuthenticator.class);
    when(mock.authenticate(context)).thenCallRealMethod();

    Map<String, Object> config = new HashMap<>();
    config.put("userIdClaim", "username");
    config.put("trustedIssuers", getTrustedIssuer());
    config.put(
        "publicKey",
        "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAu1SU1LfVLPHCozMxH2Mo4lgOEePzNm0tRgeLezV6ffAt0gunVTLw7onLRnrq0/"
            + "IzW7yWR7QkrmBL7jTKEn5u+qKhbwKfBstIs+bMY2Zkp18gnTxKLxoS2tFczGkPLPgizskuemMghRniWaoLcyehkd3qqGElvW/VDL5AaWTg0nLVkjRo9z+40RQzuVaE"
            + "8AkAFmxZzow3x+VJYKdjykkJ0iT9wCS0DRTXu269V264Vf/3jvredZiKRkgwlL9xNAwxXFg0x/XFw005UWVRIkdgcKWTjpBP2dPwVZ4WWC+9aGVd+Gyn1o0CLelf"
            + "4rEjGoXbAAEgAqeGUxrcIlbjXfbcmwIDAQAB");
    doCallRealMethod().when(mock).init(config, null);

    mock.init(config, null);
    Authentication result = mock.authenticate(context);
    Actor actor = result.getActor();
    String actualCredential = result.getCredentials();
    assertEquals(token, actualCredential);
    assertEquals("urn:li:corpuser:john_snow", actor.toUrnStr());
  }

  @Test(expectedExceptions = AuthenticationException.class)
  void testInvalidToken() throws Exception {
    String token =
        "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwidXNlcm5hbWUiOiJqb2"
            + "huX3Nub3ciLCJhZG1pbiI6dHJ1ZSwiaWF0IjoxNTE2MjM5MDIyLCJpc3MiOiJodHRwczovL3Rlc3QuY29tL3JlYWxtL2R"
            + "vbWFpbiJ9.Hz6Zpt-b5gpw3YvJ4fk8xfhLQL9Rmwqj2hPhVcvpyDw5IHoiMLIxGZsiC80lxfU8a02f-2Tmek5bNKaXbgSNzYWIT"
            + "L5lrwEO-rTXYNamy8gJOBoM8n7gHDOo6JDd25go4MsLbjHbQ-WNq5SErgaNOMfZdkg2jqKVldZvjW33v8aupx08fzONnuzaYIJBQpONhGzDkYZKkk"
            + "rewdrYYVl_naNRWsKt8uSVu83G3mLhMPazkxNT5CWfNR7sdXfladz8U6ruLFOGUJJ5KDjEVAReRpEbxaKOIY6oFio1TeUQsi"
            + "6vppLXB0RupTBmE5dr7rxdL4j9eDY94M2uowBDuOsEGA";
    final AuthenticationRequest context =
        new AuthenticationRequest(ImmutableMap.of("Authorization", token));

    DataHubJwtTokenAuthenticator mock = mock(DataHubJwtTokenAuthenticator.class);
    when(mock.authenticate(context)).thenCallRealMethod();
    Map<String, Object> config = new HashMap<>();
    config.put("userIdClaim", "username");
    config.put("trustedIssuers", getTrustedIssuer());
    doCallRealMethod().when(mock).init(config, null);

    mock.init(config, null);
    Authentication result = mock.authenticate(context);
  }

  @Test(expectedExceptions = AuthenticationException.class)
  void testUserClaim() throws Exception {
    String token =
        "bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwidXNlcm5hbWUiOi"
            + "Jqb2huX3Nub3ciLCJhZG1pbiI6dHJ1ZSwiaWF0IjoxNTE2MjM5MDIyLCJpc3MiOiJodHRwczovL3Rlc3QuY29tL3Jl"
            + "YWxtL2RvbWFpbiJ9.Hz6Zpt-b5gpw3YvJ4fk8xfhLQL9Rmwqj2hPhVcvpyDw5IHoiMLIxGZsiC80lxfU8a02f-2Tmek5bNKaXbgSNz"
            + "YWITL5lrwEO-rTXYNamy8gJOBoM8n7gHDOo6JDd25go4MsLbjHbQ-WNq5SErgaNOMfZdkg2jqKVldZvjW33v8aupx08fzONnuz"
            + "aYIJBQpONhGzDkYZKkkrewdrYYVl_naNRWsKt8uSVu83G3mLhMPazkxNT5CWfNR7sdXfladz8U6ruLFOGUJJ5KDjEVAReRpEbxaKOIY6oF"
            + "io1TeUQsi6vppLXB0RupTBmE5dr7rxdL4j9eDY94M2uowBDuOsEGA";

    HashSet<String> set = new HashSet<>();
    set.add("https://test.com/realm/domain");
    final AuthenticationRequest context =
        new AuthenticationRequest(ImmutableMap.of("Authorization", token));
    DataHubJwtTokenAuthenticator mock = mock(DataHubJwtTokenAuthenticator.class);
    when(mock.authenticate(context)).thenCallRealMethod();

    Map<String, Object> config = new HashMap<>();
    config.put("userId", "username");
    config.put("trustedIssuers", getTrustedIssuer());
    config.put(
        "publicKey",
        "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAu1SU1LfVLPHCozMxH2Mo4lgOEePzNm0tRgeLezV6"
            + "ffAt0gunVTLw7onLRnrq0/IzW7yWR7QkrmBL7jTKEn5u+qKhbwKfBstIs+bMY2Zkp18gnTxKLxoS2tFczGkPLPgizskuemM"
            + "ghRniWaoLcyehkd3qqGElvW/VDL5AaWTg0nLVkjRo9z+40RQzuVaE8AkAFmxZzow3x+VJYKdjykkJ0iT9wCS0DRTXu269V26"
            + "4Vf/3jvredZiKRkgwlL9xNAwxXFg0x/XFw005UWVRIkdgcKWTjpBP2dPwVZ4WWC+9aGVd+Gyn1o0CLelf4rEjGoXbAAEgAqeGUxrcIlbjXfbcmwIDAQAB");
    doCallRealMethod().when(mock).init(config, null);

    mock.init(config, null);
    Authentication result = mock.authenticate(context);
    Actor actor = result.getActor();
    String actualCredential = result.getCredentials();
    assertEquals(token, actualCredential);
    assertEquals("urn:li:corpuser:john_snow", actor.toUrnStr());
  }

  private LinkedHashMap<String, String> getTrustedIssuer() {
    LinkedHashMap<String, String> trustedIssuer = new LinkedHashMap<>();
    trustedIssuer.putIfAbsent("0", "https://test.com/realm/domain");
    return trustedIssuer;
  }
}
