package auth.sso.oidc;

import static org.junit.jupiter.api.Assertions.*;

import auth.sso.oidc.custom.CustomOidcClient;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.HashMap;
import java.util.Map;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.pac4j.oidc.client.OidcClient;

public class OidcConfigsPrivateKeyJwtTest {

  @Test
  void secretRequiredForClientSecretBasic() {
    Map<String, Object> values = baseConfig();
    values.remove("auth.oidc.clientSecret");
    OidcConfigs.Builder builder = new OidcConfigs.Builder().from(ConfigFactory.parseMap(values));
    assertThrows(NullPointerException.class, builder::build);
  }

  @Test
  void privateKeyJwtRequiresKeyAndCertificatePaths() {
    Map<String, Object> values = baseConfig();
    values.remove("auth.oidc.clientSecret");
    values.put("auth.oidc.clientAuthenticationMethod", OidcConfigs.PRIVATE_KEY_JWT_METHOD);

    OidcConfigs.Builder missingBoth =
        new OidcConfigs.Builder().from(ConfigFactory.parseMap(values));
    assertThrows(IllegalArgumentException.class, missingBoth::build);

    values.put("auth.oidc.privateKeyFilePath", TestKeyMaterial.PRIVATE_KEY_PATH);
    OidcConfigs.Builder missingCert =
        new OidcConfigs.Builder().from(ConfigFactory.parseMap(values));
    assertThrows(IllegalArgumentException.class, missingCert::build);

    values.put("auth.oidc.certificateFilePath", TestKeyMaterial.CERTIFICATE_PATH);
    OidcConfigs configs = new OidcConfigs.Builder().from(ConfigFactory.parseMap(values)).build();
    assertNull(configs.getClientSecret());
    assertEquals(TestKeyMaterial.PRIVATE_KEY_PATH, configs.getPrivateKeyFilePath().orElseThrow());
    assertEquals(TestKeyMaterial.CERTIFICATE_PATH, configs.getCertificateFilePath().orElseThrow());
  }

  @Test
  void envPrivateKeyJwtSurvivesSsoJsonThatOmitsPaths() {
    Map<String, Object> values = jwtConfig();
    Config config = ConfigFactory.parseMap(values);
    OidcConfigs configs =
        new OidcConfigs.Builder().from(config).from(config, new JSONObject().toString()).build();
    assertEquals(TestKeyMaterial.PRIVATE_KEY_PATH, configs.getPrivateKeyFilePath().orElseThrow());
    assertEquals("explicit-kid", configs.getPrivateKeyJwtKid().orElseThrow());
  }

  @Test
  void ssoJsonOverlaysPrivateKeyJwtFields() {
    Map<String, Object> values = jwtConfig();
    String json =
        new JSONObject()
            .put("privateKeyJwtKid", "from-json")
            .put("privateKeyJwtAlgorithm", "RS384")
            .toString();
    Config config = ConfigFactory.parseMap(values);
    OidcConfigs configs = new OidcConfigs.Builder().from(config).from(config, json).build();
    assertEquals("from-json", configs.getPrivateKeyJwtKid().orElseThrow());
    assertEquals("RS384", configs.getPrivateKeyJwtAlgorithm());
  }

  @Test
  void oidcProviderOmitsSecretWhenUsingPrivateKeyJwt() {
    OidcConfigs configs =
        new OidcConfigs.Builder().from(ConfigFactory.parseMap(jwtConfig())).build();
    OidcProvider provider = new OidcProvider(configs);
    assertNull(((OidcClient) provider.client()).getConfiguration().getSecret());
    assertTrue(provider.client() instanceof CustomOidcClient);
  }

  private static Map<String, Object> jwtConfig() {
    Map<String, Object> values = baseConfig();
    values.remove("auth.oidc.clientSecret");
    values.put("auth.oidc.clientAuthenticationMethod", OidcConfigs.PRIVATE_KEY_JWT_METHOD);
    values.put("auth.oidc.privateKeyFilePath", TestKeyMaterial.PRIVATE_KEY_PATH);
    values.put("auth.oidc.certificateFilePath", TestKeyMaterial.CERTIFICATE_PATH);
    values.put("auth.oidc.privateKeyJwtKid", "explicit-kid");
    return values;
  }

  private static Map<String, Object> baseConfig() {
    Map<String, Object> values = new HashMap<>();
    values.put("auth.oidc.enabled", "true");
    values.put("auth.oidc.clientId", "test-client");
    values.put("auth.oidc.clientSecret", "test-secret");
    values.put("auth.oidc.discoveryUri", "https://example.com/.well-known/openid-configuration");
    values.put("auth.baseUrl", "http://localhost:9002");
    return values;
  }
}
