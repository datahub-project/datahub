package io.datahubproject.metadata.services;

import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.SystemAspect;
import io.datahubproject.metadata.context.ActorContext;
import io.datahubproject.metadata.context.AgentClass;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import java.io.IOException;
import java.util.Base64;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class SecretServiceTest {

  private static final String TEST_SECRET = "test-secret-key-12345";
  private static final String ALTERNATIVE_SECRET = "different-secret-key-67890";
  private SecretService secretService;

  @BeforeMethod
  public void setUp() {
    secretService = new SecretService(TEST_SECRET, true);
  }

  // ---- helper: build a mock OperationContext with a given AgentClass ----

  private OperationContext mockOpCtxWithAgent(AgentClass agentClass) {
    return mockOpCtxWithAgentAndActor(agentClass, "urn:li:corpuser:testuser", false);
  }

  private OperationContext mockOpCtxWithAgentAndActor(
      AgentClass agentClass, String actorUrnStr, boolean systemAuth) {
    RequestContext rc = Mockito.mock(RequestContext.class);
    when(rc.getAgentClass()).thenReturn(agentClass);
    when(rc.getRequestAPI()).thenReturn(RequestContext.RequestAPI.GRAPHQL);
    when(rc.getActorUrn()).thenReturn(actorUrnStr);
    when(rc.getRequestID()).thenReturn("test-request-id");

    ActorContext actorCtx = Mockito.mock(ActorContext.class);
    when(actorCtx.getActorUrn()).thenReturn(UrnUtils.getUrn(actorUrnStr));

    OperationContext opCtx = Mockito.mock(OperationContext.class);
    when(opCtx.getRequestContext()).thenReturn(rc);
    when(opCtx.isSystemAuth()).thenReturn(systemAuth);
    when(opCtx.getSessionActorContext()).thenReturn(actorCtx);
    return opCtx;
  }

  private OperationContext mockSystemAuthOpCtx() {
    OperationContext opCtx = Mockito.mock(OperationContext.class);
    when(opCtx.isSystemAuth()).thenReturn(true);
    when(opCtx.getRequestContext()).thenReturn(null);
    return opCtx;
  }

  private OperationContext mockOpCtxWithActorUrn(String actorUrnStr) {
    return mockOpCtxWithActorUrn(actorUrnStr, false);
  }

  private OperationContext mockOpCtxWithActorUrn(String actorUrnStr, boolean systemCorpUser) {
    ActorContext actorCtx = Mockito.mock(ActorContext.class);
    when(actorCtx.getActorUrn()).thenReturn(UrnUtils.getUrn(actorUrnStr));

    OperationContext opCtx = Mockito.mock(OperationContext.class);
    when(opCtx.isSystemAuth()).thenReturn(false);
    when(opCtx.getSessionActorContext()).thenReturn(actorCtx);
    when(opCtx.getRequestContext()).thenReturn(null);

    if (systemCorpUser) {
      AspectRetriever aspectRetriever = Mockito.mock(AspectRetriever.class);
      SystemAspect systemAspect = Mockito.mock(SystemAspect.class);
      when(systemAspect.getAspect(CorpUserInfo.class))
          .thenReturn(new CorpUserInfo().setSystem(true));
      when(aspectRetriever.getLatestSystemAspect(
              eq(opCtx),
              eq(UrnUtils.getUrn(actorUrnStr)),
              eq(Constants.CORP_USER_INFO_ASPECT_NAME)))
          .thenReturn(systemAspect);
      when(opCtx.getAspectRetriever()).thenReturn(aspectRetriever);
    }
    return opCtx;
  }

  private OperationContext mockOpCtxWithNullRequestContext() {
    ActorContext actorCtx = Mockito.mock(ActorContext.class);
    when(actorCtx.getActorUrn()).thenReturn(UrnUtils.getUrn("urn:li:corpuser:testuser"));

    OperationContext opCtx = Mockito.mock(OperationContext.class);
    when(opCtx.getRequestContext()).thenReturn(null);
    when(opCtx.isSystemAuth()).thenReturn(false);
    when(opCtx.getSessionActorContext()).thenReturn(actorCtx);
    return opCtx;
  }

  // ---- existing encrypt/decrypt tests (updated to pass null opContext) ----

  @Test
  public void testEncryptDecrypt() {
    String plaintext = "This is a test credential";
    String encrypted = secretService.encrypt(null, plaintext);
    String decrypted = secretService.decrypt(null, encrypted);
    assertNotNull(encrypted);
    assertTrue(encrypted.startsWith("v2:"));
    assertEquals(decrypted, plaintext);
  }

  @Test
  public void testEncryptionProducesDifferentCiphertexts() {
    String plaintext = "Same plaintext";
    String encrypted1 = secretService.encrypt(null, plaintext);
    String encrypted2 = secretService.encrypt(null, plaintext);
    assertNotEquals(
        encrypted1, encrypted2, "Encryption should produce different ciphertexts due to random IV");
    assertEquals(secretService.decrypt(null, encrypted1), plaintext);
    assertEquals(secretService.decrypt(null, encrypted2), plaintext);
  }

  @Test
  public void testLegacyDecryption() {
    String plaintext = "Legacy credential";
    String legacyEncrypted = secretService.encryptLegacy(plaintext);
    String decrypted = secretService.decrypt(null, legacyEncrypted);
    assertFalse(legacyEncrypted.startsWith("v2:"));
    assertEquals(decrypted, plaintext);
  }

  @Test
  public void testIsLegacyFormat() {
    String v2Encrypted = secretService.encrypt(null, "test");
    String legacyEncrypted = secretService.encryptLegacy("test");
    assertFalse(secretService.isLegacyFormat(v2Encrypted));
    assertTrue(secretService.isLegacyFormat(legacyEncrypted));
  }

  @Test(
      expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp = ".*Failed to decrypt.*")
  public void testDecryptWithWrongSecret() {
    String encrypted = secretService.encrypt(null, "test credential");
    SecretService wrongSecretService = new SecretService(ALTERNATIVE_SECRET, true);
    wrongSecretService.decrypt(null, encrypted);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testDecryptInvalidData() {
    secretService.decrypt(null, "v2:invalid-base64-data!!!");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testEncryptNull() {
    secretService.encrypt(null, null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testDecryptNull() {
    secretService.decrypt(null, null);
  }

  @DataProvider(name = "credentialTestData")
  public Object[][] credentialTestData() {
    return new Object[][] {
      {"simple-password"},
      {"password-with-special-chars!@#$%^&*()"},
      {"very long credential " + "x".repeat(1000)},
      {"unicode-密码-パスワード"},
      {""}, // empty string
      {"credential\nwith\nnewlines"},
      {"credential\twith\ttabs"},
      {"{\"user\":\"admin\",\"password\":\"secret123\"}"} // JSON credential
    };
  }

  @Test(dataProvider = "credentialTestData")
  public void testEncryptDecryptVariousCredentials(String credential) {
    String encrypted = secretService.encrypt(null, credential);
    String decrypted = secretService.decrypt(null, encrypted);
    assertEquals(decrypted, credential);
  }

  @Test
  public void testGenerateUrlSafeToken() {
    int length = 32;
    String token1 = secretService.generateUrlSafeToken(length);
    String token2 = secretService.generateUrlSafeToken(length);
    assertEquals(token1.length(), length);
    assertEquals(token2.length(), length);
    assertNotEquals(token1, token2);
    assertTrue(token1.matches("[a-z]+"));
    assertTrue(token2.matches("[a-z]+"));
  }

  @Test
  public void testHashString() {
    String input = "test string to hash";
    String hash1 = secretService.hashString(input);
    String hash2 = secretService.hashString(input);
    assertNotNull(hash1);
    assertEquals(hash1, hash2, "Hashing should be deterministic");
    try {
      Base64.getDecoder().decode(hash1);
    } catch (IllegalArgumentException e) {
      fail("Hash should be valid base64: " + e.getMessage());
    }
    assertNotEquals(hash1, secretService.hashString("different input"));
  }

  @Test
  public void testGenerateSalt() {
    int length = 16;
    byte[] salt1 = secretService.generateSalt(length);
    byte[] salt2 = secretService.generateSalt(length);
    assertEquals(salt1.length, length);
    assertEquals(salt2.length, length);
    assertNotEquals(salt1, salt2);
  }

  @Test
  public void testGetHashedPassword() throws IOException {
    byte[] salt = secretService.generateSalt(16);
    String password = "myPassword123";
    String hashedPassword1 = secretService.getHashedPassword(salt, password);
    String hashedPassword2 = secretService.getHashedPassword(salt, password);
    assertEquals(
        hashedPassword1, hashedPassword2, "Same salt and password should produce same hash");
    byte[] differentSalt = secretService.generateSalt(16);
    assertNotEquals(hashedPassword1, secretService.getHashedPassword(differentSalt, password));
  }

  @Test
  public void testBackwardCompatibilityRoundTrip() {
    String[] testCredentials = {"database-password-123", "api-key-xyz789", "oauth-secret-abc456"};
    for (String credential : testCredentials) {
      String legacyEncrypted = secretService.encryptLegacy(credential);
      assertEquals(secretService.decrypt(null, legacyEncrypted), credential);
      String newEncrypted = secretService.encrypt(null, credential);
      assertTrue(newEncrypted.startsWith("v2:"));
      assertEquals(secretService.decrypt(null, newEncrypted), credential);
    }
  }

  @Test
  public void testEncryptedDataStructure() {
    String encrypted = secretService.encrypt(null, "test");
    assertTrue(encrypted.startsWith("v2:"));
    byte[] decoded = Base64.getDecoder().decode(encrypted.substring(3));
    assertTrue(
        decoded.length >= 12 + 1 + 16, "Encrypted data should contain IV, ciphertext, and tag");
  }

  @Test
  public void testDeterministicKeyDerivation() {
    SecretService service1 = new SecretService(TEST_SECRET, true);
    SecretService service2 = new SecretService(TEST_SECRET, true);
    String plaintext = "test credential";
    String encrypted = service1.encrypt(null, plaintext);
    assertEquals(
        service2.decrypt(null, encrypted),
        plaintext,
        "Different instances with same secret should be compatible");
  }

  @Test
  public void testLargeCredential() {
    StringBuilder largeCredential = new StringBuilder();
    for (int i = 0; i < 10000; i++) {
      largeCredential.append("This is line ").append(i).append(" of a large credential.\n");
    }
    String plaintext = largeCredential.toString();
    assertEquals(secretService.decrypt(null, secretService.encrypt(null, plaintext)), plaintext);
  }

  @Test
  public void testEmptyStringEncryption() {
    String empty = "";
    String encrypted = secretService.encrypt(null, empty);
    assertTrue(encrypted.startsWith("v2:"));
    assertEquals(secretService.decrypt(null, encrypted), empty);
  }

  @Test(
      expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp = ".*Failed to decrypt.*")
  public void testCorruptedEncryptedData() {
    String encrypted = secretService.encrypt(null, "test");
    String corrupted = encrypted.substring(0, 10) + "XXX" + encrypted.substring(13);
    secretService.decrypt(null, corrupted);
  }

  @Test(
      expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*Legacy v1 decryption is disabled.*")
  public void testDecryptLegacyWhenV1Disabled() {
    SecretService v1DisabledService = new SecretService(TEST_SECRET, false);
    String legacyEncrypted = secretService.encryptLegacy("test credential");
    v1DisabledService.decrypt(null, legacyEncrypted);
  }

  @Test
  public void testV2EncryptionWorksWhenV1Disabled() {
    SecretService v1DisabledService = new SecretService(TEST_SECRET, false);
    String plaintext = "test credential";
    String encrypted = v1DisabledService.encrypt(null, plaintext);
    assertTrue(encrypted.startsWith("v2:"));
    assertEquals(v1DisabledService.decrypt(null, encrypted), plaintext);
  }

  // ---- caller-guard tests ----

  @Test(expectedExceptions = SecurityException.class)
  public void testDecryptDeniedForBrowserInEnforceMode() {
    OperationContext opCtx = mockOpCtxWithAgent(AgentClass.BROWSER);
    String encrypted = secretService.encrypt(null, "secret");
    // ENFORCE is the default mode; BROWSER is human → must throw
    secretService.decrypt(opCtx, encrypted);
  }

  @Test(expectedExceptions = SecurityException.class)
  public void testDecryptDeniedForMobileAppInEnforceMode() {
    OperationContext opCtx = mockOpCtxWithAgent(AgentClass.MOBILE_APP);
    String encrypted = secretService.encrypt(null, "secret");
    secretService.decrypt(opCtx, encrypted);
  }

  // Non-system actors are denied decrypt regardless of agent class (identity check)

  @Test(expectedExceptions = SecurityException.class)
  public void testDecryptDeniedForNonSystemIngestionAgent() {
    OperationContext opCtx = mockOpCtxWithAgent(AgentClass.INGESTION);
    secretService.decrypt(opCtx, secretService.encrypt(null, "ingestion-secret"));
  }

  @Test(expectedExceptions = SecurityException.class)
  public void testDecryptDeniedForNonSystemSdkAgent() {
    OperationContext opCtx = mockOpCtxWithAgent(AgentClass.SDK);
    secretService.decrypt(opCtx, secretService.encrypt(null, "sdk-secret"));
  }

  @Test(expectedExceptions = SecurityException.class)
  public void testDecryptDeniedForNonSystemCliAgent() {
    OperationContext opCtx = mockOpCtxWithAgent(AgentClass.CLI);
    secretService.decrypt(opCtx, secretService.encrypt(null, "cli-secret"));
  }

  @Test(expectedExceptions = SecurityException.class)
  public void testDecryptDeniedForNonSystemActorWithSystemAgentClass() {
    OperationContext opCtx = mockOpCtxWithAgent(AgentClass.SYSTEM);
    secretService.decrypt(opCtx, secretService.encrypt(null, "system-agent-secret"));
  }

  @Test(expectedExceptions = SecurityException.class)
  public void testDecryptDeniedForNonSystemUnknownAgent() {
    OperationContext opCtx = mockOpCtxWithAgent(AgentClass.UNKNOWN);
    secretService.decrypt(opCtx, secretService.encrypt(null, "unknown-secret"));
  }

  @Test(expectedExceptions = SecurityException.class)
  public void testDecryptDeniedForNonSystemActorWithNullRequestContext() {
    OperationContext opCtx = mockOpCtxWithNullRequestContext();
    secretService.decrypt(opCtx, secretService.encrypt(null, "background-secret"));
  }

  // System principals are allowed regardless of agent class

  @Test
  public void testDecryptAllowedForSystemAuth() {
    OperationContext opCtx = mockSystemAuthOpCtx();
    String plaintext = "system-auth-secret";
    assertEquals(secretService.decrypt(opCtx, secretService.encrypt(null, plaintext)), plaintext);
  }

  @Test
  public void testDecryptAllowedForSystemActorUrn() {
    OperationContext opCtx = mockOpCtxWithActorUrn(SYSTEM_ACTOR);
    String plaintext = "system-actor-secret";
    assertEquals(secretService.decrypt(opCtx, secretService.encrypt(null, plaintext)), plaintext);
  }

  @Test
  public void testDecryptAllowedForSystemCorpUserFlag() {
    String systemUserUrn = "urn:li:corpuser:system-user";
    OperationContext opCtx = mockOpCtxWithActorUrn(systemUserUrn, true);
    String plaintext = "system-corp-user-secret";
    assertEquals(secretService.decrypt(opCtx, secretService.encrypt(null, plaintext)), plaintext);
  }

  @Test(expectedExceptions = SecurityException.class)
  public void testDecryptDeniedForRegularCorpUserWithoutSystemFlag() {
    OperationContext opCtx =
        mockOpCtxWithAgentAndActor(AgentClass.INGESTION, "urn:li:corpuser:regular", false);
    secretService.decrypt(opCtx, secretService.encrypt(null, "regular-user-secret"));
  }

  @Test(expectedExceptions = SecurityException.class)
  public void testDecryptDeniedWhenSystemFlagLookupFails() {
    String corpUserUrn = "urn:li:corpuser:lookup-failure";
    AspectRetriever aspectRetriever = Mockito.mock(AspectRetriever.class);
    when(aspectRetriever.getLatestSystemAspect(any(), any(), any()))
        .thenThrow(new RuntimeException("aspect lookup failed"));

    ActorContext actorCtx = Mockito.mock(ActorContext.class);
    when(actorCtx.getActorUrn()).thenReturn(UrnUtils.getUrn(corpUserUrn));

    OperationContext opCtx = Mockito.mock(OperationContext.class);
    when(opCtx.isSystemAuth()).thenReturn(false);
    when(opCtx.getSessionActorContext()).thenReturn(actorCtx);
    when(opCtx.getRequestContext()).thenReturn(null);
    when(opCtx.getAspectRetriever()).thenReturn(aspectRetriever);

    secretService.decrypt(opCtx, secretService.encrypt(null, "lookup-failure-secret"));
  }

  @Test
  public void testDecryptAllowedWhenOpContextIsNull() {
    // Legacy callers and RotateSecretsStep pass null opContext
    String plaintext = "legacy-secret";
    String encrypted = secretService.encrypt(null, plaintext);
    assertEquals(secretService.decrypt(null, encrypted), plaintext);
  }

  @Test
  public void testDecryptAuditModeLogsButDoesNotThrowForBrowser() {
    SecretService auditService =
        new SecretService(TEST_SECRET, true, SecretService.CallerGuardMode.AUDIT);
    OperationContext opCtx = mockOpCtxWithAgent(AgentClass.BROWSER);
    String plaintext = "audit-secret";
    String encrypted = auditService.encrypt(null, plaintext);
    // Should NOT throw — AUDIT mode only logs
    assertEquals(auditService.decrypt(opCtx, encrypted), plaintext);
  }

  @Test
  public void testDecryptDisabledModeBypassesGuardForBrowser() {
    SecretService disabledService =
        new SecretService(TEST_SECRET, true, SecretService.CallerGuardMode.DISABLED);
    OperationContext opCtx = mockOpCtxWithAgent(AgentClass.BROWSER);
    String plaintext = "disabled-secret";
    String encrypted = disabledService.encrypt(null, plaintext);
    // Should NOT throw — DISABLED mode skips the guard entirely
    assertEquals(disabledService.decrypt(opCtx, encrypted), plaintext);
  }

  @Test
  public void testEncryptAuditOnlyForBrowserInEnforceMode() {
    // encrypt() never throws for human agents even in ENFORCE mode — browsers legitimately
    // encrypt on write paths (password reset, OIDC config save)
    OperationContext opCtx = mockOpCtxWithAgent(AgentClass.BROWSER);
    // Should NOT throw — encrypt() uses hardDeny=false
    String encrypted = secretService.encrypt(opCtx, "browser-initiated-value");
    assertNotNull(encrypted);
  }
}
