package com.datahub.auth.authentication;

import static com.datahub.auth.authentication.AuthServiceTestConfiguration.SYSTEM_CLIENT_ID;
import static com.linkedin.metadata.Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOBAL_SETTINGS_URN;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationConfiguration;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authentication.LoginDenialReason;
import com.datahub.authentication.invite.InviteTokenService;
import com.datahub.authentication.session.UserSessionEligibilityChecker;
import com.datahub.authentication.token.StatelessTokenService;
import com.datahub.authentication.token.TokenType;
import com.datahub.authentication.user.NativeUserService;
import com.datahub.telemetry.TrackingService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.datahubusage.event.LoginSource;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.settings.global.OidcSettings;
import com.linkedin.settings.global.SsoSettings;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.services.SecretService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.trace.SpanContext;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.web.servlet.DispatcherServlet;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

@SpringBootTest(classes = {DispatcherServlet.class})
@ComponentScan(basePackages = {"com.datahub.auth.authentication"})
@Import({AuthServiceTestConfiguration.class})
public class AuthServiceControllerTest extends AbstractTestNGSpringContextTests {
  @BeforeTest
  public void disableAssert() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
  }

  @BeforeMethod
  public void stubSessionEligibility() {
    reset(mockUserSessionEligibilityChecker);
    when(mockUserSessionEligibilityChecker.checkEligibility(
            any(OperationContext.class), anyString(), anyBoolean()))
        .thenReturn(Optional.empty());
  }

  @Autowired private AuthServiceController authServiceController;
  @Autowired private EntityService mockEntityService;
  @Autowired private SecretService mockSecretService;
  @Autowired private NativeUserService mockNativeUserService;
  @Autowired private StatelessTokenService mockTokenService;
  @Autowired private InviteTokenService mockInviteTokenService;
  @Autowired private OperationContext systemOperationContext;
  @Autowired private ConfigurationProvider mockConfigProvider;
  @Autowired private SpanContext mockSpanContext;
  @Autowired private ObjectMapper objectMapper;
  @Autowired private TrackingService mockTrackingService;

  @Autowired private UserSessionEligibilityChecker mockUserSessionEligibilityChecker;

  @Autowired private MetricUtils metricUtils;

  private final String PREFERRED_JWS_ALGORITHM = "preferredJwsAlgorithm";

  @BeforeMethod
  public void clearAuthMetrics() {
    if (metricUtils.getRegistry() instanceof SimpleMeterRegistry) {
      ((SimpleMeterRegistry) metricUtils.getRegistry()).clear();
    }
  }

  private double authSessionOutcomeCount(
      String operation, String result, String loginSource, String denialReason) {
    return metricUtils
        .getRegistry()
        .counter(
            MetricUtils.DATAHUB_AUTH_LOGIN_OUTCOMES,
            MetricUtils.TAG_AUTH_OPERATION,
            operation,
            MetricUtils.TAG_AUTH_RESULT,
            result,
            MetricUtils.TAG_AUTH_LOGIN_SOURCE,
            loginSource,
            MetricUtils.TAG_AUTH_DENIAL_REASON,
            denialReason)
        .count();
  }

  @Test
  public void initTest() {
    assertNotNull(authServiceController);
    assertNotNull(mockEntityService);
  }

  @Test
  public void oldPreferredJwsAlgorithmIsNotReturned() throws IOException {
    OidcSettings mockOidcSettings =
        new OidcSettings()
            .setEnabled(true)
            .setClientId("1")
            .setClientSecret("2")
            .setDiscoveryUri("http://localhost")
            .setPreferredJwsAlgorithm("test");
    SsoSettings mockSsoSettings =
        new SsoSettings().setBaseUrl("http://localhost").setOidcSettings(mockOidcSettings);
    GlobalSettingsInfo mockGlobalSettingsInfo = new GlobalSettingsInfo().setSso(mockSsoSettings);

    when(mockEntityService.getLatestAspect(
            any(OperationContext.class),
            eq(GLOBAL_SETTINGS_URN),
            eq(GLOBAL_SETTINGS_INFO_ASPECT_NAME)))
        .thenReturn(mockGlobalSettingsInfo);

    ResponseEntity<String> httpResponse = authServiceController.getSsoSettings(null).join();
    assertEquals(httpResponse.getStatusCode(), HttpStatus.OK);

    JsonNode jsonNode = new ObjectMapper().readTree(httpResponse.getBody());
    assertFalse(jsonNode.has(PREFERRED_JWS_ALGORITHM));
  }

  @Test
  public void newPreferredJwsAlgorithmIsReturned() throws IOException {
    OidcSettings mockOidcSettings =
        new OidcSettings()
            .setEnabled(true)
            .setClientId("1")
            .setClientSecret("2")
            .setDiscoveryUri("http://localhost")
            .setPreferredJwsAlgorithm("jws1")
            .setPreferredJwsAlgorithm2("jws2");
    SsoSettings mockSsoSettings =
        new SsoSettings().setBaseUrl("http://localhost").setOidcSettings(mockOidcSettings);
    GlobalSettingsInfo mockGlobalSettingsInfo = new GlobalSettingsInfo().setSso(mockSsoSettings);

    when(mockEntityService.getLatestAspect(
            any(OperationContext.class),
            eq(GLOBAL_SETTINGS_URN),
            eq(GLOBAL_SETTINGS_INFO_ASPECT_NAME)))
        .thenReturn(mockGlobalSettingsInfo);

    ResponseEntity<String> httpResponse = authServiceController.getSsoSettings(null).join();
    assertEquals(httpResponse.getStatusCode(), HttpStatus.OK);

    JsonNode jsonNode = new ObjectMapper().readTree(httpResponse.getBody());
    assertTrue(jsonNode.has(PREFERRED_JWS_ALGORITHM));
    assertEquals(jsonNode.get(PREFERRED_JWS_ALGORITHM).asText(), "jws2");
  }

  @Test
  public void testGenerateSessionTokenForUserSuccess() throws Exception {
    // Setup
    String userId = "testUser";
    String generatedToken = "test-token-123";

    // Mock authentication as system user
    Authentication systemAuth = mock(Authentication.class);
    Actor systemActor = new Actor(ActorType.USER, SYSTEM_CLIENT_ID);
    when(systemAuth.getActor()).thenReturn(systemActor);
    AuthenticationContext.setAuthentication(systemAuth);

    // Mock token service
    when(mockTokenService.generateAccessToken(eq(TokenType.SESSION), any(Actor.class), anyLong()))
        .thenReturn(generatedToken);

    // Create request body
    ObjectNode requestBody = objectMapper.createObjectNode();
    requestBody.put("userId", userId);
    HttpEntity<String> httpEntity = new HttpEntity<>(objectMapper.writeValueAsString(requestBody));

    when(mockConfigProvider.getAuthentication()).thenReturn(new AuthenticationConfiguration());

    // Execute
    ResponseEntity<String> response =
        authServiceController.generateSessionTokenForUser(httpEntity).join();

    // Verify
    assertEquals(HttpStatus.OK, response.getStatusCode());
    JsonNode responseJson = objectMapper.readTree(response.getBody());
    assertTrue(responseJson.has("accessToken"));
    assertEquals(generatedToken, responseJson.get("accessToken").asText());

    // Verify token service was called with correct parameters
    ArgumentCaptor<Actor> actorCaptor = ArgumentCaptor.forClass(Actor.class);
    verify(mockTokenService)
        .generateAccessToken(eq(TokenType.SESSION), actorCaptor.capture(), anyLong());

    Actor capturedActor = actorCaptor.getValue();
    assertEquals(userId, capturedActor.getId());
    assertEquals(
        1.0,
        authSessionOutcomeCount(
            MetricUtils.AUTH_OPERATION_GENERATE_SESSION_TOKEN,
            MetricUtils.AUTH_RESULT_SUCCESS,
            "unknown",
            MetricUtils.AUTH_DENIAL_REASON_NONE));
  }

  @Test(expectedExceptions = CompletionException.class)
  public void testGenerateSessionTokenForUserUnauthorized() throws Exception {
    // Setup with non-system user
    Authentication nonSystemAuth = mock(Authentication.class);
    Actor regularActor = new Actor(ActorType.USER, "regularActor");
    when(nonSystemAuth.getActor()).thenReturn(regularActor);
    AuthenticationContext.setAuthentication(nonSystemAuth);

    // Create request body
    ObjectNode requestBody = objectMapper.createObjectNode();
    requestBody.put("userId", "testUser");
    HttpEntity<String> httpEntity = new HttpEntity<>(objectMapper.writeValueAsString(requestBody));

    // Execute
    ResponseEntity<String> response =
        authServiceController.generateSessionTokenForUser(httpEntity).join();
  }

  @Test
  public void testGenerateSessionTokenForUserBadRequest() throws Exception {
    // Setup
    Authentication systemAuth = mock(Authentication.class);
    Actor systemActor = new Actor(ActorType.USER, SYSTEM_CLIENT_ID);
    when(systemAuth.getActor()).thenReturn(systemActor);
    AuthenticationContext.setAuthentication(systemAuth);

    // Create invalid request body (missing userId)
    ObjectNode requestBody = objectMapper.createObjectNode();
    HttpEntity<String> httpEntity = new HttpEntity<>(objectMapper.writeValueAsString(requestBody));

    // Execute
    ResponseEntity<String> response =
        authServiceController.generateSessionTokenForUser(httpEntity).join();

    // Verify bad request status
    assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
  }

  @Test
  public void testGenerateSessionTokenForUserEligibilityDenied() throws Exception {
    String userId = "disabledUser";
    Authentication systemAuth = mock(Authentication.class);
    Actor systemActor = new Actor(ActorType.USER, SYSTEM_CLIENT_ID);
    when(systemAuth.getActor()).thenReturn(systemActor);
    AuthenticationContext.setAuthentication(systemAuth);

    when(mockUserSessionEligibilityChecker.checkEligibility(
            eq(systemOperationContext), eq(userId), anyBoolean()))
        .thenReturn(Optional.of(LoginDenialReason.SOFT_DELETED));

    AuthenticationConfiguration authConfig = new AuthenticationConfiguration();
    authConfig.setVerboseAuthFailureLogging(true);
    when(mockConfigProvider.getAuthentication()).thenReturn(authConfig);

    ObjectNode requestBody = objectMapper.createObjectNode();
    requestBody.put("userId", userId);
    HttpEntity<String> httpEntity = new HttpEntity<>(objectMapper.writeValueAsString(requestBody));

    ResponseEntity<String> response =
        authServiceController.generateSessionTokenForUser(httpEntity).join();

    assertEquals(HttpStatus.FORBIDDEN, response.getStatusCode());
    JsonNode responseJson = objectMapper.readTree(response.getBody());
    assertEquals(
        LoginDenialReason.SOFT_DELETED.name(), responseJson.get("loginDenialReason").asText());
    verify(mockTokenService, never())
        .generateAccessToken(eq(TokenType.SESSION), any(Actor.class), anyLong());
    assertEquals(
        1.0,
        authSessionOutcomeCount(
            MetricUtils.AUTH_OPERATION_GENERATE_SESSION_TOKEN,
            MetricUtils.AUTH_RESULT_FAILURE,
            "unknown",
            LoginDenialReason.SOFT_DELETED.name()));
  }

  @Test
  public void testVerifyNativeUserCredentialsEligibleDenied() throws Exception {
    String userUrn = "urn:li:corpuser:testUser";
    String password = "correctPassword";

    when(mockNativeUserService.doesPasswordMatch(
            eq(systemOperationContext), eq(userUrn), eq(password)))
        .thenReturn(true);
    when(mockUserSessionEligibilityChecker.checkEligibility(
            eq(systemOperationContext), eq(userUrn), anyBoolean()))
        .thenReturn(Optional.of(LoginDenialReason.INACTIVE));

    AuthenticationConfiguration authConfig = new AuthenticationConfiguration();
    when(mockConfigProvider.getAuthentication()).thenReturn(authConfig);

    ObjectNode requestBody = objectMapper.createObjectNode();
    requestBody.put("userUrn", userUrn);
    requestBody.put("password", password);
    HttpEntity<String> httpEntity = new HttpEntity<>(objectMapper.writeValueAsString(requestBody));

    ResponseEntity<String> response =
        authServiceController.verifyNativeUserCredentials(httpEntity).join();

    assertEquals(HttpStatus.OK, response.getStatusCode());
    JsonNode responseJson = objectMapper.readTree(response.getBody());
    assertTrue(responseJson.get("doesPasswordMatch").asBoolean());
    assertEquals(LoginDenialReason.INACTIVE.name(), responseJson.get("loginDenialReason").asText());
    assertEquals(
        1.0,
        authSessionOutcomeCount(
            MetricUtils.AUTH_OPERATION_VERIFY_NATIVE_USER_CREDENTIALS,
            MetricUtils.AUTH_RESULT_FAILURE,
            LoginSource.PASSWORD_LOGIN.getSource(),
            LoginDenialReason.INACTIVE.name()));
  }

  @Test
  public void testGenerateSessionTokenForUserEligibilityHardDeleted() throws Exception {
    String userId = "missingKeyUser";
    Authentication systemAuth = mock(Authentication.class);
    Actor systemActor = new Actor(ActorType.USER, SYSTEM_CLIENT_ID);
    when(systemAuth.getActor()).thenReturn(systemActor);
    AuthenticationContext.setAuthentication(systemAuth);

    when(mockUserSessionEligibilityChecker.checkEligibility(
            eq(systemOperationContext), eq(userId), anyBoolean()))
        .thenReturn(Optional.of(LoginDenialReason.HARD_DELETED));

    when(mockConfigProvider.getAuthentication()).thenReturn(new AuthenticationConfiguration());

    ObjectNode requestBody = objectMapper.createObjectNode();
    requestBody.put("userId", userId);
    HttpEntity<String> httpEntity = new HttpEntity<>(objectMapper.writeValueAsString(requestBody));

    ResponseEntity<String> response =
        authServiceController.generateSessionTokenForUser(httpEntity).join();

    assertEquals(HttpStatus.FORBIDDEN, response.getStatusCode());
    JsonNode responseJson = objectMapper.readTree(response.getBody());
    assertEquals(
        LoginDenialReason.HARD_DELETED.name(), responseJson.get("loginDenialReason").asText());
    verify(mockTokenService, never())
        .generateAccessToken(eq(TokenType.SESSION), any(Actor.class), anyLong());
  }

  @Test
  public void testGenerateSessionTokenForUserEligibilityNotProvisioned() throws Exception {
    String userId = "stubUser";
    Authentication systemAuth = mock(Authentication.class);
    Actor systemActor = new Actor(ActorType.USER, SYSTEM_CLIENT_ID);
    when(systemAuth.getActor()).thenReturn(systemActor);
    AuthenticationContext.setAuthentication(systemAuth);

    when(mockUserSessionEligibilityChecker.checkEligibility(
            eq(systemOperationContext), eq(userId), anyBoolean()))
        .thenReturn(Optional.of(LoginDenialReason.NOT_PROVISIONED));

    when(mockConfigProvider.getAuthentication()).thenReturn(new AuthenticationConfiguration());

    ObjectNode requestBody = objectMapper.createObjectNode();
    requestBody.put("userId", userId);
    HttpEntity<String> httpEntity = new HttpEntity<>(objectMapper.writeValueAsString(requestBody));

    ResponseEntity<String> response =
        authServiceController.generateSessionTokenForUser(httpEntity).join();

    assertEquals(HttpStatus.FORBIDDEN, response.getStatusCode());
    JsonNode responseJson = objectMapper.readTree(response.getBody());
    assertEquals(
        LoginDenialReason.NOT_PROVISIONED.name(), responseJson.get("loginDenialReason").asText());
  }

  @Test
  public void testVerifyNativeUserCredentialsNotProvisioned() throws Exception {
    String userUrn = "urn:li:corpuser:stub";
    String password = "correctPassword";

    when(mockNativeUserService.doesPasswordMatch(
            eq(systemOperationContext), eq(userUrn), eq(password)))
        .thenReturn(true);
    when(mockUserSessionEligibilityChecker.checkEligibility(
            eq(systemOperationContext), eq(userUrn), anyBoolean()))
        .thenReturn(Optional.of(LoginDenialReason.NOT_PROVISIONED));

    when(mockConfigProvider.getAuthentication()).thenReturn(new AuthenticationConfiguration());

    ObjectNode requestBody = objectMapper.createObjectNode();
    requestBody.put("userUrn", userUrn);
    requestBody.put("password", password);
    HttpEntity<String> httpEntity = new HttpEntity<>(objectMapper.writeValueAsString(requestBody));

    ResponseEntity<String> response =
        authServiceController.verifyNativeUserCredentials(httpEntity).join();

    assertEquals(HttpStatus.OK, response.getStatusCode());
    JsonNode responseJson = objectMapper.readTree(response.getBody());
    assertTrue(responseJson.get("doesPasswordMatch").asBoolean());
    assertEquals(
        LoginDenialReason.NOT_PROVISIONED.name(), responseJson.get("loginDenialReason").asText());
  }

  @Test
  public void testSignUpSuccess() throws Exception {
    // Setup
    String userUrn = "urn:li:corpuser:testUser";
    String fullName = "Test User";
    String email = "test@example.com";
    String title = "Software Engineer";
    String password = "securePassword123";
    String inviteToken = "valid-invite-token";
    Urn inviteTokenUrn = mock(Urn.class);

    // Mock invite token service
    when(mockInviteTokenService.getInviteTokenUrn(inviteToken)).thenReturn(inviteTokenUrn);
    when(mockInviteTokenService.isInviteTokenValid(eq(systemOperationContext), eq(inviteTokenUrn)))
        .thenReturn(true);

    // Create request body
    ObjectNode requestBody = objectMapper.createObjectNode();
    requestBody.put("userUrn", userUrn);
    requestBody.put("fullName", fullName);
    requestBody.put("email", email);
    requestBody.put("title", title);
    requestBody.put("password", password);
    requestBody.put("inviteToken", inviteToken);
    HttpEntity<String> httpEntity = new HttpEntity<>(objectMapper.writeValueAsString(requestBody));

    AuthenticationConfiguration authenticationConfiguration = new AuthenticationConfiguration();
    authenticationConfiguration.setSystemClientId(SYSTEM_CLIENT_ID);
    when(mockConfigProvider.getAuthentication()).thenReturn(authenticationConfiguration);

    // Execute
    ResponseEntity<String> response = authServiceController.signUp(httpEntity).join();

    // Verify
    assertEquals(HttpStatus.OK, response.getStatusCode());
    JsonNode responseJson = objectMapper.readTree(response.getBody());
    assertTrue(responseJson.has("isNativeUserCreated"));
    assertTrue(responseJson.get("isNativeUserCreated").asBoolean());

    // Verify native user service was called with correct parameters
    verify(mockNativeUserService)
        .createNativeUser(
            eq(systemOperationContext),
            eq(userUrn),
            eq(fullName),
            eq(email),
            eq(title),
            eq(password));
  }

  @Test
  public void testSignUpWithInvalidInviteToken() throws Exception {
    // Setup
    String userUrn = "urn:li:corpuser:testUser";
    String fullName = "Test User";
    String email = "test@example.com";
    String title = "Software Engineer";
    String password = "securePassword123";
    String inviteToken = "invalid-invite-token";
    Urn inviteTokenUrn = mock(Urn.class);

    // Mock invite token service to return invalid token
    when(mockInviteTokenService.getInviteTokenUrn(inviteToken)).thenReturn(inviteTokenUrn);
    when(mockInviteTokenService.isInviteTokenValid(eq(systemOperationContext), eq(inviteTokenUrn)))
        .thenReturn(false);

    // Create request body
    ObjectNode requestBody = objectMapper.createObjectNode();
    requestBody.put("userUrn", userUrn);
    requestBody.put("fullName", fullName);
    requestBody.put("email", email);
    requestBody.put("title", title);
    requestBody.put("password", password);
    requestBody.put("inviteToken", inviteToken);
    HttpEntity<String> httpEntity = new HttpEntity<>(objectMapper.writeValueAsString(requestBody));

    AuthenticationConfiguration authenticationConfiguration = new AuthenticationConfiguration();
    authenticationConfiguration.setSystemClientId(SYSTEM_CLIENT_ID);
    when(mockConfigProvider.getAuthentication()).thenReturn(authenticationConfiguration);

    // Execute
    ResponseEntity<String> response = authServiceController.signUp(httpEntity).join();

    // Verify
    assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
  }

  @Test
  public void testSignUpWithoutTitle() throws Exception {
    // Setup - no title provided
    String userUrn = "urn:li:corpuser:testUser";
    String fullName = "Test User";
    String email = "test@example.com";
    String password = "securePassword123";
    String inviteToken = "valid-invite-token";
    Urn inviteTokenUrn = mock(Urn.class);

    // Mock invite token service
    when(mockInviteTokenService.getInviteTokenUrn(inviteToken)).thenReturn(inviteTokenUrn);
    when(mockInviteTokenService.isInviteTokenValid(eq(systemOperationContext), eq(inviteTokenUrn)))
        .thenReturn(true);

    // Create request body without title field
    ObjectNode requestBody = objectMapper.createObjectNode();
    requestBody.put("userUrn", userUrn);
    requestBody.put("fullName", fullName);
    requestBody.put("email", email);
    // Note: title is intentionally omitted
    requestBody.put("password", password);
    requestBody.put("inviteToken", inviteToken);
    HttpEntity<String> httpEntity = new HttpEntity<>(objectMapper.writeValueAsString(requestBody));

    AuthenticationConfiguration authenticationConfiguration = new AuthenticationConfiguration();
    authenticationConfiguration.setSystemClientId(SYSTEM_CLIENT_ID);
    when(mockConfigProvider.getAuthentication()).thenReturn(authenticationConfiguration);

    // Execute
    ResponseEntity<String> response = authServiceController.signUp(httpEntity).join();

    // Verify
    assertEquals(HttpStatus.OK, response.getStatusCode());
    JsonNode responseJson = objectMapper.readTree(response.getBody());
    assertTrue(responseJson.has("isNativeUserCreated"));
    assertTrue(responseJson.get("isNativeUserCreated").asBoolean());

    // Verify native user service was called with null title
    verify(mockNativeUserService)
        .createNativeUser(
            eq(systemOperationContext),
            eq(userUrn),
            eq(fullName),
            eq(email),
            isNull(),
            eq(password));
  }

  @Test
  public void testVerifyNativeUserCredentialsSuccess() throws Exception {
    // Setup
    String userUrn = "urn:li:corpuser:testUser";
    String password = "correctPassword";

    // Mock password verification
    when(mockNativeUserService.doesPasswordMatch(
            eq(systemOperationContext), eq(userUrn), eq(password)))
        .thenReturn(true);

    when(mockConfigProvider.getAuthentication()).thenReturn(new AuthenticationConfiguration());

    // Create request body
    ObjectNode requestBody = objectMapper.createObjectNode();
    requestBody.put("userUrn", userUrn);
    requestBody.put("password", password);
    HttpEntity<String> httpEntity = new HttpEntity<>(objectMapper.writeValueAsString(requestBody));

    // Execute
    ResponseEntity<String> response =
        authServiceController.verifyNativeUserCredentials(httpEntity).join();

    // Verify
    assertEquals(HttpStatus.OK, response.getStatusCode());
    JsonNode responseJson = objectMapper.readTree(response.getBody());
    assertTrue(responseJson.has("doesPasswordMatch"));
    assertTrue(responseJson.get("doesPasswordMatch").asBoolean());
    assertEquals(
        1.0,
        authSessionOutcomeCount(
            MetricUtils.AUTH_OPERATION_VERIFY_NATIVE_USER_CREDENTIALS,
            MetricUtils.AUTH_RESULT_SUCCESS,
            LoginSource.PASSWORD_LOGIN.getSource(),
            MetricUtils.AUTH_DENIAL_REASON_NONE));
  }

  @Test
  public void testVerifyNativeUserCredentialsFailure() throws Exception {
    // Setup
    String userUrn = "urn:li:corpuser:testUser";
    String password = "incorrectPassword";

    // Mock password verification
    when(mockNativeUserService.doesPasswordMatch(
            eq(systemOperationContext), eq(userUrn), eq(password)))
        .thenReturn(false);

    // Create request body
    ObjectNode requestBody = objectMapper.createObjectNode();
    requestBody.put("userUrn", userUrn);
    requestBody.put("password", password);
    HttpEntity<String> httpEntity = new HttpEntity<>(objectMapper.writeValueAsString(requestBody));

    when(mockConfigProvider.getAuthentication()).thenReturn(new AuthenticationConfiguration());

    // Execute
    ResponseEntity<String> response =
        authServiceController.verifyNativeUserCredentials(httpEntity).join();

    // Verify
    assertEquals(HttpStatus.OK, response.getStatusCode());
    JsonNode responseJson = objectMapper.readTree(response.getBody());
    assertTrue(responseJson.has("doesPasswordMatch"));
    assertFalse(responseJson.get("doesPasswordMatch").asBoolean());
    assertTrue(responseJson.has("loginDenialReason"));
    assertEquals(
        LoginDenialReason.INVALID_CREDENTIALS.name(),
        responseJson.get("loginDenialReason").asText());
    assertEquals(
        1.0,
        authSessionOutcomeCount(
            MetricUtils.AUTH_OPERATION_VERIFY_NATIVE_USER_CREDENTIALS,
            MetricUtils.AUTH_RESULT_FAILURE,
            LoginSource.PASSWORD_LOGIN.getSource(),
            LoginDenialReason.INVALID_CREDENTIALS.name()));
  }

  @Test
  public void testResetNativeUserCredentialsSuccess() throws Exception {
    // Setup
    String userUrn = "urn:li:corpuser:testUser";
    String password = "newPassword123";
    String resetToken = "valid-reset-token";

    // Create request body
    ObjectNode requestBody = objectMapper.createObjectNode();
    requestBody.put("userUrn", userUrn);
    requestBody.put("password", password);
    requestBody.put("resetToken", resetToken);
    HttpEntity<String> httpEntity = new HttpEntity<>(objectMapper.writeValueAsString(requestBody));

    // Execute
    ResponseEntity<String> response =
        authServiceController.resetNativeUserCredentials(httpEntity).join();

    // Verify
    assertEquals(HttpStatus.OK, response.getStatusCode());
    JsonNode responseJson = objectMapper.readTree(response.getBody());
    assertTrue(responseJson.has("areNativeUserCredentialsReset"));
    assertTrue(responseJson.get("areNativeUserCredentialsReset").asBoolean());

    // Verify native user service was called with correct parameters
    verify(mockNativeUserService)
        .resetCorpUserCredentials(
            eq(systemOperationContext), eq(userUrn), eq(password), eq(resetToken));
  }

  @Test
  public void testGetSsoSettingsNotFound() throws Exception {
    // Mock entity service to return null (no SSO settings available)
    when(mockEntityService.getLatestAspect(
            any(OperationContext.class),
            eq(GLOBAL_SETTINGS_URN),
            eq(GLOBAL_SETTINGS_INFO_ASPECT_NAME)))
        .thenReturn(null);

    // Execute
    ResponseEntity<String> response = authServiceController.getSsoSettings(null).join();

    // Verify
    assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
  }

  @Test
  public void testGetSsoSettingsWithoutOidc() throws IOException {
    // Setup SSO settings without OIDC
    SsoSettings mockSsoSettings = new SsoSettings().setBaseUrl("http://localhost");
    GlobalSettingsInfo mockGlobalSettingsInfo = new GlobalSettingsInfo().setSso(mockSsoSettings);

    when(mockEntityService.getLatestAspect(
            any(OperationContext.class),
            eq(GLOBAL_SETTINGS_URN),
            eq(GLOBAL_SETTINGS_INFO_ASPECT_NAME)))
        .thenReturn(mockGlobalSettingsInfo);

    when(mockSecretService.decrypt(any())).thenReturn("decrypted-secret");

    // Execute
    ResponseEntity<String> httpResponse = authServiceController.getSsoSettings(null).join();

    // Verify
    assertEquals(httpResponse.getStatusCode(), HttpStatus.OK);
    JsonNode jsonNode = new ObjectMapper().readTree(httpResponse.getBody());
    assertTrue(jsonNode.has("baseUrl"));
    assertEquals("http://localhost", jsonNode.get("baseUrl").asText());
    assertFalse(jsonNode.has("oidcEnabled"));
  }

  @Test
  public void testGetSsoSettingsWithFullOidcConfiguration() throws IOException {
    // Setup complete OIDC settings
    OidcSettings mockOidcSettings =
        new OidcSettings()
            .setEnabled(true)
            .setClientId("client123")
            .setClientSecret("encrypted-secret")
            .setDiscoveryUri("http://auth.example.com")
            .setUserNameClaim("preferred_username")
            .setUserNameClaimRegex(".*")
            .setScope("openid profile email")
            .setClientAuthenticationMethod("client_secret_basic")
            .setJitProvisioningEnabled(true)
            .setPreProvisioningRequired(false)
            .setExtractGroupsEnabled(true)
            .setGroupsClaim("groups")
            .setResponseType("code")
            .setResponseMode("query")
            .setUseNonce(true)
            .setReadTimeout(30000)
            .setExtractJwtAccessTokenClaims(true)
            .setPreferredJwsAlgorithm2("RS256");

    SsoSettings mockSsoSettings =
        new SsoSettings().setBaseUrl("http://localhost").setOidcSettings(mockOidcSettings);

    GlobalSettingsInfo mockGlobalSettingsInfo = new GlobalSettingsInfo().setSso(mockSsoSettings);

    when(mockEntityService.getLatestAspect(
            any(OperationContext.class),
            eq(GLOBAL_SETTINGS_URN),
            eq(GLOBAL_SETTINGS_INFO_ASPECT_NAME)))
        .thenReturn(mockGlobalSettingsInfo);

    when(mockSecretService.decrypt("encrypted-secret")).thenReturn("decrypted-secret");

    // Execute
    ResponseEntity<String> httpResponse = authServiceController.getSsoSettings(null).join();

    // Verify
    assertEquals(httpResponse.getStatusCode(), HttpStatus.OK);
    JsonNode jsonNode = new ObjectMapper().readTree(httpResponse.getBody());

    // Check all fields are present and have correct values
    assertEquals("http://localhost", jsonNode.get("baseUrl").asText());
    assertTrue(jsonNode.get("oidcEnabled").asBoolean());
    assertEquals("client123", jsonNode.get("clientId").asText());
    assertEquals("decrypted-secret", jsonNode.get("clientSecret").asText());
    assertEquals("http://auth.example.com", jsonNode.get("discoveryUri").asText());
    assertEquals("preferred_username", jsonNode.get("userNameClaim").asText());
    assertEquals(".*", jsonNode.get("userNameClaimRegex").asText());
    assertEquals("openid profile email", jsonNode.get("scope").asText());
    assertEquals("client_secret_basic", jsonNode.get("clientAuthenticationMethod").asText());
    assertTrue(jsonNode.get("jitProvisioningEnabled").asBoolean());
    assertFalse(jsonNode.get("preProvisioningRequired").asBoolean());
    assertTrue(jsonNode.get("extractGroupsEnabled").asBoolean());
    assertEquals("groups", jsonNode.get("groupsClaim").asText());
    assertEquals("code", jsonNode.get("responseType").asText());
    assertEquals("query", jsonNode.get("responseMode").asText());
    assertTrue(jsonNode.get("useNonce").asBoolean());
    assertEquals(30000, jsonNode.get("readTimeout").asInt());
    assertTrue(jsonNode.get("extractJwtAccessTokenClaims").asBoolean());
    assertEquals("RS256", jsonNode.get("preferredJwsAlgorithm").asText());
  }

  /*
  @Test
  public void testTrackSuccess() throws Exception {
    // Setup
    String eventPayload = "{\"type\":\"page_view\",\"properties\":{\"page\":\"dashboard\"}}";
    HttpEntity<String> httpEntity = new HttpEntity<>(eventPayload);

    // Mock tracking service (already @Autowired in the test class)
    // No need to configure behavior as the method doesn't return anything

    // Execute
    ResponseEntity<String> response = authServiceController.track(httpEntity).join();

    // Verify
    assertEquals(HttpStatus.OK, response.getStatusCode());

    // Verify tracking service was called with correct parameters
    ArgumentCaptor<JsonNode> jsonCaptor = ArgumentCaptor.forClass(JsonNode.class);
    verify(mockTrackingService)
        .track(
            eq("page_view"),
            eq(systemOperationContext),
            isNull(),
            isNull(),
            jsonCaptor.capture(),
            any());

    JsonNode capturedJson = jsonCaptor.getValue();
    assertTrue(capturedJson.has("type"));
    assertEquals("page_view", capturedJson.get("type").asText());
    assertTrue(capturedJson.has("properties"));
    assertTrue(capturedJson.get("properties").has("page"));
    assertEquals("dashboard", capturedJson.get("properties").get("page").asText());
  }

  @Test
  public void testTrackBadRequest() throws Exception {
    // Setup - invalid JSON
    String invalidJson = "{malformed json";
    HttpEntity<String> httpEntity = new HttpEntity<>(invalidJson);

    // Execute
    ResponseEntity<String> response = authServiceController.track(httpEntity).join();

    // Verify
    assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());

    // Verify tracking service was not called
    verify(mockTrackingService, never()).track(any(), any(), any(), any(), any(), any());
  }

  @Test
  public void testTrackMissingRequiredFields() throws Exception {
    // Setup - missing type fields
    String missingFieldsJson = "{\"user\":\"testUser\"}";
    HttpEntity<String> httpEntity = new HttpEntity<>(missingFieldsJson);

    // Execute
    ResponseEntity<String> response = authServiceController.track(httpEntity).join();

    // Verify
    assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());

    // Verify tracking service was not called
    verify(mockTrackingService, never()).track(any(), any(), any(), any(), any(), any());
  }

  @Test
  public void testTrackServiceException() throws Exception {
    // Setup
    String eventPayload = "{\"type\":\"error_event\"}";
    HttpEntity<String> httpEntity = new HttpEntity<>(eventPayload);

    // Mock tracking service to throw exception
    doThrow(new RuntimeException("Test exception"))
        .when(mockTrackingService)
        .track(
            eq("error_event"),
            eq(systemOperationContext),
            any(),
            any(),
            any(JsonNode.class),
            any());

    // Execute
    ResponseEntity<String> response = authServiceController.track(httpEntity).join();

    // Verify
    assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
  }

  @Test
  public void testTrackWithComplexPageViewEventPayload() throws Exception {
    // Setup
    String complexPayload =
        "{\n"
            + "  \"title\" : \"DataHub\",\n"
            + "  \"url\" : \"http://localhost:9002/\",\n"
            + "  \"path\" : \"/\",\n"
            + "  \"hash\" : \"\",\n"
            + "  \"search\" : \"\",\n"
            + "  \"width\" : 1785,\n"
            + "  \"height\" : 857,\n"
            + "  \"referrer\" : \"http://localhost:9002/\",\n"
            + "  \"prevPathname\" : \"http://localhost:9002/\",\n"
            + "  \"type\" : \"PageViewEvent\",\n"
            + "  \"actorUrn\" : \"urn:li:corpuser:datahub\",\n"
            + "  \"timestamp\" : 1746475429127,\n"
            + "  \"date\" : \"Mon May 05 2025 15:03:49 GMT-0500 (Central Daylight Time)\",\n"
            + "  \"userAgent\" : \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36\",\n"
            + "  \"browserId\" : \"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx\",\n"
            + "  \"origin\" : \"http://localhost:9002\",\n"
            + "  \"isThemeV2Enabled\" : true,\n"
            + "  \"userPersona\" : \"urn:li:dataHubPersona:businessUser\",\n"
            + "  \"serverVersion\" : \"v1.1.0\"\n"
            + "}";

    HttpEntity<String> httpEntity = new HttpEntity<>(complexPayload);

    // Execute
    ResponseEntity<String> response = authServiceController.track(httpEntity).join();

    // Verify response status
    assertEquals(HttpStatus.OK, response.getStatusCode());

    // Verify tracking service was called with correct parameters
    ArgumentCaptor<JsonNode> jsonCaptor = ArgumentCaptor.forClass(JsonNode.class);
    verify(mockTrackingService)
        .track(
            eq("PageViewEvent"),
            eq(systemOperationContext),
            any(),
            any(),
            jsonCaptor.capture(),
            any());

    // Verify the complex JSON structure was correctly parsed and passed to the tracking service
    JsonNode capturedJson = jsonCaptor.getValue();

    // Verify key fields from the complex payload
    assertEquals("DataHub", capturedJson.get("title").asText());
    assertEquals("http://localhost:9002/", capturedJson.get("url").asText());
    assertEquals("PageViewEvent", capturedJson.get("type").asText());
    assertEquals("urn:li:corpuser:datahub", capturedJson.get("actorUrn").asText());
    assertEquals(1746475429127L, capturedJson.get("timestamp").asLong());
    assertEquals("xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx", capturedJson.get("browserId").asText());
    assertEquals("urn:li:dataHubPersona:businessUser", capturedJson.get("userPersona").asText());
    assertEquals("v1.1.0", capturedJson.get("serverVersion").asText());
    assertTrue(capturedJson.get("isThemeV2Enabled").asBoolean());
  }

   */
}
