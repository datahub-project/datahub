package client;

import static com.linkedin.metadata.Constants.DATAHUB_LOGIN_SOURCE_HEADER_NAME;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.LoginDenialReason;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.metadata.auth.LoginIdentityMask;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import play.mvc.Http;

/** This class is responsible for coordinating authentication with the backend Metadata Service. */
@Slf4j
public class AuthServiceClient {

  private static final String GENERATE_SESSION_TOKEN_ENDPOINT = "auth/generateSessionTokenForUser";
  private static final String SIGN_UP_ENDPOINT = "auth/signUp";
  private static final String RESET_NATIVE_USER_CREDENTIALS_ENDPOINT =
      "auth/resetNativeUserCredentials";
  private static final String VERIFY_NATIVE_USER_CREDENTIALS_ENDPOINT =
      "auth/verifyNativeUserCredentials";
  private static final String TRACK_ENDPOINT = "auth/track";
  private static final String ACCESS_TOKEN_FIELD = "accessToken";
  private static final String USER_ID_FIELD = "userId";
  private static final String USER_URN_FIELD = "userUrn";
  private static final String FULL_NAME_FIELD = "fullName";
  private static final String EMAIL_FIELD = "email";
  private static final String TITLE_FIELD = "title";
  private static final String PASSWORD_FIELD = "password";
  private static final String INVITE_TOKEN_FIELD = "inviteToken";
  private static final String RESET_TOKEN_FIELD = "resetToken";
  private static final String IS_NATIVE_USER_CREATED_FIELD = "isNativeUserCreated";
  private static final String ARE_NATIVE_USER_CREDENTIALS_RESET_FIELD =
      "areNativeUserCredentialsReset";
  private static final String DOES_PASSWORD_MATCH_FIELD = "doesPasswordMatch";
  private static final String LOGIN_DENIAL_REASON_FIELD = "loginDenialReason";

  private final String metadataServiceHost;
  private final Integer metadataServicePort;
  private final String metadataServiceBasePath;
  private final Boolean metadataServiceUseSsl;
  private final Authentication systemAuthentication;
  private final CloseableHttpClient httpClient;
  private final boolean authVerboseLogging;

  /** Not {@code @Inject} — constructed by {@code AuthModule#provideAuthClient} (config-backed). */
  public AuthServiceClient(
      @Nonnull final String metadataServiceHost,
      @Nonnull final Integer metadataServicePort,
      @Nonnull final String metadataServiceBasePath,
      @Nonnull final Boolean useSsl,
      @Nonnull final Authentication systemAuthentication,
      @Nonnull final CloseableHttpClient httpClient,
      final boolean authVerboseLogging) {
    this.metadataServiceHost = Objects.requireNonNull(metadataServiceHost);
    this.metadataServicePort = Objects.requireNonNull(metadataServicePort);
    this.metadataServiceBasePath = Objects.requireNonNull(metadataServiceBasePath);
    this.metadataServiceUseSsl = Objects.requireNonNull(useSsl);
    this.systemAuthentication = Objects.requireNonNull(systemAuthentication);
    this.httpClient = Objects.requireNonNull(httpClient);
    this.authVerboseLogging = authVerboseLogging;
  }

  /**
   * Call the Auth Service to generate a session token for a particular user with a unique actor id,
   * or throws an exception if generation fails.
   *
   * <p>Notice that the "userId" parameter should NOT be of type "urn", but rather the unique id of
   * an Actor of type USER.
   */
  @Nonnull
  public String generateSessionTokenForUser(@Nonnull final String userId, String loginSource) {
    Objects.requireNonNull(userId, "userId must not be null");
    CloseableHttpResponse response = null;

    try {
      final String protocol = this.metadataServiceUseSsl ? "https" : "http";
      final HttpPost request =
          new HttpPost(
              String.format(
                  "%s://%s:%s%s/%s",
                  protocol,
                  this.metadataServiceHost,
                  this.metadataServicePort,
                  this.metadataServiceBasePath,
                  GENERATE_SESSION_TOKEN_ENDPOINT));

      log.info("Requesting session token for userRef={}", LoginIdentityMask.mask(userId));

      // Build JSON request to generate a token on behalf of a user.
      final ObjectMapper objectMapper = new ObjectMapper();
      final ObjectNode objectNode = objectMapper.createObjectNode();
      objectNode.put(USER_ID_FIELD, userId);
      final String json =
          objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectNode);
      request.setEntity(new StringEntity(json, StandardCharsets.UTF_8));

      // Add authorization header with DataHub frontend system id and secret.
      request.addHeader(Http.HeaderNames.AUTHORIZATION, this.systemAuthentication.getCredentials());

      request.addHeader(DATAHUB_LOGIN_SOURCE_HEADER_NAME, loginSource);

      response = httpClient.execute(request);
      final HttpEntity entity = response.getEntity();
      final int code = response.getStatusLine().getStatusCode();
      if (code == HttpStatus.SC_OK && entity != null) {
        log.info(
            "Successfully received session token for userRef={}", LoginIdentityMask.mask(userId));
        final String jsonStr = EntityUtils.toString(entity);
        return getAccessTokenFromJson(jsonStr);
      }
      if (code == HttpStatus.SC_FORBIDDEN && entity != null) {
        final String jsonStr = EntityUtils.toString(entity);
        final LoginDenialReason denial = parseLoginDenialReasonFromJson(jsonStr);
        emitFrontendLoginDeniedLog(userId, denial, "generateSessionTokenForUser");
        throw new SessionTokenDeniedException(denial, null);
      }
      throw new RuntimeException(
          String.format(
              "Bad response from the Metadata Service: %s %s",
              response.getStatusLine().toString(), response.getEntity().toString()));
    } catch (SessionTokenDeniedException e) {
      throw e;
    } catch (Exception e) {
      log.error(
          "Failed to generate session token for userRef: {}", LoginIdentityMask.mask(userId), e);
      throw new RuntimeException("Failed to generate session token for user", e);
    } finally {
      try {
        if (response != null) {
          response.close();
        }
      } catch (Exception e) {
        log.error("Failed to close http response", e);
      }
    }
  }

  /** Call the Auth Service to create a native Datahub user. */
  public boolean signUp(
      @Nonnull final String userUrn,
      @Nonnull final String fullName,
      @Nonnull final String email,
      final String title,
      @Nonnull final String password,
      @Nonnull final String inviteToken) {
    Objects.requireNonNull(userUrn, "userUrn must not be null");
    Objects.requireNonNull(fullName, "fullName must not be null");
    Objects.requireNonNull(email, "email must not be null");
    Objects.requireNonNull(password, "password must not be null");
    Objects.requireNonNull(inviteToken, "inviteToken must not be null");
    CloseableHttpResponse response = null;

    try {

      final String protocol = this.metadataServiceUseSsl ? "https" : "http";
      final HttpPost request =
          new HttpPost(
              String.format(
                  "%s://%s:%s%s/%s",
                  protocol,
                  this.metadataServiceHost,
                  this.metadataServicePort,
                  this.metadataServiceBasePath,
                  SIGN_UP_ENDPOINT));

      // Build JSON request to sign up a native user.
      final ObjectMapper objectMapper = new ObjectMapper();
      final ObjectNode objectNode = objectMapper.createObjectNode();
      objectNode.put(USER_URN_FIELD, userUrn);
      objectNode.put(FULL_NAME_FIELD, fullName);
      objectNode.put(EMAIL_FIELD, email);
      if (title != null) {
        objectNode.put(TITLE_FIELD, title);
      }
      objectNode.put(PASSWORD_FIELD, password);
      objectNode.put(INVITE_TOKEN_FIELD, inviteToken);
      final String json =
          objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectNode);
      request.setEntity(new StringEntity(json, StandardCharsets.UTF_8));

      // Add authorization header with DataHub frontend system id and secret.
      request.addHeader(Http.HeaderNames.AUTHORIZATION, this.systemAuthentication.getCredentials());

      response = httpClient.execute(request);
      final HttpEntity entity = response.getEntity();
      if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK && entity != null) {
        // Successfully generated a token for the User
        final String jsonStr = EntityUtils.toString(entity);
        return getIsNativeUserCreatedFromJson(jsonStr);
      } else {
        String content =
            response.getEntity().getContent() == null
                ? ""
                : new String(
                    response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
        throw new RuntimeException(
            String.format(
                "Bad response from the Metadata Service: %s %s Body: %s",
                response.getStatusLine().toString(), response.getEntity().toString(), content));
      }
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to create user %s", userUrn), e);
    } finally {
      try {
        if (response != null) {
          response.close();
        }
      } catch (Exception e) {
        log.error("Failed to close http response", e);
      }
    }
  }

  /** Call the Auth Service to reset credentials for a native DataHub user. */
  public boolean resetNativeUserCredentials(
      @Nonnull final String userUrn,
      @Nonnull final String password,
      @Nonnull final String resetToken) {
    Objects.requireNonNull(userUrn, "userUrn must not be null");
    Objects.requireNonNull(password, "password must not be null");
    Objects.requireNonNull(resetToken, "reset token must not be null");
    CloseableHttpResponse response = null;

    try {

      final String protocol = this.metadataServiceUseSsl ? "https" : "http";
      final HttpPost request =
          new HttpPost(
              String.format(
                  "%s://%s:%s%s/%s",
                  protocol,
                  this.metadataServiceHost,
                  this.metadataServicePort,
                  this.metadataServiceBasePath,
                  RESET_NATIVE_USER_CREDENTIALS_ENDPOINT));

      // Build JSON request to verify credentials for a native user.
      final ObjectMapper objectMapper = new ObjectMapper();
      final ObjectNode objectNode = objectMapper.createObjectNode();
      objectNode.put(USER_URN_FIELD, userUrn);
      objectNode.put(PASSWORD_FIELD, password);
      objectNode.put(RESET_TOKEN_FIELD, resetToken);
      final String json =
          objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectNode);
      request.setEntity(new StringEntity(json, StandardCharsets.UTF_8));

      // Add authorization header with DataHub frontend system id and secret.
      request.addHeader(Http.HeaderNames.AUTHORIZATION, this.systemAuthentication.getCredentials());

      response = httpClient.execute(request);
      final HttpEntity entity = response.getEntity();
      if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK && entity != null) {
        // Successfully generated a token for the User
        final String jsonStr = EntityUtils.toString(entity);
        return getAreNativeUserCredentialsResetFromJson(jsonStr);
      } else {
        throw new RuntimeException(
            String.format(
                "Bad response from the Metadata Service: %s %s",
                response.getStatusLine().toString(), response.getEntity().toString()));
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to reset credentials for user", e);
    } finally {
      try {
        if (response != null) {
          response.close();
        }
      } catch (Exception e) {
        log.error("Failed to close http response", e);
      }
    }
  }

  /** Call the Auth Service to verify the credentials for a native Datahub user. */
  @Nonnull
  public NativeUserCredentialVerifyResult verifyNativeUserCredentials(
      @Nonnull final String userUrn, @Nonnull final String password) {
    Objects.requireNonNull(userUrn, "userUrn must not be null");
    Objects.requireNonNull(password, "password must not be null");
    CloseableHttpResponse response = null;

    try {

      final String protocol = this.metadataServiceUseSsl ? "https" : "http";
      final HttpPost request =
          new HttpPost(
              String.format(
                  "%s://%s:%s%s/%s",
                  protocol,
                  this.metadataServiceHost,
                  this.metadataServicePort,
                  this.metadataServiceBasePath,
                  VERIFY_NATIVE_USER_CREDENTIALS_ENDPOINT));

      // Build JSON request to verify credentials for a native user.
      final ObjectMapper objectMapper = new ObjectMapper();
      final ObjectNode objectNode = objectMapper.createObjectNode();
      objectNode.put(USER_URN_FIELD, userUrn);
      objectNode.put(PASSWORD_FIELD, password);
      final String json =
          objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectNode);
      request.setEntity(new StringEntity(json, StandardCharsets.UTF_8));

      // Add authorization header with DataHub frontend system id and secret.
      request.addHeader(Http.HeaderNames.AUTHORIZATION, this.systemAuthentication.getCredentials());

      response = httpClient.execute(request);
      final HttpEntity entity = response.getEntity();
      if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK && entity != null) {
        final String jsonStr = EntityUtils.toString(entity);
        return parseVerifyNativeCredentialsResponse(jsonStr, userUrn);
      } else {
        throw new RuntimeException(
            String.format(
                "Bad response from the Metadata Service: %s %s",
                response.getStatusLine().toString(), response.getEntity().toString()));
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to verify credentials for user", e);
    } finally {
      try {
        if (response != null) {
          response.close();
        }
      } catch (Exception e) {
        log.error("Failed to close http response", e);
      }
    }
  }

  private void emitFrontendLoginDeniedLog(
      @Nonnull final String rawUserRef,
      @Nullable final LoginDenialReason loginDenialReason,
      @Nonnull final String operation) {
    final String masked = LoginIdentityMask.mask(rawUserRef);
    final boolean warn = loginDenialReason == null || loginDenialReason.logsAtWarn();
    if (loginDenialReason != null) {
      if (warn) {
        log.warn("loginDenied userRef={} loginDenialReason={}", masked, loginDenialReason.name());
        if (authVerboseLogging) {
          log.warn(
              "loginDenied userRef={} loginDenialReason={} operation={}",
              rawUserRef,
              loginDenialReason.name(),
              operation);
        }
      } else {
        log.info("loginDenied userRef={} loginDenialReason={}", masked, loginDenialReason.name());
        if (authVerboseLogging) {
          log.info(
              "loginDenied userRef={} loginDenialReason={} operation={}",
              rawUserRef,
              loginDenialReason.name(),
              operation);
        }
      }
    } else {
      log.warn(
          "loginDenied userRef={} event=auth_failure reason=missing_loginDenialReason", masked);
      if (authVerboseLogging) {
        log.warn(
            "loginDenied userRef={} event=auth_failure reason=missing_loginDenialReason operation={}",
            rawUserRef,
            operation);
      }
    }
  }

  @Nullable
  private static LoginDenialReason parseLoginDenialReasonName(@Nullable final String name) {
    if (name == null || name.isEmpty()) {
      return null;
    }
    try {
      return LoginDenialReason.valueOf(name);
    } catch (IllegalArgumentException e) {
      return LoginDenialReason.UNKNOWN;
    }
  }

  @Nullable
  private static LoginDenialReason parseLoginDenialReasonFromJson(final String jsonStr) {
    try {
      final JsonNode node = new ObjectMapper().readTree(jsonStr);
      if (node.has(LOGIN_DENIAL_REASON_FIELD) && !node.get(LOGIN_DENIAL_REASON_FIELD).isNull()) {
        return parseLoginDenialReasonName(node.get(LOGIN_DENIAL_REASON_FIELD).asText());
      }
    } catch (Exception ignored) {
    }
    return null;
  }

  @Nonnull
  private NativeUserCredentialVerifyResult parseVerifyNativeCredentialsResponse(
      final String jsonStr, final String userUrn) {
    try {
      final JsonNode node = new ObjectMapper().readTree(jsonStr);
      final boolean match = node.get(DOES_PASSWORD_MATCH_FIELD).asBoolean();
      LoginDenialReason denialReason = null;
      if (node.has(LOGIN_DENIAL_REASON_FIELD) && !node.get(LOGIN_DENIAL_REASON_FIELD).isNull()) {
        denialReason = parseLoginDenialReasonName(node.get(LOGIN_DENIAL_REASON_FIELD).asText());
      }
      if (!match || denialReason != null) {
        emitFrontendLoginDeniedLog(userUrn, denialReason, "verifyNativeUserCredentials");
      }
      return new NativeUserCredentialVerifyResult(match, denialReason);
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to parse JSON received from the MetadataService!");
    }
  }

  private String getAccessTokenFromJson(final String jsonStr) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.readTree(jsonStr).get(ACCESS_TOKEN_FIELD).asText();
    } catch (Exception e) {
      // Do not log the raw json in case it contains access token.
      throw new IllegalArgumentException("Failed to parse JSON received from the MetadataService!");
    }
  }

  private boolean getIsNativeUserCreatedFromJson(final String jsonStr) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.readTree(jsonStr).get(IS_NATIVE_USER_CREATED_FIELD).asBoolean();
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to parse JSON received from the MetadataService!");
    }
  }

  private boolean getAreNativeUserCredentialsResetFromJson(final String jsonStr) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.readTree(jsonStr).get(ARE_NATIVE_USER_CREDENTIALS_RESET_FIELD).asBoolean();
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to parse JSON received from the MetadataService!");
    }
  }
}
