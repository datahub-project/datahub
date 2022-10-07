package client;

import com.datahub.authentication.Authentication;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Objects;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import play.mvc.Http;


/**
 * This class is responsible for coordinating authentication with the backend Metadata Service.
 */
@Slf4j
public class AuthServiceClient {

  private static final String GENERATE_SESSION_TOKEN_ENDPOINT = "auth/generateSessionTokenForUser";
  private static final String SIGN_UP_ENDPOINT = "auth/signUp";
  private static final String RESET_NATIVE_USER_CREDENTIALS_ENDPOINT = "auth/resetNativeUserCredentials";
  private static final String VERIFY_NATIVE_USER_CREDENTIALS_ENDPOINT = "auth/verifyNativeUserCredentials";
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
  private static final String ARE_NATIVE_USER_CREDENTIALS_RESET_FIELD = "areNativeUserCredentialsReset";
  private static final String DOES_PASSWORD_MATCH_FIELD = "doesPasswordMatch";

  private final String metadataServiceHost;
  private final Integer metadataServicePort;
  private final Boolean metadataServiceUseSsl;
  private final Authentication systemAuthentication;
  private final CloseableHttpClient httpClient;

  public AuthServiceClient(@Nonnull final String metadataServiceHost, @Nonnull final Integer metadataServicePort,
      @Nonnull final Boolean useSsl, @Nonnull final Authentication systemAuthentication,
      @Nonnull final CloseableHttpClient httpClient) {
    this.metadataServiceHost = Objects.requireNonNull(metadataServiceHost);
    this.metadataServicePort = Objects.requireNonNull(metadataServicePort);
    this.metadataServiceUseSsl = Objects.requireNonNull(useSsl);
    this.systemAuthentication = Objects.requireNonNull(systemAuthentication);
    this.httpClient = Objects.requireNonNull(httpClient);
  }

  /**
   * Call the Auth Service to generate a session token for a particular user with a unique actor id, or throws an exception if generation fails.
   *
   * Notice that the "userId" parameter should NOT be of type "urn", but rather the unique id of an Actor of type
   * USER.
   */
  @Nonnull
  public String generateSessionTokenForUser(@Nonnull final String userId) {
    Objects.requireNonNull(userId, "userId must not be null");
    CloseableHttpResponse response = null;

    try {

      final String protocol = this.metadataServiceUseSsl ? "https" : "http";
      final HttpPost request = new HttpPost(
          String.format("%s://%s:%s/%s", protocol, this.metadataServiceHost, this.metadataServicePort,
              GENERATE_SESSION_TOKEN_ENDPOINT));

      // Build JSON request to generate a token on behalf of a user.
      final ObjectMapper objectMapper = new ObjectMapper();
      final ObjectNode objectNode = objectMapper.createObjectNode();
      objectNode.put(USER_ID_FIELD, userId);
      final String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectNode);
      request.setEntity(new StringEntity(json));

      // Add authorization header with DataHub frontend system id and secret.
      request.addHeader(Http.HeaderNames.AUTHORIZATION, this.systemAuthentication.getCredentials());

      response = httpClient.execute(request);
      final HttpEntity entity = response.getEntity();
      if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK && entity != null) {
        // Successfully generated a token for the User
        final String jsonStr = EntityUtils.toString(entity);
        return getAccessTokenFromJson(jsonStr);
      } else {
        throw new RuntimeException(
            String.format("Bad response from the Metadata Service: %s %s",
                response.getStatusLine().toString(), response.getEntity().toString()));
      }
    } catch (Exception e) {
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

  /**
   * Call the Auth Service to create a native Datahub user.
   */
  public boolean signUp(@Nonnull final String userUrn, @Nonnull final String fullName, @Nonnull final String email,
      @Nonnull final String title, @Nonnull final String password, @Nonnull final String inviteToken) {
    Objects.requireNonNull(userUrn, "userUrn must not be null");
    Objects.requireNonNull(fullName, "fullName must not be null");
    Objects.requireNonNull(email, "email must not be null");
    Objects.requireNonNull(title, "title must not be null");
    Objects.requireNonNull(password, "password must not be null");
    Objects.requireNonNull(inviteToken, "inviteToken must not be null");
    CloseableHttpResponse response = null;

    try {

      final String protocol = this.metadataServiceUseSsl ? "https" : "http";
      final HttpPost request = new HttpPost(
          String.format("%s://%s:%s/%s", protocol, this.metadataServiceHost, this.metadataServicePort,
              SIGN_UP_ENDPOINT));

      // Build JSON request to sign up a native user.
      final ObjectMapper objectMapper = new ObjectMapper();
      final ObjectNode objectNode = objectMapper.createObjectNode();
      objectNode.put(USER_URN_FIELD, userUrn);
      objectNode.put(FULL_NAME_FIELD, fullName);
      objectNode.put(EMAIL_FIELD, email);
      objectNode.put(TITLE_FIELD, title);
      objectNode.put(PASSWORD_FIELD, password);
      objectNode.put(INVITE_TOKEN_FIELD, inviteToken);
      final String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectNode);
      request.setEntity(new StringEntity(json));

      // Add authorization header with DataHub frontend system id and secret.
      request.addHeader(Http.HeaderNames.AUTHORIZATION, this.systemAuthentication.getCredentials());

      response = httpClient.execute(request);
      final HttpEntity entity = response.getEntity();
      if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK && entity != null) {
        // Successfully generated a token for the User
        final String jsonStr = EntityUtils.toString(entity);
        return getIsNativeUserCreatedFromJson(jsonStr);
      } else {
        throw new RuntimeException(
            String.format("Bad response from the Metadata Service: %s %s", response.getStatusLine().toString(),
                response.getEntity().toString()));
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

  /**
   * Call the Auth Service to reset credentials for a native DataHub user.
   */
  public boolean resetNativeUserCredentials(@Nonnull final String userUrn, @Nonnull final String password,
      @Nonnull final String resetToken) {
    Objects.requireNonNull(userUrn, "userUrn must not be null");
    Objects.requireNonNull(password, "password must not be null");
    Objects.requireNonNull(resetToken, "reset token must not be null");
    CloseableHttpResponse response = null;

    try {

      final String protocol = this.metadataServiceUseSsl ? "https" : "http";
      final HttpPost request = new HttpPost(
          String.format("%s://%s:%s/%s", protocol, this.metadataServiceHost, this.metadataServicePort,
              RESET_NATIVE_USER_CREDENTIALS_ENDPOINT));

      // Build JSON request to verify credentials for a native user.
      final ObjectMapper objectMapper = new ObjectMapper();
      final ObjectNode objectNode = objectMapper.createObjectNode();
      objectNode.put(USER_URN_FIELD, userUrn);
      objectNode.put(PASSWORD_FIELD, password);
      objectNode.put(RESET_TOKEN_FIELD, resetToken);
      final String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectNode);
      request.setEntity(new StringEntity(json));

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
            String.format("Bad response from the Metadata Service: %s %s", response.getStatusLine().toString(),
                response.getEntity().toString()));
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

  /**
   * Call the Auth Service to verify the credentials for a native Datahub user.
   */
  public boolean verifyNativeUserCredentials(@Nonnull final String userUrn, @Nonnull final String password) {
    Objects.requireNonNull(userUrn, "userUrn must not be null");
    Objects.requireNonNull(password, "password must not be null");
    CloseableHttpResponse response = null;

    try {

      final String protocol = this.metadataServiceUseSsl ? "https" : "http";
      final HttpPost request = new HttpPost(
          String.format("%s://%s:%s/%s", protocol, this.metadataServiceHost, this.metadataServicePort,
              VERIFY_NATIVE_USER_CREDENTIALS_ENDPOINT));

      // Build JSON request to verify credentials for a native user.
      final ObjectMapper objectMapper = new ObjectMapper();
      final ObjectNode objectNode = objectMapper.createObjectNode();
      objectNode.put(USER_URN_FIELD, userUrn);
      objectNode.put(PASSWORD_FIELD, password);
      final String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectNode);
      request.setEntity(new StringEntity(json));

      // Add authorization header with DataHub frontend system id and secret.
      request.addHeader(Http.HeaderNames.AUTHORIZATION, this.systemAuthentication.getCredentials());

      response = httpClient.execute(request);
      final HttpEntity entity = response.getEntity();
      if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK && entity != null) {
        // Successfully generated a token for the User
        final String jsonStr = EntityUtils.toString(entity);
        return getDoesPasswordMatchFromJson(jsonStr);
      } else {
        throw new RuntimeException(
            String.format("Bad response from the Metadata Service: %s %s", response.getStatusLine().toString(),
                response.getEntity().toString()));
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

  /**
   * Call the Auth Service to track an analytics event
   */
  public void track(@Nonnull final String event) {
    Objects.requireNonNull(event, "event must not be null");
    CloseableHttpResponse response = null;

    try {
      final String protocol = this.metadataServiceUseSsl ? "https" : "http";
      final HttpPost request = new HttpPost(
          String.format("%s://%s:%s/%s", protocol, this.metadataServiceHost, this.metadataServicePort,
              TRACK_ENDPOINT));

      // Build JSON request to track event.
      request.setEntity(new StringEntity(event));

      // Add authorization header with DataHub frontend system id and secret.
      request.addHeader(Http.HeaderNames.AUTHORIZATION, this.systemAuthentication.getCredentials());

      response = httpClient.execute(request);
      final HttpEntity entity = response.getEntity();

      if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK || entity == null) {
        throw new RuntimeException(
            String.format("Bad response from the Metadata Service: %s %s", response.getStatusLine().toString(),
                response.getEntity().toString()));
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to track event", e);
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

  private boolean getDoesPasswordMatchFromJson(final String jsonStr) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.readTree(jsonStr).get(DOES_PASSWORD_MATCH_FIELD).asBoolean();
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to parse JSON received from the MetadataService!");
    }
  }
}
