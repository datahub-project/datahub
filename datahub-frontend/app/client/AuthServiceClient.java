package client;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Objects;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import play.mvc.Http;
import com.datahub.authentication.Authentication;


/**
 * This class is responsible for coordinating authentication with the backend Metadata Service.
 */
@Slf4j
public class AuthServiceClient {

  private static final String GENERATE_SESSION_TOKEN_ENDPOINT = "auth/generateSessionTokenForUser";
  private static final String ACCESS_TOKEN_FIELD = "accessToken";
  private static final String USER_ID_FIELD = "userId";

  private final String metadataServiceHost;
  private final Integer metadataServicePort;
  private final Boolean metadataServiceUseSsl;
  private final Authentication systemAuthentication;

  public AuthServiceClient(
      @Nonnull final String metadataServiceHost,
      @Nonnull final Integer metadataServicePort,
      @Nonnull final Boolean useSsl,
      @Nonnull final Authentication systemAuthentication) {
    this.metadataServiceHost = Objects.requireNonNull(metadataServiceHost);
    this.metadataServicePort = Objects.requireNonNull(metadataServicePort);
    this.metadataServiceUseSsl = Objects.requireNonNull(useSsl);
    this.systemAuthentication = Objects.requireNonNull(systemAuthentication);
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
    CloseableHttpClient httpClient = HttpClients.createDefault();

    try {

      final String protocol = this.metadataServiceUseSsl ? "https" : "http";
      final HttpPost request = new HttpPost(String.format("%s://%s:%s/%s", protocol, this.metadataServiceHost,
          this.metadataServicePort, GENERATE_SESSION_TOKEN_ENDPOINT));

      // Build JSON request to generate a token on behalf of a user.
      String json = String.format("{ \"%s\":\"%s\" }", USER_ID_FIELD, userId);
      request.setEntity(new StringEntity(json));

      // Add authorization header with DataHub frontend system id and secret.
      request.addHeader(Http.HeaderNames.AUTHORIZATION, this.systemAuthentication.getCredentials());

      CloseableHttpResponse response = httpClient.execute(request);
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
        httpClient.close();
      } catch (Exception e) {
        log.warn("Failed to close http client", e);
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
}
