package auth;

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


/**
 * This class is responsible for coordinating authentication with GMS
 * using username / password, SSO, or other means.
 */
@Slf4j
public class AuthClient {

  private static final String GENERATE_SESSION_TOKEN_ENDPOINT = "auth/generateSessionToken";

  private final String metadataServiceHost;
  private final Integer metadataServicePort;
  private final Boolean metadataServiceUseSsl;
  private final String systemId;
  private final String systemSecret;

  public AuthClient(
      @Nonnull final String metadataServiceHost,
      @Nonnull final Integer metadataServicePort,
      @Nonnull final Boolean useSsl,
      @Nonnull final String systemId,
      @Nonnull final String systemSecret) {
    this.metadataServiceHost = Objects.requireNonNull(metadataServiceHost);
    this.metadataServicePort = Objects.requireNonNull(metadataServicePort);
    this.metadataServiceUseSsl = Objects.requireNonNull(useSsl);
    this.systemId = Objects.requireNonNull(systemId);
    this.systemSecret = Objects.requireNonNull(systemSecret);
  }

  /**
   * Generate a session token for a particular actor, or throws an exception if generation fails.
   */
  public String generateSessionTokenForUser(@Nonnull final String userUrn) {
    Objects.requireNonNull(userUrn);
    CloseableHttpClient httpClient = HttpClients.createDefault();

    try {

      final String protocol = this.metadataServiceUseSsl ? "https" : "http";
      final HttpPost request = new HttpPost(String.format("%s://%s:%s/%s", protocol, this.metadataServiceHost, this.metadataServicePort, GENERATE_SESSION_TOKEN_ENDPOINT));

      // Build JSON request to generate a token on behalf of a user.
      String json = "{" + String.format("\"userUrn\":\"%s\"", userUrn) + "}";
      request.setEntity(new StringEntity(json));

      // Add authorization header with DataHub frontend system id and secret.
      request.addHeader(Http.HeaderNames.AUTHORIZATION, String.format("Basic %s:%s", this.systemId, this.systemSecret));

      try (CloseableHttpResponse response = httpClient.execute(request)) {
        final HttpEntity entity = response.getEntity();
        if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK && entity != null) {
          // Successfully generated a token for the User
          final String jsonStr = EntityUtils.toString(entity);
          return getAccessTokenFromJson(jsonStr);
        } else {
          throw new RuntimeException(
              String.format("Failed to generated a session token for user. Bad response from the Metadata Service: %s %s",
                  response.getStatusLine().toString(), response.getEntity().toString()));
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed to generate session token for user!", e);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to generate session token for user!", e);
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
      return mapper.readTree(jsonStr).get("accessToken").asText();
    } catch (Exception e) {
      // Do not log the raw json in case it contains access token.
      throw new IllegalArgumentException("Failed to parse JSON received from the MetadataService!");
    }
  }
}
