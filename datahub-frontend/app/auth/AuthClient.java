package auth;

import com.linkedin.util.Configuration;
import java.util.Objects;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;


/**
 * This class is responsible for coordinating authentication with GMS
 * using username / password, SSO, or other means.
 */
@Slf4j
public class AuthClient {

  /**
   * The following environment variables are expected to be provided.
   * They are used in establishing the connection to the downstream GMS.
   * Currently, only 1 downstream GMS is supported.
   */
  private static final String GMS_HOST_ENV_VAR = "DATAHUB_GMS_HOST";
  private static final String GMS_PORT_ENV_VAR = "DATAHUB_GMS_PORT";
  private static final String GMS_USE_SSL_ENV_VAR = "DATAHUB_GMS_USE_SSL";
  private static final String GMS_SSL_PROTOCOL_VAR = "DATAHUB_GMS_SSL_PROTOCOL";

  private final String systemId;
  private final String systemSecret;
  private final String gmsHost;
  private final Integer gmsPort;

  public AuthClient(final String systemId, final String systemSecret) {
    final String gmsHost = Configuration.getEnvironmentVariable(GMS_HOST_ENV_VAR, "localhost");
    final Integer gmsPort = Integer.valueOf(Configuration.getEnvironmentVariable(GMS_PORT_ENV_VAR, "8080"));
    final Boolean useSsl = Boolean.parseBoolean(Configuration.getEnvironmentVariable(GMS_USE_SSL_ENV_VAR, "False"));
    final String sslProtocol = Configuration.getEnvironmentVariable(GMS_SSL_PROTOCOL_VAR);
    this.systemId = systemId;
    this.systemSecret = systemSecret;
    this.gmsHost = gmsHost;
    this.gmsPort = gmsPort;
  }

  /**
   * Generate a session token for a particular actor, or throws an exception if generation fails.
   */
  public String generateSessionTokenForUser(@Nonnull final String userUrn) {
    Objects.requireNonNull(userUrn);

    CloseableHttpClient httpClient = HttpClients.createDefault();

    try {

      // TODO: Support SSL to GMS.
      HttpPost request = new HttpPost(String.format("http://%s:%s/auth/generateSessionToken", this.gmsHost, this.gmsPort));

      StringBuilder json = new StringBuilder();
      json.append("{");
      json.append(String.format("\"userUrn\":\"%s\"", userUrn));
      json.append("}");

      // send a JSON data
      request.setEntity(new StringEntity(json.toString()));

      // Add authorization header with Datahub frontend system id and secret.
      request.addHeader("Authorization", String.format("Basic %s:%s", this.systemId, this.systemSecret));

      CloseableHttpResponse response = httpClient.execute(request);

      try {
        final HttpEntity entity = response.getEntity();
        if (response.getStatusLine().getStatusCode() == 200 && entity != null) {
          // Successfully generated a token.
          String token = EntityUtils.toString(entity);
          return token;
        } else {
          throw new RuntimeException(String.format("Failed to generated a session token for user. Bad response from GMS: %s %s",
              response.getStatusLine().toString(),
              response.getEntity().toString()
              ));
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed to generate session token for user!", e);
      } finally {
        response.close();
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
}
