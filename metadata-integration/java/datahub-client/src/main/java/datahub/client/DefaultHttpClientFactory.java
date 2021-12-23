package datahub.client;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;


public class DefaultHttpClientFactory implements HttpClientFactory {
  @Override
  public HttpClient getHttpClient() {
    CloseableHttpClient httpClient = HttpClients.createDefault();
    return httpClient;
  }
}
