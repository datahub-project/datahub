package datahub.client;

import org.apache.http.client.HttpClient;


public interface HttpClientFactory {

  HttpClient getHttpClient();

}
