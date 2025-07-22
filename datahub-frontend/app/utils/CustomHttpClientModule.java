package utils;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import java.net.http.HttpClient;
import org.apache.http.impl.client.CloseableHttpClient;

public class CustomHttpClientModule extends AbstractModule {
  @Provides
  public CloseableHttpClient provideCloseableHttpClient(Config config) {
    TruststoreConfig tsConfig = TruststoreConfig.fromConfig(config);
    try {
      if (tsConfig.isValid()) {
        return CustomHttpClientFactory.getApacheHttpClient(
            tsConfig.path, tsConfig.password, tsConfig.type);
      } else {
        return org.apache.http.impl.client.HttpClients.createDefault();
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize CloseableHttpClient", e);
    }
  }

  @Provides
  public HttpClient provideHttpClient(Config config) {
    TruststoreConfig tsConfig = TruststoreConfig.fromConfig(config);
    try {
      if (tsConfig.isValid()) {
        return CustomHttpClientFactory.getJavaHttpClient(
            tsConfig.path, tsConfig.password, tsConfig.type);
      } else {
        return HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize HttpClient", e);
    }
  }
}
