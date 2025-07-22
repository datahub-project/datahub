package utils;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;
import java.net.URL;

import static org.junit.jupiter.api.Assertions.*;

class CustomHttpClientModuleTest {

    private static String getTruststorePathFromClasspath() {
        URL url = CustomHttpClientModuleTest.class.getClassLoader().getResource("test-truststore.p12");
        return url != null ? url.getPath() : null;
    }

    @Test
    void testProvideClientsWithValidTruststore() {
        String truststorePath = getTruststorePathFromClasspath();
        String truststorePassword = "testpassword";
        String truststoreType = "PKCS12";

        if (truststorePath == null) {
            System.out.println("Truststore not found on classpath, skipping test.");
            return;
        }

        Config config = ConfigFactory.parseString(
                "metadata.service.ssl.trust-store-path=\"" + truststorePath + "\"\n" +
                        "metadata.service.ssl.trust-store-password=\"" + truststorePassword + "\"\n" +
                        "metadata.service.ssl.trust-store-type=\"" + truststoreType + "\""
        );

        Injector injector = Guice.createInjector(binder -> {
            binder.bind(Config.class).toInstance(config);
            binder.install(new CustomHttpClientModule());
        });

        CloseableHttpClient apacheClient = injector.getInstance(CloseableHttpClient.class);
        HttpClient javaClient = injector.getInstance(HttpClient.class);

        assertNotNull(apacheClient);
        assertNotNull(javaClient);
    }

    @Test
    void testProvideClientsWithInvalidTruststoreFallsBack() {
        Config config = ConfigFactory.parseString(
                "metadata.service.ssl.trust-store-path=\"invalid/path.p12\"\n" +
                        "metadata.service.ssl.trust-store-password=\"badpassword\"\n" +
                        "metadata.service.ssl.trust-store-type=\"PKCS12\""
        );

        Injector injector = Guice.createInjector(binder -> {
            binder.bind(Config.class).toInstance(config);
            binder.install(new CustomHttpClientModule());
        });

        CloseableHttpClient apacheClient = injector.getInstance(CloseableHttpClient.class);
        HttpClient javaClient = injector.getInstance(HttpClient.class);

        assertNotNull(apacheClient);
        assertNotNull(javaClient);
    }
}
