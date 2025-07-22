package utils;

import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import java.net.URL;
import java.net.http.HttpClient;

import static org.junit.jupiter.api.Assertions.*;

class CustomHttpClientFactoryTest {

    private static String getTruststorePathFromClasspath() {
        URL url = CustomHttpClientFactoryTest.class.getClassLoader().getResource("test-truststore.p12");
        return url != null ? url.getPath() : null;
    }

    private static final String VALID_TRUSTSTORE_PASSWORD = "testpassword";
    private static final String VALID_TRUSTSTORE_TYPE = "PKCS12";
    private static final String INVALID_TRUSTSTORE_PATH = "src/test/resources/doesnotexist.p12";
    private static final String INVALID_TRUSTSTORE_PASSWORD = "wrongpassword";

    @Test
    void testCreateSslContextWithValidTruststore() throws Exception {
        String path = getTruststorePathFromClasspath();
        if (path == null) {
            System.out.println("Truststore not found on classpath, skipping test.");
            return;
        }
        SSLContext context = CustomHttpClientFactory.createSslContext(
                path, VALID_TRUSTSTORE_PASSWORD, VALID_TRUSTSTORE_TYPE);
        assertNotNull(context);
        assertEquals("TLSv1.2", context.getProtocol());
    }

    @Test
    void testCreateSslContextWithInvalidTruststoreThrows() {
        assertThrows(Exception.class, () -> CustomHttpClientFactory.createSslContext(
                INVALID_TRUSTSTORE_PATH, INVALID_TRUSTSTORE_PASSWORD, VALID_TRUSTSTORE_TYPE));
    }

    @Test
    void testGetJavaHttpClientWithValidTruststore() {
        String path = getTruststorePathFromClasspath();
        if (path == null) {
            System.out.println("Truststore not found on classpath, skipping test.");
            return;
        }
        HttpClient client = CustomHttpClientFactory.getJavaHttpClient(
                path, VALID_TRUSTSTORE_PASSWORD, VALID_TRUSTSTORE_TYPE);
        assertNotNull(client);
    }

    @Test
    void testGetJavaHttpClientWithInvalidTruststoreFallsBack() {
        HttpClient client = CustomHttpClientFactory.getJavaHttpClient(
                INVALID_TRUSTSTORE_PATH, INVALID_TRUSTSTORE_PASSWORD, VALID_TRUSTSTORE_TYPE);
        assertNotNull(client);
    }

    @Test
    void testGetApacheHttpClientWithValidTruststore() {
        String path = getTruststorePathFromClasspath();
        if (path == null) {
            System.out.println("Truststore not found on classpath, skipping test.");
            return;
        }
        CloseableHttpClient client = CustomHttpClientFactory.getApacheHttpClient(
                path, VALID_TRUSTSTORE_PASSWORD, VALID_TRUSTSTORE_TYPE);
        assertNotNull(client);
    }

    @Test
    void testGetApacheHttpClientWithInvalidTruststoreFallsBack() {
        CloseableHttpClient client = CustomHttpClientFactory.getApacheHttpClient(
                INVALID_TRUSTSTORE_PATH, INVALID_TRUSTSTORE_PASSWORD, VALID_TRUSTSTORE_TYPE);
        assertNotNull(client);
    }
}
