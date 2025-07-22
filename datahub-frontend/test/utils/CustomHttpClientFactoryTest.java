package utils;

import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import java.net.http.HttpClient;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class CustomHttpClientFactoryTest {

    // Provide valid test truststore details here.
    // You can generate a test truststore with keytool for real integration testing.
    private static final String VALID_TRUSTSTORE_PATH = "test/resources/test-truststore.p12";
    private static final String VALID_TRUSTSTORE_PASSWORD = "testpassword";
    private static final String VALID_TRUSTSTORE_TYPE = "PKCS12";
    private static final String INVALID_TRUSTSTORE_PATH = "src/test/resources/doesnotexist.p12";
    private static final String INVALID_TRUSTSTORE_PASSWORD = "wrongpassword";

    @Test
    void testCreateSslContextWithValidTruststore() throws Exception {
        if (!Files.exists(Path.of(VALID_TRUSTSTORE_PATH))) return; // skip if not present
        SSLContext context = CustomHttpClientFactory.createSslContext(
                VALID_TRUSTSTORE_PATH, VALID_TRUSTSTORE_PASSWORD, VALID_TRUSTSTORE_TYPE);
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
        if (!Files.exists(Path.of(VALID_TRUSTSTORE_PATH))) return;
        HttpClient client = CustomHttpClientFactory.getJavaHttpClient(
                VALID_TRUSTSTORE_PATH, VALID_TRUSTSTORE_PASSWORD, VALID_TRUSTSTORE_TYPE);
        assertNotNull(client);
    }

    @Test
    void testGetJavaHttpClientWithInvalidTruststoreFallsBack() {
        HttpClient client = CustomHttpClientFactory.getJavaHttpClient(
                INVALID_TRUSTSTORE_PATH, INVALID_TRUSTSTORE_PASSWORD, VALID_TRUSTSTORE_TYPE);
        assertNotNull(client);
        // Could add more checks if needed (e.g. class type)
    }

    @Test
    void testGetApacheHttpClientWithValidTruststore() {
        if (!Files.exists(Path.of(VALID_TRUSTSTORE_PATH))) return;
        CloseableHttpClient client = CustomHttpClientFactory.getApacheHttpClient(
                VALID_TRUSTSTORE_PATH, VALID_TRUSTSTORE_PASSWORD, VALID_TRUSTSTORE_TYPE);
        assertNotNull(client);
    }

    @Test
    void testGetApacheHttpClientWithInvalidTruststoreFallsBack() {
        CloseableHttpClient client = CustomHttpClientFactory.getApacheHttpClient(
                INVALID_TRUSTSTORE_PATH, INVALID_TRUSTSTORE_PASSWORD, VALID_TRUSTSTORE_TYPE);
        assertNotNull(client);
    }
}
