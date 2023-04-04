package com.linkedin.gms.factory.secret;

import com.linkedin.metadata.secret.SecretService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

@TestPropertySource(locations = "classpath:/application.yml")
@SpringBootTest(classes = {SecretServiceFactory.class})
public class SecretServiceFactoryTest extends AbstractTestNGSpringContextTests {

    @Value("${secretService.encryptionKey}")
    private String encryptionKey;

    @Autowired
    SecretService test;

    @Test
    void testInjection() throws IOException {
        assertEquals(encryptionKey, "ENCRYPTION_KEY");
        assertNotNull(test);
        assertEquals(test.getHashedPassword("".getBytes(StandardCharsets.UTF_8), "password"),
                "XohImNooBHFR0OVvjcYpJ3NgPQ1qq73WKhHvch0VQtg=");
    }
}
