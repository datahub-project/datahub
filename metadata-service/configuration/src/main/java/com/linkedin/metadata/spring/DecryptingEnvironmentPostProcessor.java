package com.linkedin.metadata.spring;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertySource;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class DecryptingEnvironmentPostProcessor implements EnvironmentPostProcessor, Ordered {

    private static final String ALGO = "AES";

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        String base64Key = System.getenv("CONFIG_ENCRYPTION_KEY");
        if (base64Key == null || base64Key.isEmpty()) {
            log.info("[Decryptor] No CONFIG_ENCRYPTION_KEY set — skipping decryption.");
            return;
        }

        MutablePropertySources propertySources = environment.getPropertySources();

        // Process all property sources
        for (PropertySource<?> source : propertySources) {
            if (source.getSource() instanceof Map) {
                Map<String, Object> original = (Map<String, Object>) source.getSource();
                Map<String, Object> updated = new HashMap<>(original.size());

                for (Map.Entry<String, Object> entry : original.entrySet()) {
                    updated.put(entry.getKey(), processValue(entry.getValue(), base64Key));
                }

                propertySources.replace(source.getName(),
                        new org.springframework.core.env.MapPropertySource(source.getName(), updated));
            }
        }
    }

    /**
     * Converts value to String, decrypts if in ENC(...) format.
     */
    private String processValue(Object value, String base64Key) {
        if (value == null) {
            return null;
        }
        String strValue = value.toString();
        if (strValue.startsWith("ENC(") && strValue.endsWith(")")) {
            try {
                return decrypt(strValue, base64Key);
            } catch (Exception e) {
                throw new IllegalStateException("Failed to decrypt property value: " + strValue, e);
            }
        }
        return strValue;
    }

    private String decrypt(String encValue, String base64Key) throws Exception {
        String cipherText = encValue.substring(4, encValue.length() - 1); // Remove ENC(...)
        SecretKeySpec keySpec = new SecretKeySpec(Base64.getDecoder().decode(base64Key), ALGO);
        Cipher cipher = Cipher.getInstance(ALGO);
        cipher.init(Cipher.DECRYPT_MODE, keySpec);
        byte[] decryptedBytes = cipher.doFinal(Base64.getDecoder().decode(cipherText));
        return new String(decryptedBytes);
    }
}
