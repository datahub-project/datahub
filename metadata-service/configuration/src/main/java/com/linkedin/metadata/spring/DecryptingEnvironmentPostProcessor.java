// START GENAI
package com.linkedin.metadata.spring;

import java.util.HashMap;
import java.util.Map;
import io.datahubproject.metadata.services.SecretService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertySource;

@Slf4j
public class DecryptingEnvironmentPostProcessor implements EnvironmentPostProcessor, Ordered {

    private static final SecretService secretService;

    static {
        String base64Key = System.getenv("SECRET_SERVICE_ENCRYPTION_KEY");
        if (base64Key != null && !base64Key.isEmpty()) {
            secretService = new SecretService(base64Key);
            log.info("[Decryptor] SECRET_SERVICE_ENCRYPTION_KEY loaded successfully.");
        } else {
            secretService = null;
            log.warn("[Decryptor] No SECRET_SERVICE_ENCRYPTION_KEY set — encrypted values will fail.");
        }
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        MutablePropertySources propertySources = environment.getPropertySources();

        for (PropertySource<?> source : propertySources) {
            if (source.getSource() instanceof Map) {
                Map<String, Object> original = (Map<String, Object>) source.getSource();
                Map<String, Object> updated = new HashMap<>(original.size());

                for (Map.Entry<String, Object> entry : original.entrySet()) {
                    updated.put(entry.getKey(), processValue(entry.getValue()));
                }

                propertySources.replace(
                        source.getName(),
                        new org.springframework.core.env.MapPropertySource(source.getName(), updated)
                );
            }
        }
    }

    private String processValue(Object value) {
        if (value == null) {
            return null;
        }
        String strValue = value.toString();
        if (isEncrypted(strValue)) {
            if (secretService == null) {
                throw new IllegalStateException(
                        "[Decryptor] Found encrypted value but no SECRET_SERVICE_ENCRYPTION_KEY is set: " + strValue
                );
            }
            try {
                // Strip ENC(...) wrapper before decrypting
                String cipherText = strValue.substring(4, strValue.length() - 1);
                return secretService.decrypt(cipherText);
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Failed to decrypt property value (GCM only): " + strValue, e
                );
            }
        }
        return strValue;
    }

    private boolean isEncrypted(String value) {
        return value.startsWith("ENC(") && value.endsWith(")");
    }
}
