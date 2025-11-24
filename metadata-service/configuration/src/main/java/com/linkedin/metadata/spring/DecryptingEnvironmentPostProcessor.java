package com.linkedin.metadata.spring;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertySource;

@Slf4j
public class DecryptingEnvironmentPostProcessor implements EnvironmentPostProcessor, Ordered {

  private static final String ALGO = "AES";
  private static final String CIPHER_ALGO = "AES/GCM/NoPadding";
  private static final int GCM_TAG_LENGTH = 128; // bits
  private static final int IV_LENGTH = 12; // bytes

  @Override
  public int getOrder() {
    return Ordered.HIGHEST_PRECEDENCE;
  }

  @Override
  public void postProcessEnvironment(
      ConfigurableEnvironment environment, SpringApplication application) {
    String base64Key = System.getenv("CONFIG_ENCRYPTION_KEY");
    if (base64Key == null || base64Key.isEmpty()) {
      log.info("[Decryptor] No CONFIG_ENCRYPTION_KEY set — skipping decryption.");
      return;
    }

    MutablePropertySources propertySources = environment.getPropertySources();

    for (PropertySource<?> source : propertySources) {
      if (source.getSource() instanceof Map) {
        Map<String, Object> original = (Map<String, Object>) source.getSource();
        Map<String, Object> updated = new HashMap<>(original.size());

        for (Map.Entry<String, Object> entry : original.entrySet()) {
          updated.put(entry.getKey(), processValue(entry.getValue(), base64Key));
        }

        propertySources.replace(
            source.getName(),
            new org.springframework.core.env.MapPropertySource(source.getName(), updated));
      }
    }
  }

  /** Converts value to String, decrypts if in ENC(...) format using AES/GCM/NoPadding. */
  private String processValue(Object value, String base64Key) {
    if (value == null) {
      return null;
    }
    String strValue = value.toString();
    if (strValue.startsWith("ENC(") && strValue.endsWith(")")) {
      try {
        return decryptGCM(strValue, base64Key);
      } catch (Exception e) {
        throw new IllegalStateException(
            "Failed to decrypt property value (GCM only): " + strValue, e);
      }
    }
    return strValue;
  }

  /** Standard AES/GCM decryptor for ENC(...) values. Format: ENC(Base64(IV + Ciphertext + Tag)) */
  public static String decryptGCM(String encValue, String base64Key) throws Exception {
    if (encValue == null) {
      return null;
    }

    if (!encValue.startsWith("ENC(") || !encValue.endsWith(")")) {
      return encValue; // not encrypted
    }

    String cipherTextBase64 = encValue.substring(4, encValue.length() - 1); // Remove ENC(...)
    byte[] cipherMessage = Base64.getDecoder().decode(cipherTextBase64);

    if (cipherMessage.length < (IV_LENGTH + 16)) {
      throw new IllegalStateException(
          "Ciphertext too short to be valid AES/GCM. Re-encrypt using AES/GCM with a 12-byte IV.");
    }

    // Extract IV
    byte[] iv = new byte[IV_LENGTH];
    System.arraycopy(cipherMessage, 0, iv, 0, IV_LENGTH);

    // Extract ciphertext + tag
    byte[] cipherText = new byte[cipherMessage.length - IV_LENGTH];
    System.arraycopy(cipherMessage, IV_LENGTH, cipherText, 0, cipherText.length);

    // Init AES/GCM cipher
    SecretKeySpec keySpec = new SecretKeySpec(Base64.getDecoder().decode(base64Key), ALGO);
    Cipher cipher = Cipher.getInstance(CIPHER_ALGO);
    GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH, iv);
    cipher.init(Cipher.DECRYPT_MODE, keySpec, spec);

    byte[] decryptedBytes = cipher.doFinal(cipherText);
    return new String(decryptedBytes);
  }
}
