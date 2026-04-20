package utils;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import org.junit.jupiter.api.Test;
import org.pac4j.core.exception.http.FoundAction;
import org.pac4j.play.store.PlayCookieSessionStore;

public class SerializationUtilsTest {

  @Test
  public void testRoundTrip() {
    FoundAction original = new FoundAction("/dashboard");
    String encoded = SerializationUtils.serializeFoundAction(original);
    FoundAction decoded = SerializationUtils.deserializeFoundAction(encoded);
    assertEquals("/dashboard", decoded.getLocation());
  }

  @Test
  public void testRoundTripWithBasePath() {
    FoundAction original = new FoundAction("/datahub/dashboard?tab=overview");
    String encoded = SerializationUtils.serializeFoundAction(original);
    FoundAction decoded = SerializationUtils.deserializeFoundAction(encoded);
    assertEquals("/datahub/dashboard?tab=overview", decoded.getLocation());
  }

  @Test
  public void testSerializedFormIsPlainBase64NotJavaSerialization() {
    FoundAction action = new FoundAction("/test");
    String encoded = SerializationUtils.serializeFoundAction(action);

    // Verify the encoded value is simply Base64(URL string), not Java serialization
    String decoded = new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8);
    assertEquals("/test", decoded);
  }

  @Test
  public void testDeserializeInvalidBase64FallsBackToRoot() {
    // Corrupt/malicious cookie value should not throw, should fall back to "/"
    FoundAction result = SerializationUtils.deserializeFoundAction("not-valid-base64!!!");
    assertEquals("/", result.getLocation());
  }

  @Test
  public void testDeserializeRejectsAbsoluteUrl() {
    String payload =
        Base64.getEncoder().encodeToString("https://evil.com".getBytes(StandardCharsets.UTF_8));
    FoundAction result = SerializationUtils.deserializeFoundAction(payload);
    assertEquals("/", result.getLocation());
  }

  @Test
  public void testDeserializeRejectsProtocolRelativeUrl() {
    String payload =
        Base64.getEncoder().encodeToString("//evil.com/phish".getBytes(StandardCharsets.UTF_8));
    FoundAction result = SerializationUtils.deserializeFoundAction(payload);
    assertEquals("/", result.getLocation());
  }

  @Test
  public void testDeserializeRejectsTripleSlashUrl() {
    String payload =
        Base64.getEncoder().encodeToString("///evil.com".getBytes(StandardCharsets.UTF_8));
    FoundAction result = SerializationUtils.deserializeFoundAction(payload);
    assertEquals("/", result.getLocation());
  }

  @Test
  public void testDeserializeAllowsRelativePaths() {
    for (String path : new String[] {"/", "/dashboard", "/datahub/settings?tab=1", "/a/b/c#frag"}) {
      String payload = Base64.getEncoder().encodeToString(path.getBytes(StandardCharsets.UTF_8));
      FoundAction result = SerializationUtils.deserializeFoundAction(payload);
      assertEquals(path, result.getLocation());
    }
  }

  /**
   * Simulates the old vulnerable format: a Java-serialized object that has been gzip-compressed and
   * Base64-encoded. Verifies that the new implementation does NOT deserialize it as a Java object —
   * instead it treats the bytes as a UTF-8 string (producing garbage) and wraps it in a harmless
   * FoundAction.
   */
  @Test
  public void testOldFormatJavaSerializedPayloadDoesNotDeserialize() throws Exception {
    // Create a Java-serialized HashMap (the simplest gadget base) in the old format
    HashMap<String, String> maliciousObject = new HashMap<>();
    maliciousObject.put("exploit", "value");

    byte[] serializedBytes = javaSerialize(maliciousObject);
    byte[] compressed = PlayCookieSessionStore.compressBytes(serializedBytes);
    String oldFormatPayload = Base64.getEncoder().encodeToString(compressed);

    // The new code Base64-decodes, then interprets as UTF-8 string — no ObjectInputStream
    FoundAction result = SerializationUtils.deserializeFoundAction(oldFormatPayload);

    // Should return a FoundAction (not throw), and the location will be binary garbage as UTF-8
    assertNotNull(result);
    assertNotNull(result.getLocation());
    // The key assertion: the result is NOT the deserialized HashMap — it's a FoundAction with a
    // garbage string location. The HashMap was never reconstructed.
    assertNotEquals("value", result.getLocation());
  }

  /**
   * Simulates a URLDNS-style payload: a Java-serialized URL object (the actual gadget used in the
   * vulnerability report) in the old gzip+Base64 format. Verifies no DNS lookup or deserialization
   * occurs.
   */
  @Test
  public void testUrldnsGadgetPayloadDoesNotTriggerDeserialization() throws Exception {
    // Serialize a java.net.URL — this is the exact class used in the URLDNS gadget chain.
    // In the old code, deserializing this would trigger URL.hashCode() → DNS lookup.
    URL url = new URL("http://urldns-test-should-never-resolve.example.com");
    byte[] serializedBytes = javaSerialize(url);
    byte[] compressed = PlayCookieSessionStore.compressBytes(serializedBytes);
    String payload = Base64.getEncoder().encodeToString(compressed);

    // New code never calls ObjectInputStream.readObject(), so no DNS lookup occurs
    FoundAction result = SerializationUtils.deserializeFoundAction(payload);

    assertNotNull(result);
    assertNotNull(result.getLocation());
    // The URL object was never deserialized — location is just the raw bytes as a string
    assertFalse(result.getLocation().contains("urldns-test-should-never-resolve"));
  }

  /**
   * Verifies that raw Java serialization bytes (without gzip wrapping) are also safe. The
   * Base64-decoded bytes start with the Java serialization magic (0xACED) but are never passed to
   * ObjectInputStream.
   */
  @Test
  public void testRawJavaSerializedBytesDoNotDeserialize() throws Exception {
    FoundAction maliciousAction = new FoundAction("http://attacker.com/steal-token");
    byte[] serializedBytes = javaSerialize(maliciousAction);
    // No gzip — just raw serialized bytes, Base64-encoded
    String payload = Base64.getEncoder().encodeToString(serializedBytes);

    FoundAction result = SerializationUtils.deserializeFoundAction(payload);

    assertNotNull(result);
    // The FoundAction was NOT reconstructed via deserialization. The location is the raw
    // serialized bytes interpreted as UTF-8, which will start with garbage (0xACED magic bytes),
    // not the original URL.
    assertNotEquals("http://attacker.com/steal-token", result.getLocation());
  }

  private static byte[] javaSerialize(Object obj) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(obj);
    }
    return baos.toByteArray();
  }
}
