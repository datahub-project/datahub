package utils;

import static org.pac4j.play.store.PlayCookieSessionStore.compressBytes;
import static org.pac4j.play.store.PlayCookieSessionStore.uncompressBytes;

import java.util.Base64;
import javax.annotation.Nonnull;
import org.pac4j.core.exception.http.FoundAction;
import org.pac4j.core.util.serializer.JavaSerializer;

public class SerializationUtils {

  private static final JavaSerializer JAVA_SERIALIZER = new JavaSerializer();

  private SerializationUtils() {}

  public static String serializeFoundAction(@Nonnull final FoundAction foundAction) {
    byte[] javaSerBytes = JAVA_SERIALIZER.serializeToBytes(foundAction);
    return Base64.getEncoder().encodeToString(compressBytes(javaSerBytes));
  }

  public static FoundAction deserializeFoundAction(@Nonnull final String serialized) {
    return (FoundAction)
        JAVA_SERIALIZER.deserializeFromBytes(
            uncompressBytes(Base64.getDecoder().decode(serialized)));
  }
}
