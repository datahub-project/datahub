package auth.pac4j;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import javax.inject.Singleton;
import lombok.Getter;
import lombok.Setter;
import org.pac4j.core.context.WebContext;
import org.pac4j.core.context.session.SessionStore;
import org.pac4j.core.profile.CommonProfile;
import org.pac4j.core.util.Pac4jConstants;
import org.pac4j.core.util.serializer.JsonSerializer;
import org.pac4j.core.util.serializer.Serializer;
import org.pac4j.play.PlayWebContext;
import org.pac4j.play.store.DataEncrypter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import play.mvc.Http;

/**
 * Same behavior as pac4j {@code PlayCookieSessionStore} (play-pac4j 12.0.x) but without a default
 * field initializer that references {@code ShiroAesDataEncrypter} (Shiro 1.x / broken classpath
 * with Shiro 2.x).
 *
 * <p>See <a href="https://shiro.apache.org/security-reports.html">CVE-2026-23903</a> and DataHub's
 * upgrade to Apache Shiro 2.x.
 */
@Singleton
@Getter
@Setter
public class DatahubPlayCookieSessionStore implements SessionStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatahubPlayCookieSessionStore.class);

  private String sessionName = "pac4j";

  private DataEncrypter dataEncrypter;

  private Serializer serializer = new JsonSerializer();

  public DatahubPlayCookieSessionStore() {}

  public DatahubPlayCookieSessionStore(final DataEncrypter dataEncrypter) {
    this.dataEncrypter = dataEncrypter;
  }

  @Override
  public Optional getSessionId(final WebContext context, final boolean createSession) {
    final Http.Session session = ((PlayWebContext) context).getNativeSession();
    if (session.get(sessionName).isPresent()) {
      return Optional.of(sessionName);
    } else if (createSession) {
      putSessionValues(context, new HashMap<>());
      return Optional.of(sessionName);
    } else {
      return Optional.empty();
    }
  }

  @Override
  public Optional get(final WebContext context, final String key) {
    final Map values = getSessionValues(context);
    final Object value = values.get(key);
    if (value instanceof Exception) {
      LOGGER.debug("Get value: {} for key: {}", value.toString(), key);
    } else {
      LOGGER.debug("Get value: {} for key: {}", value, key);
    }
    return Optional.ofNullable(value);
  }

  protected Map getSessionValues(final WebContext context) {
    final Http.Session session = ((PlayWebContext) context).getNativeSession();
    final String sessionValue = session.get(sessionName).orElse(null);
    Map values = null;
    if (sessionValue != null) {
      final byte[] inputBytes = Base64.getDecoder().decode(sessionValue);
      values =
          (Map) serializer.deserializeFromBytes(uncompressBytes(dataEncrypter.decrypt(inputBytes)));
    }
    if (values != null) {
      return values;
    } else {
      return new HashMap<>();
    }
  }

  @Override
  public void set(final WebContext context, final String key, final Object value) {
    if (value instanceof Exception) {
      LOGGER.debug("set key: {} with value: {}", key, value.toString());
    } else {
      LOGGER.debug("set key: {}, with value: {}", key, value);
    }

    final Map values = getSessionValues(context);
    if (value == null) {
      values.remove(key);
    } else {
      Object clearedValue = value;
      if (Pac4jConstants.USER_PROFILES.equals(key)) {
        clearedValue = clearUserProfiles(value);
      }
      values.put(key, clearedValue);
    }

    putSessionValues(context, values);
  }

  protected void putSessionValues(final WebContext context, final Map values) {
    String serialized = null;
    if (values != null) {
      final byte[] javaSerBytes = serializer.serializeToBytes(values);
      serialized =
          Base64.getEncoder().encodeToString(dataEncrypter.encrypt(compressBytes(javaSerBytes)));
    }
    if (serialized != null) {
      LOGGER.trace("serialized token size = {}", serialized.length());
    } else {
      LOGGER.trace("-> null serialized token");
    }
    final PlayWebContext playWebContext = (PlayWebContext) context;
    if (serialized == null) {
      playWebContext.setNativeSession(playWebContext.getNativeSession().removing(sessionName));
    } else {
      playWebContext.setNativeSession(
          playWebContext.getNativeSession().adding(sessionName, serialized));
    }
  }

  @Override
  public boolean destroySession(final WebContext context) {
    putSessionValues(context, null);
    return true;
  }

  @Override
  public Optional getTrackableSession(final WebContext context) {
    return Optional.empty();
  }

  @Override
  public Optional buildFromTrackableSession(
      final WebContext context, final Object trackableSession) {
    return Optional.empty();
  }

  @Override
  public boolean renewSession(final WebContext context) {
    return false;
  }

  protected Object clearUserProfiles(Object value) {
    final LinkedHashMap profiles = (LinkedHashMap) value;
    profiles.forEach((name, profile) -> ((CommonProfile) profile).removeLoginData());
    return profiles;
  }

  public static byte[] uncompressBytes(byte[] zippedBytes) {
    final ByteArrayOutputStream resultBao = new ByteArrayOutputStream();
    try (GZIPInputStream zipInputStream =
        new GZIPInputStream(new ByteArrayInputStream(zippedBytes))) {
      byte[] buffer = new byte[8192];
      int len;
      while ((len = zipInputStream.read(buffer)) > 0) {
        resultBao.write(buffer, 0, len);
      }
      return resultBao.toByteArray();
    } catch (IOException e) {
      LOGGER.error("Unable to uncompress session cookie", e);
      return null;
    }
  }

  public static byte[] compressBytes(byte[] srcBytes) {
    final ByteArrayOutputStream resultBao = new ByteArrayOutputStream();
    try (GZIPOutputStream zipOutputStream = new GZIPOutputStream(resultBao)) {
      zipOutputStream.write(srcBytes);
    } catch (IOException e) {
      LOGGER.error("Unable to compress session cookie", e);
      return null;
    }

    return resultBao.toByteArray();
  }
}
