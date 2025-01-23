package auth;

import com.typesafe.config.Config;

public class CookieConfigs {
  public static final String SESSION_TTL_CONFIG_PATH = "auth.session.ttlInHours";
  public static final Integer DEFAULT_SESSION_TTL_HOURS = 720;
  public static final String AUTH_COOKIE_SAME_SITE = "play.http.session.sameSite";
  public static final String DEFAULT_AUTH_COOKIE_SAME_SITE = "LAX";
  public static final String AUTH_COOKIE_SECURE = "play.http.session.secure";
  public static final boolean DEFAULT_AUTH_COOKIE_SECURE = false;

  private final int ttlInHours;
  private final String authCookieSameSite;
  private final boolean authCookieSecure;

  public CookieConfigs(final Config configs) {
    ttlInHours =
        configs.hasPath(SESSION_TTL_CONFIG_PATH)
            ? configs.getInt(SESSION_TTL_CONFIG_PATH)
            : DEFAULT_SESSION_TTL_HOURS;
    authCookieSameSite =
        configs.hasPath(AUTH_COOKIE_SAME_SITE)
            ? configs.getString(AUTH_COOKIE_SAME_SITE)
            : DEFAULT_AUTH_COOKIE_SAME_SITE;
    authCookieSecure =
        configs.hasPath(AUTH_COOKIE_SECURE)
            ? configs.getBoolean(AUTH_COOKIE_SECURE)
            : DEFAULT_AUTH_COOKIE_SECURE;
  }

  public int getTtlInHours() {
    return ttlInHours;
  }

  public String getAuthCookieSameSite() {
    return authCookieSameSite;
  }

  public boolean getAuthCookieSecure() {
    return authCookieSecure;
  }
}
