package auth.cookie;

import com.google.inject.AbstractModule;
import play.api.libs.crypto.CookieSigner;
import play.api.libs.crypto.CookieSignerProvider;
import play.api.mvc.DefaultFlashCookieBaker;
import play.api.mvc.FlashCookieBaker;
import play.api.mvc.SessionCookieBaker;

public class CustomCookiesModule extends AbstractModule {

  @Override
  public void configure() {
    bind(CookieSigner.class).toProvider(CookieSignerProvider.class);
    // We override the session cookie baker to not use a fallback, this prevents using an old URL
    // Encoded cookie
    bind(SessionCookieBaker.class).to(CustomSessionCookieBaker.class);
    // We don't care about flash cookies, we don't use them
    bind(FlashCookieBaker.class).to(DefaultFlashCookieBaker.class);
  }
}
