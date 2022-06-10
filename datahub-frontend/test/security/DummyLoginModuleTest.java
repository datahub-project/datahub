package security;

import com.sun.security.auth.callback.TextCallbackHandler;
import java.util.HashMap;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;
import org.testng.annotations.Test;

import static org.junit.Assert.*;


public class DummyLoginModuleTest {

  @Test
  public void testAuthenticate() {
    DummyLoginModule lmodule = new DummyLoginModule();
    lmodule.initialize(new Subject(), new TextCallbackHandler(), null, new HashMap<>());

    try {
      assertTrue("Failed to login", lmodule.login());
      assertTrue("Failed to logout", lmodule.logout());
      assertTrue("Failed to commit", lmodule.commit());
      assertTrue("Failed to abort", lmodule.abort());
    } catch (LoginException e) {
      fail(e.toString());
    }
  }
}
