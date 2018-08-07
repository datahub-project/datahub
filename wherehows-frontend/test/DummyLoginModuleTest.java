import com.sun.security.auth.callback.TextCallbackHandler;
import java.util.HashMap;
import javax.security.auth.login.LoginException;
import javax.security.auth.Subject;
import org.junit.Test;
import security.DummyLoginModule;

import static org.junit.Assert.*;


public class DummyLoginModuleTest {

  @Test
  public void testAuthenticate() {
    DummyLoginModule lmodule = new DummyLoginModule();
    lmodule.initialize(new Subject(), new TextCallbackHandler(), null, new HashMap());
    LoginException ex = null;
    try {
      assertTrue("Failed to login", lmodule.login());
      assertTrue("Failed to logout",lmodule.logout());
      assertTrue("Failed to commit", lmodule.commit());
      assertTrue("Failed to abort", lmodule.abort());
    } catch (LoginException e) {
      ex = e;
    }
    assertNull(ex);
  }

}
