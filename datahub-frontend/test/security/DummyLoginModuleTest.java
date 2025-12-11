/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package security;

import static org.junit.jupiter.api.Assertions.*;

import com.sun.security.auth.callback.TextCallbackHandler;
import java.util.HashMap;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;
import org.junit.jupiter.api.Test;

public class DummyLoginModuleTest {

  @Test
  public void testAuthenticate() {
    DummyLoginModule lmodule = new DummyLoginModule();
    lmodule.initialize(new Subject(), new TextCallbackHandler(), null, new HashMap<>());

    try {
      assertTrue(lmodule.login(), "Failed to login");
      assertTrue(lmodule.logout(), "Failed to logout");
      assertTrue(lmodule.commit(), "Failed to commit");
      assertTrue(lmodule.abort(), "Failed to abort");
    } catch (LoginException e) {
      fail(e.toString());
    }
  }
}
