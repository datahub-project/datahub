/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package security;

import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;

import javax.naming.AuthenticationException;
import javax.naming.NamingException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.Callback;

public class AuthenticationManager {

  public static void authenticateUser(String userName, String password) throws NamingException {
    if (userName == null || userName.isEmpty() || password == null || password.isEmpty()) {
      throw new IllegalArgumentException("Username and password can not be blank.");
    }
    LoginContext lc = null;
    try {
      lc = new LoginContext("WHZ-Authentication", new WHZCallbackHandler(userName, password));
    } catch (LoginException le) {
      throw new AuthenticationException(le.toString());
    }

    try {
      lc.login();
    } catch (LoginException le) {
      throw new AuthenticationException(le.toString());
    }
  }

  private static class WHZCallbackHandler implements CallbackHandler {
    private String password = null;
    private String username = null;
    private WHZCallbackHandler(String username, String password) {
      this.username = username;
      this.password = password;
    }
    public void handle(Callback[] callbacks)
        throws UnsupportedCallbackException {
      NameCallback nc = null;
      PasswordCallback pc = null;
      for (Callback callback : callbacks) {
        if (callback instanceof NameCallback) {
          nc = (NameCallback) callback;
          nc.setName(this.username);
        } else if (callback instanceof PasswordCallback) {
          pc = (PasswordCallback) callback;
          pc.setPassword(this.password.toCharArray());
        } else {
          throw new UnsupportedCallbackException(callback, "The submitted Callback is unsupported");
        }
      }
    }
  }
}
