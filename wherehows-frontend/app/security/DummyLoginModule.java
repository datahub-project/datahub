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

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;
import java.util.Map;


/**
 * This LoginModule performs dummy authentication.
 * Any username and password can work for authentication
 */
public class DummyLoginModule implements LoginModule {

  public void initialize(final Subject subject, final CallbackHandler callbackHandler, final Map<String, ?> sharedState, final Map<String, ?> options) {
  }

  public boolean login() throws LoginException {
    return true;
  }

  public boolean commit() throws LoginException {
    return true;
  }

  public boolean abort() throws LoginException {
    return true;
  }

  public boolean logout() throws LoginException {
    return true;
  }

}