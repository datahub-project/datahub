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
package controllers;

import play.mvc.Security;
import play.mvc.Http.Context;
import play.mvc.Result;

public class Secured extends Security.Authenticator
{
  @Override
  public String getUsername(Context ctx) {
    return ctx.session().get("user");
  }

  @Override
  public Result onUnauthorized(Context ctx) {
    return unauthorized();
  }
}
