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
package controllers.api.v1;

import com.fasterxml.jackson.databind.node.ObjectNode;
import dao.UserDAO;
import org.apache.commons.lang3.StringUtils;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;
import java.util.Map;

public class User extends Controller
{
    public static Result getLoggedInUser()
    {
        ObjectNode result = Json.newObject();

        result.put("status", "ok");
        String username = session("user");
        result.set("user", Json.toJson(UserDAO.getCurrentUser(username)));
        return ok(result);
    }


    public static Result updateSettings()
    {
        Map<String, String[]> params = request().body().asFormUrlEncoded();
        ObjectNode result = Json.newObject();

        String username = session("user");
        if (StringUtils.isNotBlank(username))
        {
            String message = UserDAO.updateUserSettings(params, username);
            if (StringUtils.isBlank(message))
            {
                result.put("status", "success");
            }
            else
            {
                result.put("status", "failed");
                result.put("message", message);
            }
        }
        else
        {
            result.put("status", "failed");
            result.put("message", "User is not authenticated");
        }
        return ok(result);
    }

    public static Result getAllCompanyUsers()
    {
        ObjectNode result = Json.newObject();

        result.put("status", "ok");
        result.set("employees", Json.toJson(UserDAO.getAllCompanyUsers()));
        return ok(result);
    }

    public static Result getAllGroups()
    {
        ObjectNode result = Json.newObject();

        result.put("status", "ok");
        result.set("groups", Json.toJson(UserDAO.getAllGroups()));
        return ok(result);
    }

    public static Result getAllUserEntities()
    {
        ObjectNode result = Json.newObject();

        result.put("status", "ok");
        result.set("userEntities", Json.toJson(UserDAO.getAllUserEntities()));
        return ok(result);
    }
}
