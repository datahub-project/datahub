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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dao.TrackingDAO;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;
import org.apache.commons.lang3.StringUtils;

public class Tracking extends Controller
{

    public static Result addTrackingEvent()
    {
        ObjectNode result = Json.newObject();
        String username = session("user");
        ObjectNode json = Json.newObject();
        ArrayNode res = json.arrayNode();
        JsonNode requestNode = request().body().asJson();

        if (StringUtils.isNotBlank(username))
        {
            String message = TrackingDAO.addTrackingEvent(requestNode, username);
            if (StringUtils.isBlank(message))
            {
                result.put("status", "success");
                return ok(result);
            }
            else
            {
                result.put("status", "failed");
                result.put("message", message);
                return badRequest(result);
            }
        }
        else
        {
            result.put("status", "failed");
            result.put("message", "User is not authenticated");
            return unauthorized(result);
        }
    }
}
