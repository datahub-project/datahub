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
package dao;

import java.util.*;

import com.fasterxml.jackson.databind.JsonNode;
import models.*;
import org.apache.commons.lang3.StringUtils;
import org.springframework.dao.EmptyResultDataAccessException;
import play.Logger;
import play.libs.Json;

public class TrackingDAO extends AbstractMySQLOpenSourceDAO
{
    public static String TRACKING_ACCESS_UNIX_TIME_COLUMN = "access_unixtime";

    public static String TRACKING_OBJECT_TYPE_COLUMN = "object_type";

    public static String TRACKING_OBJECT_ID_COLUMN = "object_id";

    public static String TRACKING_OBJECT_NAME_COLUMN = "object_name";

    public static String TRACKING_PARAMETERS_COLUMN = "parameters";

    private final static String GET_USER_ID = "SELECT id FROM users WHERE username = ?";

    private final static String ADD_TRACKING_EVENT  = "INSERT INTO track_object_access_log (access_unixtime, " +
            "login_id, object_type, object_id, object_name, parameters) VALUES (?, ?, ?, ?, ?, ?) ";

    public static String addTrackingEvent(JsonNode requestNode, String user)
	{
        String message = "Internal error";
        if (requestNode == null || (!requestNode.isContainerNode()))
        {
            return "Empty post body";
        }

        Long accessTime = 0L;
        if (requestNode.has(TRACKING_ACCESS_UNIX_TIME_COLUMN))
        {
            accessTime = requestNode.get(TRACKING_ACCESS_UNIX_TIME_COLUMN).asLong();
        }
        else
        {
            return "Missing " + TRACKING_ACCESS_UNIX_TIME_COLUMN;
        }

        String objectType = "";
        if (requestNode.has(TRACKING_OBJECT_TYPE_COLUMN))
        {
            objectType = requestNode.get(TRACKING_OBJECT_TYPE_COLUMN).asText();
        }
        else
        {
            return "Missing " + TRACKING_OBJECT_TYPE_COLUMN;
        }


        Long objectId = 0L;
        if (requestNode.has(TRACKING_OBJECT_ID_COLUMN))
        {
            objectId = requestNode.get(TRACKING_OBJECT_ID_COLUMN).asLong();
        }
        else
        {
            return "Missing " + TRACKING_OBJECT_ID_COLUMN;
        }

        String objectName = "";
        if (requestNode.has(TRACKING_OBJECT_NAME_COLUMN))
        {
            objectName = requestNode.get(TRACKING_OBJECT_NAME_COLUMN).asText();
        }
        else
        {
            return "Missing " + TRACKING_OBJECT_NAME_COLUMN;
        }

        String parameters = "";
        if (requestNode.has(TRACKING_PARAMETERS_COLUMN))
        {
            parameters = requestNode.get(TRACKING_PARAMETERS_COLUMN).asText();
        }
        else
        {
            return "Missing " + TRACKING_PARAMETERS_COLUMN;
        }

        Integer userId = 0;
        if (StringUtils.isNotBlank(user))
        {
            try
            {
                userId = (Integer)getJdbcTemplate().queryForObject(
                        GET_USER_ID,
                        Integer.class,
                        user);
            }
            catch(EmptyResultDataAccessException e)
            {
                Logger.error("TrackingDAO addTrackingEvent get user id failed, username = " + user);
                Logger.error("Exception = " + e.getMessage());
            }
        }

        if (userId != null && userId > 0)
        {
            try
            {
                int row = getJdbcTemplate().update(
                        ADD_TRACKING_EVENT,
                        accessTime,
                        userId,
                        objectType,
                        objectId,
                        objectName,
                        parameters);
                if (row > 0) {
                    message = "";
                }
            }
            catch (Exception e)
            {
                message = e.getMessage();
            }
        }
        else
        {
            message = "User not found";
        }
        return message;
    }

}
