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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import models.daos.DatasetDao;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import play.Logger;
import play.libs.Json;

import play.mvc.Http;
import play.mvc.Result;
import play.test.FakeApplication;

import java.lang.Exception;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static play.test.Helpers.*;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;


public class DatasetControllerTest {

    public static FakeApplication app;
    private final Http.Request request = mock(Http.Request.class);
    public static String DB_DEFAULT_DRIVER = "db.default.driver";
    public static String DB_DEFAULT_URL = "db.default.url";
    public static String DB_DEFAULT_USER = "db.default.user";
    public static String DB_DEFAULT_PASSWORD = "db.default.password";
    public static String MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";
    public static String DATABASE_WHEREHOWS_OPENSOURCE_USER_NAME = "wherehows";
    public static String DATABASE_WHEREHOWS_OPENSOURCE_USER_PASSWORD = "wherehows";
    public static String DATABASE_WHEREHOWS_OPENSOURCE_URL = "jdbc:mysql://localhost/wherehows";

    @BeforeClass
    public static void startApp() {
        HashMap<String, String> mysql = new HashMap<String, String>();
        mysql.put(DB_DEFAULT_DRIVER, MYSQL_DRIVER_CLASS);
        mysql.put(DB_DEFAULT_URL, DATABASE_WHEREHOWS_OPENSOURCE_URL);
        mysql.put(DB_DEFAULT_USER, DATABASE_WHEREHOWS_OPENSOURCE_USER_NAME);
        mysql.put(DB_DEFAULT_PASSWORD, DATABASE_WHEREHOWS_OPENSOURCE_USER_PASSWORD);
        app = fakeApplication(mysql);
        start(app);
    }

    @Before
    public void setUp() throws Exception {
        Map<String, String> flashData = Collections.emptyMap();
        Map<String, Object> argData = Collections.emptyMap();
        Long id = 2L;
        play.api.mvc.RequestHeader header = mock(play.api.mvc.RequestHeader.class);
        Http.Context context = new Http.Context(id, header, request, flashData, flashData, argData);
        Http.Context.current.set(context);
    }

    @Test
    public void testDataset()
    {
        ObjectNode inputJson = Json.newObject();
        inputJson.put("dataset_uri", "dalids:///feedimpressionevent_mp/feedimpressionevent");
        try
        {
            ObjectNode resultNode = DatasetDao.getDatasetDependency(
                    inputJson);
            assertThat(resultNode.isContainerNode());
        }
        catch (Exception e)
        {
            assertThat(false);
        }
    }

    @AfterClass
    public static void stopApp() {
        stop(app);
    }
}
