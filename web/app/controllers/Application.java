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

import java.io.InputStream;

import dao.FlowsDAO;
import dao.MetricsDAO;
import dao.UserDAO;
import play.Play;
import play.data.DynamicForm;
import play.mvc.Controller;
import play.mvc.Result;
import play.Logger;
import play.mvc.Security;
import utils.Tree;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import play.libs.Json;
import play.mvc.BodyParser;

import static play.data.Form.form;
import org.apache.commons.lang3.StringUtils;
import security.AuthenticationManager;

public class Application extends Controller
{
    private static String TREE_NAME_SUBFIX = ".tree.name";
    private static final Integer PIWIK_SITE_ID = Play.application().configuration().getInt("tracking.piwik.siteid");
    private static final String PIWIK_URL = Play.application().configuration().getString("tracking.piwik.url");
    private static final Boolean IS_INTERNAL = Play.application().configuration().getBoolean("linkedin.internal", false);
    private static final InputStream indexHtml = Play.application().classloader().getResourceAsStream("public/index.html");


    /**
     * Serves the build output index.html for any given path
     * @param path takes a path string, which essentially is ignored
     *             routing is managed client side
     * @return {Result} build output index.html resource
     */
    private static Result serveAsset(String path) {
        response().setHeader("Cache-Control", "no-cache");

        return ok(indexHtml).as("text/html");
    }

    public static Result healthcheck()
    {
        return ok("GOOD");
    }

    /**
     * index Action proxies to serveAsset
     * @param path takes a path string which is either index.html or the path segment after /
     * @return {Result} response from serveAsset method
     */
    public static Result index(String path) {
        return serveAsset("");
    }

    /**
     * Creates a wrapping ObjectNode containing config information
     *
     * @return Http Result instance with app configuration attributes
     */
    public static Result appConfig() {
        ObjectNode response = Json.newObject();
        ObjectNode config = Json.newObject();

        config.put("isInternal", IS_INTERNAL);
        config.put("tracking", trackingInfo());
        response.put("status", "ok");
        response.put("config", config);

        return ok(response);
    }

    /**
     * @return Json object containing the tracking configuration details
     */
    private static ObjectNode trackingInfo() {
        ObjectNode trackingConfig = Json.newObject();
        ObjectNode trackers = Json.newObject();
        ObjectNode piwik = Json.newObject();

        piwik.put("piwikSiteId", PIWIK_SITE_ID);
        piwik.put("piwikUrl", PIWIK_URL);
        trackers.put("piwik", piwik);
        trackingConfig.put("trackers", trackers);
        trackingConfig.put("isEnabled", true);

        return trackingConfig;
    }

    @Security.Authenticated(Secured.class)
    public static Result lineage()
    {
        String username = session("user");
        if (username == null)
        {
            username = "";
        }
        return serveAsset("");
    }

    @Security.Authenticated(Secured.class)
    public static Result datasetLineage(int id)
    {
        String username = session("user");
        if (username == null)
        {
            username = "";
        }

        return serveAsset("");
    }

    @Security.Authenticated(Secured.class)
    public static Result metricLineage(int id)
    {
        String username = session("user");
        if (username == null)
        {
            username = "";
        }

        return serveAsset("");
    }

    @Security.Authenticated(Secured.class)
    public static Result flowLineage(String application, String project, String flow)
    {
        String username = session("user");
        if (username == null)
        {
            username = "";
        }
        String type = "azkaban";
        if (StringUtils.isNotBlank(application) && (application.toLowerCase().indexOf("appworx") != -1))
        {
            type = "appworx";

        }

        return serveAsset("");
    }

    @Security.Authenticated(Secured.class)
    public static Result schemaHistory()
    {
        String username = session("user");
        if (username == null)
        {
            username = "";
        }

        return serveAsset("");
    }

    @Security.Authenticated(Secured.class)
    public static Result scriptFinder()
    {
        String username = session("user");
        if (username == null)
        {
            username = "";
        }

        return serveAsset("");
    }

    @Security.Authenticated(Secured.class)
    public static Result idpc()
    {
        String username = session("user");
        if (username == null)
        {
            username = "";
        }
        return serveAsset("");
    }

    @Security.Authenticated(Secured.class)
    public static Result dashboard()
    {
        String username = session("user");
        if (username == null)
        {
            username = "";
        }
        return serveAsset("");
    }

    public static Result login()
    {
        //You cann generate the Csrf token such as String csrfToken = SecurityPlugin.getInstance().getCsrfToken();
        String csrfToken = "";
        return serveAsset("");
    }

    @BodyParser.Of(BodyParser.Json.class)
    public static Result authenticate() {
        JsonNode json = request().body().asJson();
        // Extract username and password as String from JsonNode,
        //   null if they are not strings
        String username = json.findPath("username").textValue();
        String password = json.findPath("password").textValue();

        if (username == null || StringUtils.isBlank(username)) {
            return badRequest("Missing or invalid [username]");
        }
        if (password == null || StringUtils.isBlank(password)) {
            return badRequest("Missing or invalid [credentials]");
        }

        session().clear();

        // Create a uuid string for this session if one doesn't already exist
        //   to be appended to the Result object
        String uuid = session("uuid");
        if (uuid == null) {
            uuid = java.util.UUID.randomUUID().toString();
            session("uuid", uuid);
        }

        try {
            AuthenticationManager.authenticateUser(username, password);
        } catch (Exception e) {
            return badRequest("Invalid credentials");
        }

        // Adds the username to the session cookie
        session("user", username);
        // Construct an ObjectNode with the username and uuid token to be sent with the response
        ObjectNode data = Json.newObject();
        data.put("username", username);
        data.put("uuid", uuid);

        // Create a new response ObjectNode to return when authenticate request is successful
        ObjectNode response = Json.newObject();
        response.put("status", "ok");
        response.set("data", data);
        return ok(response);
    }

    public static Result signUp()
    {
        DynamicForm loginForm = form().bindFromRequest();
        String username = loginForm.get("inputName");
        String firstName = loginForm.get("inputFirstName");
        String lastName = loginForm.get("inputLastName");
        String email = loginForm.get("inputEmail");
        String password = loginForm.get("inputPassword");
        String errorMessage = "";
        try
        {
            errorMessage = UserDAO.signUp(username, firstName, lastName, email, password);
            if (StringUtils.isNotBlank(errorMessage))
            {
                flash("error", errorMessage);
            }
            else
            {
                flash("success", "Congratulations! Your account has been created. Please login.");
            }
        }
        catch (Exception e)
        {
            flash("error", e.getMessage());
        }

        return serveAsset("");
    }

    public static Result logout()
    {
        session().clear();
        return ok();
    }

    public static Result loadTree(String key)
    {
        if (StringUtils.isNotBlank(key) && key.equalsIgnoreCase("flows"))
        {
            return ok(FlowsDAO.getFlowApplicationNodes());
        }
        else if (StringUtils.isNotBlank(key) && key.equalsIgnoreCase("metrics"))
        {
            return ok(MetricsDAO.getMetricDashboardNodes());
        }
        return ok(Tree.loadTreeJsonNode(key + TREE_NAME_SUBFIX));
    }

    public static Result loadFlowProjects(String app)
    {
        return ok(FlowsDAO.getFlowProjectNodes(app));
    }

    public static Result loadFlowNodes(String app, String project)
    {
        return ok(FlowsDAO.getFlowNodes(app, project));
    }

    public static Result loadMetricGroups(String dashboard)
    {
        return ok(MetricsDAO.getMetricGroupNodes(dashboard));
    }

    public static Result loadMetricNodes(String dashboard, String group)
    {
        return ok(MetricsDAO.getMetricNodes(dashboard, group));
    }

}
