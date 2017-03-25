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
import views.html.*;

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
    private static String LINKEDIN_INTERNAL_KEY = "linkedin.internal";
    private static String PIWIK_SITE_ID = "tracking.piwik.siteid";

    @Security.Authenticated(Secured.class)
    public static Result index()
    {
        Boolean isInternal = Play.application().configuration().getBoolean(LINKEDIN_INTERNAL_KEY, false);
        Integer piwikSiteId = Play.application().configuration().getInt(PIWIK_SITE_ID);
        String username = session("user");
        if (username == null)
        {
            username = "";
        }
        //You cann generate the Csrf token such as String csrfToken = SecurityPlugin.getInstance().getCsrfToken();
        String csrfToken = "";
        return ok(index.render(username, csrfToken, isInternal, piwikSiteId));
    }

    @Security.Authenticated(Secured.class)
    public static Result lineage()
    {
        Boolean isInternal = Play.application().configuration().getBoolean(LINKEDIN_INTERNAL_KEY, false);
        Integer piwikSiteId = Play.application().configuration().getInt(PIWIK_SITE_ID);
        String username = session("user");
        if (username == null)
        {
            username = "";
        }
        return ok(lineage.render(username, isInternal, "chains", 0, null, null, null, piwikSiteId));
    }

    @Security.Authenticated(Secured.class)
    public static Result datasetLineage(int id)
    {
        Boolean isInternal = Play.application().configuration().getBoolean(LINKEDIN_INTERNAL_KEY, false);
        Integer piwikSiteId = Play.application().configuration().getInt(PIWIK_SITE_ID);
        String username = session("user");
        if (username == null)
        {
            username = "";
        }
        return ok(lineage.render(username, isInternal, "dataset", id, null, null, null, piwikSiteId));
    }

    @Security.Authenticated(Secured.class)
    public static Result metricLineage(int id)
    {
        Boolean isInternal = Play.application().configuration().getBoolean(LINKEDIN_INTERNAL_KEY, false);
        Integer piwikSiteId = Play.application().configuration().getInt(PIWIK_SITE_ID);
        String username = session("user");
        if (username == null)
        {
            username = "";
        }
        return ok(lineage.render(username, isInternal, "metric", id, null, null, null, piwikSiteId));
    }

    @Security.Authenticated(Secured.class)
    public static Result flowLineage(String application, String project, String flow)
    {
        Boolean isInternal = Play.application().configuration().getBoolean(LINKEDIN_INTERNAL_KEY, false);
        Integer piwikSiteId = Play.application().configuration().getInt(PIWIK_SITE_ID);
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
        return ok(lineage.render(username, isInternal, type, 0, application.replace(" ", "."), project, flow, piwikSiteId));
    }

    @Security.Authenticated(Secured.class)
    public static Result schemaHistory()
    {
        Boolean isInternal = Play.application().configuration().getBoolean(LINKEDIN_INTERNAL_KEY, false);
        Integer piwikSiteId = Play.application().configuration().getInt(PIWIK_SITE_ID);
        String username = session("user");
        if (username == null)
        {
            username = "";
        }
        return ok(schemaHistory.render(username, isInternal, piwikSiteId));
    }

    @Security.Authenticated(Secured.class)
    public static Result scriptFinder()
    {
        Boolean isInternal = Play.application().configuration().getBoolean(LINKEDIN_INTERNAL_KEY, false);
        Integer piwikSiteId = Play.application().configuration().getInt(PIWIK_SITE_ID);
        String username = session("user");
        if (username == null)
        {
            username = "";
        }

        return ok(scriptFinder.render(username, isInternal, piwikSiteId));
    }

    @Security.Authenticated(Secured.class)
    public static Result idpc()
    {
        Boolean isInternal = Play.application().configuration().getBoolean(LINKEDIN_INTERNAL_KEY, false);
        Integer piwikSiteId = Play.application().configuration().getInt(PIWIK_SITE_ID);
        String username = session("user");
        if (username == null)
        {
            username = "";
        }
        return ok(idpc.render(username, isInternal, piwikSiteId));
    }

    @Security.Authenticated(Secured.class)
    public static Result dashboard()
    {
        Boolean isInternal = Play.application().configuration().getBoolean(LINKEDIN_INTERNAL_KEY, false);
        Integer piwikSiteId = Play.application().configuration().getInt(PIWIK_SITE_ID);
        String username = session("user");
        if (username == null)
        {
            username = "";
        }
        return ok(dashboard.render(username, isInternal, piwikSiteId));
    }

    public static Result login()
    {
        Boolean isInternal = Play.application().configuration().getBoolean(LINKEDIN_INTERNAL_KEY, false);
        Integer piwikSiteId = Play.application().configuration().getInt(PIWIK_SITE_ID);
        //You cann generate the Csrf token such as String csrfToken = SecurityPlugin.getInstance().getCsrfToken();
        String csrfToken = "";
        return ok(login.render(csrfToken, isInternal, piwikSiteId));
    }

    @BodyParser.Of(BodyParser.Json.class)
    public static Result authenticate() {
        // Create a new reponse ObjectNode to return when authenticate
        //   requst is successful
        ObjectNode response = Json.newObject();
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

        session().clear();

        // Contruct an ObjectNode with the username and uuid token to be sent with the response
        ObjectNode data = Json.newObject();
        data.put("username", username);
        data.put("uuid", uuid);
        response.put("status", "ok");
        response.put("data", data);

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

        return redirect(controllers.routes.Application.login());
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
