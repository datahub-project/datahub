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
import dao.UserDAO;
import play.data.DynamicForm;
import play.mvc.Controller;
import play.mvc.Result;
import play.Logger;
import play.mvc.Security;
import utils.Tree;
import views.html.index;
import views.html.login;
import views.html.lineage;
import views.html.schemaHistory;
import static play.data.Form.form;
import org.apache.commons.lang3.StringUtils;
import security.AuthenticationManager;

public class Application extends Controller
{
    private static String TREE_NAME_SUBFIX = ".tree.name";

    @Security.Authenticated(Secured.class)
    public static Result index()
    {
        String username = session("user");
        if (username == null)
        {
            username = "";
        }
        //You cann generate the Csrf token such as String csrfToken = SecurityPlugin.getInstance().getCsrfToken();
        String csrfToken = "";
        return ok(index.render(username, csrfToken));
    }

    @Security.Authenticated(Secured.class)
    public static Result lineage()
    {
        String username = session("user");
        if (username == null)
        {
            username = "";
        }
        return ok(lineage.render(username, "chains", 0, null, null, null));
    }

    @Security.Authenticated(Secured.class)
    public static Result datasetLineage(int id)
    {
        String username = session("user");
        if (username == null)
        {
            username = "";
        }
        return ok(lineage.render(username, "dataset", id, null, null, null));
    }

    @Security.Authenticated(Secured.class)
    public static Result metricLineage(int id)
    {
        String username = session("user");
        if (username == null)
        {
            username = "";
        }
        return ok(lineage.render(username, "metric", id, null, null, null));
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
        return ok(lineage.render(username, type, 0, application.replace(" ", "."), project, flow));
    }

    @Security.Authenticated(Secured.class)
    public static Result schemaHistory()
    {
        String username = session("user");
        if (username == null)
        {
            username = "";
        }
        return ok(schemaHistory.render(username));
    }

    public static Result login()
    {
        //You cann generate the Csrf token such as String csrfToken = SecurityPlugin.getInstance().getCsrfToken();
        String csrfToken = "";
        return ok(login.render(csrfToken));
    }

    public static Result authenticate()
    {
        DynamicForm loginForm = form().bindFromRequest();
        String username = loginForm.get("username");
        String password = loginForm.get("password");

        if (StringUtils.isBlank(username) || StringUtils.isBlank(password))
        {
            flash("error", "Invalid username or password");
            return redirect(controllers.routes.Application.login());
        }

        try
        {
            AuthenticationManager.authenticateUser(username, password);
        }
        catch (Exception e)
        {
            Logger.error("Authentication failed for user " + username);
            Logger.error(e.getMessage());
            flash("error", "Invalid username or password");
            return redirect(controllers.routes.Application.login());
        }
        session().clear();
        session("user", username);
        return redirect(controllers.routes.Application.index());
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
        flash("success", "You've been logged out");
        return redirect(controllers.routes.Application.login());
    }

    public static Result loadTree(String key)
    {
        if (StringUtils.isNotBlank(key) && key.equalsIgnoreCase("flows"))
        {
            return ok(FlowsDAO.getFlowApplicationNodes());
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

}
