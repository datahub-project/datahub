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
import dao.JiraDAO;
import dao.UserDAO;
import models.JiraTicket;
import org.apache.commons.lang3.StringUtils;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Jira extends Controller
{
    public static String HEADLESS_COMPONENT = "Auto Purge Program - Headless";

    public static Result getLdapInfo()
    {
        ObjectNode result = Json.newObject();

        result.put("status", "ok");
        result.set("ldapinfo", Json.toJson(JiraDAO.getLdapInfo()));
        return ok(result);
    }

    public static Result getFirstLevelLdapInfo(String managerId)
    {
        ObjectNode result = Json.newObject();

        result.put("status", "ok");
        result.set("currentUser", Json.toJson(JiraDAO.getCurrentUserLdapInfo(managerId)));
        result.set("members", Json.toJson(JiraDAO.getFirstLevelLdapInfo(managerId)));
        return ok(result);
    }

    public static Result getJiraTickets(String managerId)
    {
        ObjectNode result = Json.newObject();

        List<JiraTicket> headlessTickets = new ArrayList<JiraTicket>();
        List<JiraTicket> userTickets = new ArrayList<JiraTicket>();
        List<JiraTicket> tickets = JiraDAO.getUserTicketsByManagerId(managerId);
        if ( tickets != null && tickets.size() > 0)
        {
            for( JiraTicket ticket : tickets)
            {
                if (ticket.ticketComponent.equalsIgnoreCase(HEADLESS_COMPONENT))
                {
                    headlessTickets.add(ticket);
                }
                else
                {
                    userTickets.add(ticket);
                }
            }
        }
        result.put("status", "ok");
        result.set("headlessTickets", Json.toJson(headlessTickets));
        result.set("userTickets", Json.toJson(userTickets));
        return ok(result);
    }

}
