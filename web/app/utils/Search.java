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
package utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;

import play.Logger;
import play.cache.Cache;
import play.libs.Json;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

public class Search
{

    private final static String datasetMustQueryUnit =
            "{\"bool\": " +
                "{\"must\":[" +
                    "{\"bool\":" +
                        "{\"should\":" +
                            "[{\"bool\":{\"should\":[{\"wildcard\":{\"name\":{\"value\":\"*$VALUE*\",\"boost\":16}}}," +
                            "{\"prefix\":{\"name\":{\"value\":\"$VALUE\",\"boost\":32}}}," +
                            "{\"match\":{\"name\":{\"query\":\"$VALUE\",\"boost\":48}}}," +
                            "{\"match\":{\"name\":{\"query\":\"$VALUE\",\"type\":\"phrase\",\"boost\":64}}}," +
                            "{\"wildcard\":{\"urn\":{\"value\":\"*$VALUE*\",\"boost\":8}}}," +
                            "{\"wildcard\":{\"field\":{\"value\":\"*$VALUE*\",\"boost\":4}}}," +
                            "{\"wildcard\":{\"properties\":{\"value\":\"*$VALUE*\",\"boost\":2}}}," +
                            "{\"wildcard\":{\"schema\":{\"value\":\"*$VALUE*\",\"boost\":1}}}]}}]}}, " +
                    "{\"match\":{\"source\": \"$SOURCE\"}}]}}";

    private final static String datasetShouldQueryUnit =
            "{\"bool\": " +
                    "{\"should\": [" +
                    "{\"wildcard\": {\"name\": {\"value\": \"*$VALUE*\", \"boost\": 16}}}, " +
                    "{\"prefix\": {\"name\": {\"value\": \"$VALUE\", \"boost\": 32}}}, " +
                    "{\"match\": {\"name\": {\"query\": \"$VALUE\", \"boost\": 48}}}, " +
                    "{\"match\": {\"name\": {\"query\": \"$VALUE\", \"type\": \"phrase\", \"boost\": 64}}}, " +
                    "{\"wildcard\": {\"urn\": {\"value\": \"*$VALUE*\", \"boost\": 8}}}, " +
                    "{\"wildcard\": {\"field\": {\"value\": \"*$VALUE*\", \"boost\": 4}}}, " +
                    "{\"wildcard\": {\"properties\": {\"value\": \"*$VALUE*\", \"boost\": 2}}}, " +
                    "{\"wildcard\": {\"schema\": {\"value\": \"*$VALUE*\", \"boost\": 1}}}" +
                    "]}" +
                    "}";


    private final static String metricShouldQueryUnit =
            "{\"bool\": " +
                "{\"should\": [" +
                    "{\"wildcard\": {\"metric_name\": {\"value\": \"*$VALUE*\", \"boost\": 32}}}, " +
                    "{\"prefix\": {\"metric_name\": {\"value\": \"$VALUE\", \"boost\": 36}}}, " +
                    "{\"match\": {\"metric_name\": {\"query\": \"$VALUE\", \"boost\": 48}}}, " +
                    "{\"match\": {\"metric_name\": {\"query\": \"$VALUE\", \"type\": \"phrase\", \"boost\": 64}}}, " +
                    "{\"wildcard\": {\"dashboard_name\": {\"value\": \"*$VALUE*\", \"boost\": 20}}}," +
                    "{\"prefix\": {\"dashboard_name\": {\"value\": \"$VALUE\", \"boost\": 24}}}, " +
                    "{\"match\": {\"dashboard_name\": {\"query\": \"$VALUE\", \"boost\": 26}}}, " +
                    "{\"match\": {\"dashboard_name\": {\"query\": \"$VALUE\", \"type\": \"phrase\", \"boost\": 28}}}, " +
                    "{\"wildcard\": {\"metric_group\": {\"value\": \"*$VALUE*\", \"boost\": 8}}}, " +
                    "{\"prefix\": {\"metric_group\": {\"value\": \"$VALUE\", \"boost\": 12}}}, " +
                    "{\"match\": {\"metric_group\": {\"query\": \"$VALUE\", \"boost\": 14}}}, " +
                    "{\"match\": {\"metric_group\": {\"query\": \"$VALUE\", \"type\": \"phrase\", \"boost\": 16}}}, " +
                    "{\"wildcard\": {\"metric_category\": {\"value\": \"*$VALUE*\", \"boost\": 1}}}, " +
                    "{\"prefix\": {\"metric_category\": {\"value\": \"$VALUE\", \"boost\": 2}}}, " +
                    "{\"match\": {\"metric_category\": {\"query\": \"$VALUE\", \"boost\": 3}}}, " +
                    "{\"match\": {\"metric_category\": {\"query\": \"$VALUE\", \"type\": \"phrase\", \"boost\": 4}}}" +
                    "]" +
                "}" +
            "}";

    private final static String commentsQuery =
            "{\"bool\":" +
                "{ \"should\": [" +
                    "{\"has_child\": {\"type\": \"comment\", \"query\": {\"match\" : {\"text\" : \"$VALUE\"}}}}, " +
                    "{\"has_child\": {\"type\": \"field\", \"query\": {\"match\": {\"comments\" : \"$VALUE\" }}}}" +
                    "]" +
                "}" +
            "}";

    private final static String flowShouldQueryUnit =
            "{\"bool\": " +
                "{\"should\": " +
                    "[{\"wildcard\": {\"app_code\": {\"value\": \"*$VALUE*\", \"boost\": 1}}}, " +
                    "{\"prefix\": {\"app_code\": {\"value\": \"$VALUE\", \"boost\": 2}}}, " +
                    "{\"match\": {\"app_code\": {\"query\": \"$VALUE\", \"boost\": 3}}}, " +
                    "{\"match\": {\"app_code\": {\"query\": \"$VALUE\", \"type\": \"phrase\", \"boost\": 4}}}, " +
                    "{\"wildcard\": {\"flow_name\": {\"value\": \"*$VALUE*\", \"boost\": 8}}}, " +
                    "{\"prefix\": {\"flow_name\": {\"value\": \"$VALUE\", \"boost\": 16}}}, " +
                    "{\"match\": {\"flow_name\": {\"query\": \"$VALUE\", \"boost\": 24}}}, " +
                    "{\"match\": {\"flow_name\": {\"query\": \"$VALUE\", \"type\": \"phrase\", \"boost\": 32}}}, " +
                    "{\"wildcard\": {\"jobs.job_name\": {\"value\": \"*$VALUE*\", \"boost\": 8}}}, " +
                    "{\"prefix\": {\"jobs.job_name\": {\"value\": \"$VALUE\", \"boost\": 16}}}, " +
                    "{\"match\": {\"jobs.job_name\": {\"query\": \"$VALUE\", \"boost\": 24}}}, " +
                    "{\"match\": {\"jobs.job_name\": {\"query\": \"$VALUE\", \"type\": \"phrase\", \"boost\": 32}}}" +
                    "]" +
                "}" +
            "}";

    public final static String DATASET_CATEGORY = "datasets";

    public final static String METRIC_CATEGORY = "metrics";

    public final static String COMMENT_CATEGORY = "comments";

    public final static String FLOW_CATEGORY = "flows";

    public final static String JOB_CATEGORY = "jobs";

    public static ObjectNode generateElasticSearchQueryString(String category, String source, String keywords)
    {
        if (StringUtils.isBlank(keywords))
            return null;

        List<JsonNode> shouldValueList = new ArrayList<JsonNode>();

        String queryTemplate = datasetShouldQueryUnit;
        String[] values = keywords.trim().split(",");
        if (StringUtils.isNotBlank(category))
        {
            if (category.equalsIgnoreCase(METRIC_CATEGORY))
            {
                queryTemplate = metricShouldQueryUnit;
            }
            else if (category.equalsIgnoreCase(COMMENT_CATEGORY))
            {
                queryTemplate = commentsQuery;
            }
            else if (category.equalsIgnoreCase(FLOW_CATEGORY) || category.equalsIgnoreCase(JOB_CATEGORY))
            {
                queryTemplate = flowShouldQueryUnit;
            }
            else if (category.equalsIgnoreCase(DATASET_CATEGORY) && StringUtils.isNotBlank(source))
            {
                queryTemplate = datasetMustQueryUnit;
            }
        }

        for(String value : values)
        {
            if (StringUtils.isNotBlank(value)) {
                String query= queryTemplate.replace("$VALUE", value.replace("\"", "").toLowerCase().trim());
                if (StringUtils.isNotBlank(source))
                {
                    query = query.replace("$SOURCE", source.toLowerCase());
                }
                shouldValueList.add(Json.parse(query));
            }
        }

        ObjectNode shouldNode = Json.newObject();
        shouldNode.set("should", Json.toJson(shouldValueList));
        ObjectNode queryNode = Json.newObject();
        queryNode.put("bool", shouldNode);
        return queryNode;
    }

    public static ObjectNode generateDatasetAdvSearchQueryString(JsonNode searchOpt)
    {
        List<String> scopeInList = new ArrayList<String>();
        List<String> scopeNotInList = new ArrayList<String>();
        List<String> tableInList = new ArrayList<String>();
        List<String> tableNotInList = new ArrayList<String>();
        List<String> fieldAnyList = new ArrayList<String>();
        List<String> fieldAllList = new ArrayList<String>();
        List<String> fieldNotInList = new ArrayList<String>();
        String comments = null;
        String datasetSources = null;

        if (searchOpt != null && (searchOpt.isContainerNode())) {
            if (searchOpt.has("scope")) {
                JsonNode scopeNode = searchOpt.get("scope");
                if (scopeNode != null && scopeNode.isContainerNode()) {
                    if (scopeNode.has("in")) {
                        JsonNode scopeInNode = scopeNode.get("in");
                        if (scopeInNode != null) {
                            String scopeInStr = scopeInNode.asText();
                            if (StringUtils.isNotBlank(scopeInStr)) {
                                String[] scopeInArray = scopeInStr.split(",");
                                if (scopeInArray != null) {
                                    for (String value : scopeInArray) {
                                        if (StringUtils.isNotBlank(value)) {
                                            scopeInList.add(value.trim());
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if (scopeNode.has("not")) {
                        JsonNode scopeNotInNode = scopeNode.get("not");
                        if (scopeNotInNode != null) {
                            String scopeNotInStr = scopeNotInNode.asText();
                            if (StringUtils.isNotBlank(scopeNotInStr)) {
                                String[] scopeNotInArray = scopeNotInStr.split(",");
                                if (scopeNotInArray != null) {
                                    for (String value : scopeNotInArray) {
                                        if (StringUtils.isNotBlank(value)) {
                                            scopeNotInList.add(value.trim());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if (searchOpt.has("table")) {
                JsonNode tableNode = searchOpt.get("table");
                if (tableNode != null && tableNode.isContainerNode()) {
                    if (tableNode.has("in")) {
                        JsonNode tableInNode = tableNode.get("in");
                        if (tableInNode != null) {
                            String tableInStr = tableInNode.asText();
                            if (StringUtils.isNotBlank(tableInStr)) {
                                String[] tableInArray = tableInStr.split(",");
                                if (tableInArray != null) {
                                    for (String value : tableInArray) {
                                        if (StringUtils.isNotBlank(value)) {
                                            tableInList.add(value.trim());
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if (tableNode.has("not")) {
                        JsonNode tableNotInNode = tableNode.get("not");
                        if (tableNotInNode != null) {
                            String tableNotInStr = tableNotInNode.asText();
                            if (StringUtils.isNotBlank(tableNotInStr)) {
                                String[] tableNotInArray = tableNotInStr.split(",");
                                if (tableNotInArray != null) {
                                    for (String value : tableNotInArray) {
                                        if (StringUtils.isNotBlank(value)) {
                                            tableNotInList.add(value.trim());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if (searchOpt.has("fields")) {
                JsonNode fieldNode = searchOpt.get("fields");
                if (fieldNode != null && fieldNode.isContainerNode()) {
                    if (fieldNode.has("any")) {
                        JsonNode fieldAnyNode = fieldNode.get("any");
                        if (fieldAnyNode != null) {
                            String fieldAnyStr = fieldAnyNode.asText();
                            if (StringUtils.isNotBlank(fieldAnyStr)) {
                                String[] fieldAnyArray = fieldAnyStr.split(",");
                                if (fieldAnyArray != null) {
                                    for (String value : fieldAnyArray) {
                                        if (StringUtils.isNotBlank(value)) {
                                            fieldAnyList.add(value.trim());
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if (fieldNode.has("all")) {
                        JsonNode fieldAllNode = fieldNode.get("all");
                        if (fieldAllNode != null) {
                            String fieldAllStr = fieldAllNode.asText();
                            if (StringUtils.isNotBlank(fieldAllStr)) {
                                String[] fieldAllArray = fieldAllStr.split(",");
                                if (fieldAllArray != null) {
                                    for (String value : fieldAllArray) {
                                        if (StringUtils.isNotBlank(value)) {
                                            fieldAllList.add(value.trim());
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if (fieldNode.has("not")) {
                        JsonNode fieldNotInNode = fieldNode.get("not");
                        if (fieldNotInNode != null) {
                            String fieldNotInStr = fieldNotInNode.asText();
                            if (StringUtils.isNotBlank(fieldNotInStr)) {
                                String[] fieldNotInArray = fieldNotInStr.split(",");
                                if (fieldNotInArray != null) {
                                    for (String value : fieldNotInArray) {
                                        if (StringUtils.isNotBlank(value)) {
                                            fieldNotInList.add(value.trim());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if (searchOpt.has("sources")) {
                JsonNode sourcesNode = searchOpt.get("sources");
                if (sourcesNode != null)
                {
                    datasetSources = sourcesNode.asText();
                }
            }
            if (searchOpt.has("comments")) {
                JsonNode commentsNode = searchOpt.get("comments");
                if (commentsNode != null) {
                    comments = commentsNode.asText();
                }
            }
        }

        List<JsonNode> mustValueList = new ArrayList<JsonNode>();
        List<JsonNode> mustNotValueList = new ArrayList<JsonNode>();
        List<JsonNode> shouldValueList = new ArrayList<JsonNode>();
        String hasChildValue = "{\"has_child\": " +
                "{\"type\": \"$TYPE\", \"query\": {\"match\" : {\"$FIELD\" : \"$VALUE\"}}}}";
        String matchQueryUnit = "{\"query\": {\"match\" : {\"$FIELD\" : \"$VALUE\"}}}";

        String shouldQueryUnit = "{\"bool\": " +
                "{\"should\": [" +
                    "{\"wildcard\": {\"$FIELD\": {\"value\": \"*$VALUE*\", \"boost\": 1}}}, " +
                    "{\"prefix\": {\"$FIELD\": {\"value\": \"$VALUE\", \"boost\": 4}}}, " +
                    "{\"term\": {\"$FIELD\": {\"value\": \"$VALUE\", \"boost\": 16}}}" +
                "]}}";
        String mustNotQueryUnit = "{\"term\" : {\"$FIELD\" : \"$VALUE\"}}";

        if (scopeInList != null && scopeInList.size() > 0)
        {
            for(String scope : scopeInList)
            {
                if (StringUtils.isNotBlank(scope))
                {
                    shouldValueList.add(
                            Json.parse(
                                    shouldQueryUnit.replace("$FIELD", "parent_name").
                                            replace("$VALUE", scope.toLowerCase().trim())));
                }
            }
            ObjectNode shouldNode = Json.newObject();
            ObjectNode boolNode = Json.newObject();
            shouldNode.set("should", Json.toJson(shouldValueList));
            boolNode.put("bool", shouldNode);
            mustValueList.add(boolNode);
        }
        if (scopeNotInList != null && scopeNotInList.size() > 0)
        {
            shouldValueList.clear();
            for(String scope: scopeNotInList)
            {
                if (StringUtils.isNotBlank(scope))
                {
                    shouldValueList.add(
                            Json.parse(
                                    mustNotQueryUnit.replace("$FIELD", "parent_name").
                                            replace("$VALUE", scope.toLowerCase().trim())));
                }
            }
            ObjectNode shouldNode = Json.newObject();
            ObjectNode boolNode = Json.newObject();
            shouldNode.set("should", Json.toJson(shouldValueList));
            boolNode.put("bool", shouldNode);
            mustNotValueList.add(boolNode);
        }
        if (tableInList != null && tableInList.size() > 0)
        {
            shouldValueList.clear();
            for(String table : tableInList)
            {
                if (StringUtils.isNotBlank(table))
                {
                    shouldValueList.add(
                            Json.parse(
                                    shouldQueryUnit.replace("$FIELD", "name").
                                            replace("$VALUE", table.toLowerCase().trim())));
                }
            }
            ObjectNode shouldNode = Json.newObject();
            ObjectNode boolNode = Json.newObject();
            shouldNode.set("should", Json.toJson(shouldValueList));
            boolNode.put("bool", shouldNode);
            mustValueList.add(boolNode);
        }
        if (tableNotInList != null && tableNotInList.size() > 0)
        {
            shouldValueList.clear();
            for(String table : tableNotInList)
            {
                if (StringUtils.isNotBlank(table))
                {
                    shouldValueList.add(
                            Json.parse(
                                    mustNotQueryUnit.replace("$FIELD", "name").
                                            replace("$VALUE", table.toLowerCase().trim())));
                }
            }
            ObjectNode shouldNode = Json.newObject();
            ObjectNode boolNode = Json.newObject();
            shouldNode.set("should", Json.toJson(shouldValueList));
            boolNode.put("bool", shouldNode);
            mustNotValueList.add(boolNode);
        }
        if (fieldAnyList != null && fieldAnyList.size() > 0)
        {
            shouldValueList.clear();
            for(String field : fieldAnyList)
            {
                if (StringUtils.isNotBlank(field))
                {
                    shouldValueList.add(
                            Json.parse(
                                    hasChildValue.replace("$TYPE", "field").
                                            replace("$FIELD", "field_name").
                                            replace("$VALUE", field.toLowerCase().trim())));
                }
            }
            ObjectNode shouldNode = Json.newObject();
            ObjectNode boolNode = Json.newObject();
            shouldNode.set("should", Json.toJson(shouldValueList));
            boolNode.put("bool", shouldNode);
            mustValueList.add(boolNode);
        }
        if (fieldAllList != null && fieldAllList.size() > 0)
        {
            for(String field : fieldAllList)
            {
                if (StringUtils.isNotBlank(field))
                {
                    mustValueList.add(
                            Json.parse(
                                    hasChildValue.replace("$TYPE", "field").
                                            replace("$FIELD", "field_name").
                                            replace("$VALUE", field.toLowerCase().trim())));
                }
            }
        }
        if (fieldNotInList != null && fieldNotInList.size() > 0)
        {
            shouldValueList.clear();
            for(String field : fieldNotInList)
            {
                if (StringUtils.isNotBlank(field))
                {
                    shouldValueList.add(
                            Json.parse(
                                    hasChildValue.replace("$TYPE", "field").
                                            replace("$FIELD", "field_name").
                                            replace("$VALUE", field.toLowerCase().trim())));
                }
            }
            ObjectNode shouldNode = Json.newObject();
            ObjectNode boolNode = Json.newObject();
            shouldNode.set("should", Json.toJson(shouldValueList));
            boolNode.put("bool", shouldNode);
            mustNotValueList.add(boolNode);
        }
        if (StringUtils.isNotBlank(comments))
        {
            shouldValueList.clear();
            String inputComments[] = comments.trim().split(",");
            for(String comment : inputComments)
            {
                if (StringUtils.isNotBlank(comment))
                {
                    shouldValueList.add(
                            Json.parse(
                                    hasChildValue.replace("$TYPE", "comment").
                                            replace("$FIELD", "text").
                                            replace("$VALUE", comment.toLowerCase().trim())));
                    shouldValueList.add(
                            Json.parse(
                                    hasChildValue.replace("$TYPE", "field").
                                            replace("$FIELD", "comments").
                                            replace("$VALUE", comment.toLowerCase().trim())));
                }
            }
            ObjectNode shouldNode = Json.newObject();
            ObjectNode boolNode = Json.newObject();
            shouldNode.set("should", Json.toJson(shouldValueList));
            boolNode.put("bool", shouldNode);
            mustValueList.add(boolNode);
        }
        if (StringUtils.isNotBlank(datasetSources))
        {
            String inputSources[] = datasetSources.trim().split(",");
            shouldValueList.clear();
            for(String source : inputSources)
            {
                if (StringUtils.isNotBlank(source))
                {
                    shouldValueList.add(
                            Json.parse(
                                    matchQueryUnit.replace("$FIELD", "source").
                                            replace("$VALUE", source.toLowerCase().trim())));
                }
            }
            ObjectNode shouldNode = Json.newObject();
            ObjectNode boolNode = Json.newObject();
            shouldNode.set("should", Json.toJson(shouldValueList));
            boolNode.put("bool", shouldNode);
            mustValueList.add(boolNode);
        }

        ObjectNode boolNode = Json.newObject();
        ObjectNode queryNode = Json.newObject();
        if (mustValueList.size() > 0 && mustNotValueList.size() > 0)
        {
            boolNode.set("must", Json.toJson(mustValueList));
            boolNode.set("must_not", Json.toJson(mustNotValueList));
            queryNode.put("bool", boolNode);
        }
        else if (mustValueList.size() > 0)
        {
            boolNode.set("must", Json.toJson(mustValueList));
            queryNode.put("bool", boolNode);
        }
        else if (mustNotValueList.size() > 0)
        {
            boolNode.set("must_not", Json.toJson(mustNotValueList));
            queryNode.put("bool", boolNode);
        }

        return queryNode;
    }

    public static ObjectNode generateMetricAdvSearchQueryString(JsonNode searchOpt)
    {
        List<String> dashboardInList = new ArrayList<String>();
        List<String> dashboardNotInList = new ArrayList<String>();
        List<String> groupInList = new ArrayList<String>();
        List<String> groupNotInList = new ArrayList<String>();
        List<String> categoryInList = new ArrayList<String>();
        List<String> categoryNotInList = new ArrayList<String>();
        List<String> metricInList = new ArrayList<String>();
        List<String> metricNotInList = new ArrayList<String>();

        if (searchOpt != null && (searchOpt.isContainerNode())) {
            if (searchOpt.has("dashboard")) {
                JsonNode dashboardNode = searchOpt.get("dashboard");
                if (dashboardNode != null && dashboardNode.isContainerNode()) {
                    if (dashboardNode.has("in")) {
                        JsonNode dashboardInNode = dashboardNode.get("in");
                        if (dashboardInNode != null) {
                            String dashboardInStr = dashboardInNode.asText();
                            if (StringUtils.isNotBlank(dashboardInStr)) {
                                String[] dashboardInArray = dashboardInStr.split(",");
                                if (dashboardInArray != null) {
                                    for (String value : dashboardInArray) {
                                        if (StringUtils.isNotBlank(value)) {
                                            dashboardInList.add(value.trim());
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if (dashboardNode.has("not")) {
                        JsonNode dashboardNotInNode = dashboardNode.get("not");
                        if (dashboardNotInNode != null) {
                            String dashboardNotInStr = dashboardNotInNode.asText();
                            if (StringUtils.isNotBlank(dashboardNotInStr)) {
                                String[] dashboardNotInArray = dashboardNotInStr.split(",");
                                if (dashboardNotInArray != null) {
                                    for (String value : dashboardNotInArray) {
                                        if (StringUtils.isNotBlank(value)) {
                                            dashboardNotInList.add(value.trim());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if (searchOpt.has("group")) {
                JsonNode groupNode = searchOpt.get("group");
                if (groupNode != null && groupNode.isContainerNode()) {
                    if (groupNode.has("in")) {
                        JsonNode groupInNode = groupNode.get("in");
                        if (groupInNode != null) {
                            String groupInStr = groupInNode.asText();
                            if (StringUtils.isNotBlank(groupInStr)) {
                                String[] groupInArray = groupInStr.split(",");
                                if (groupInArray != null) {
                                    for (String value : groupInArray) {
                                        if (StringUtils.isNotBlank(value)) {
                                            groupInList.add(value.trim());
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if (groupNode.has("not")) {
                        JsonNode groupNotInNode = groupNode.get("not");
                        if (groupNotInNode != null) {
                            String groupNotInStr = groupNotInNode.asText();
                            if (StringUtils.isNotBlank(groupNotInStr)) {
                                String[] groupNotInArray = groupNotInStr.split(",");
                                if (groupNotInArray != null) {
                                    for (String value : groupNotInArray) {
                                        if (StringUtils.isNotBlank(value)) {
                                            groupNotInList.add(value.trim());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if (searchOpt.has("cat")) {
                JsonNode categoryNode = searchOpt.get("cat");
                if (categoryNode != null && categoryNode.isContainerNode()) {
                    if (categoryNode.has("in")) {
                        JsonNode categoryInNode = categoryNode.get("in");
                        if (categoryInNode != null) {
                            String categoryInStr = categoryInNode.asText();
                            if (StringUtils.isNotBlank(categoryInStr)) {
                                String[] categoryInArray = categoryInStr.split(",");
                                if (categoryInArray != null) {
                                    for (String value : categoryInArray) {
                                        if (StringUtils.isNotBlank(value)) {
                                            categoryInList.add(value.trim());
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if (categoryNode.has("not")) {
                        JsonNode categoryNotInNode = categoryNode.get("not");
                        if (categoryNotInNode != null) {
                            String categoryNotInStr = categoryNotInNode.asText();
                            if (StringUtils.isNotBlank(categoryNotInStr)) {
                                String[] categoryNotInArray = categoryNotInStr.split(",");
                                if (categoryNotInArray != null) {
                                    for (String value : categoryNotInArray) {
                                        if (StringUtils.isNotBlank(value)) {
                                            categoryNotInList.add(value.trim());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if (searchOpt.has("metric")) {
                JsonNode metricNode = searchOpt.get("metric");
                if (metricNode != null && metricNode.isContainerNode()) {
                    if (metricNode.has("in")) {
                        JsonNode metricInNode = metricNode.get("in");
                        if (metricInNode != null) {
                            String metricInStr = metricInNode.asText();
                            if (StringUtils.isNotBlank(metricInStr)) {
                                String[] metricInArray = metricInStr.split(",");
                                if (metricInArray != null) {
                                    for (String value : metricInArray) {
                                        if (StringUtils.isNotBlank(value)) {
                                            metricInList.add(value.trim());
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if (metricNode.has("not")) {
                        JsonNode metricNotInNode = metricNode.get("not");
                        if (metricNotInNode != null) {
                            String metricNotInStr = metricNotInNode.asText();
                            if (StringUtils.isNotBlank(metricNotInStr)) {
                                String[] metricNotInArray = metricNotInStr.split(",");
                                if (metricNotInArray != null) {
                                    for (String value : metricNotInArray) {
                                        if (StringUtils.isNotBlank(value)) {
                                            metricNotInList.add(value.trim());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        List<JsonNode> mustValueList = new ArrayList<JsonNode>();
        List<JsonNode> mustNotValueList= new ArrayList<JsonNode>();
        List<JsonNode> shouldValueList = new ArrayList<JsonNode>();

        String shouldQueryUnit =
                "{\"bool\": " +
                    "{\"should\": " +
                        "[{\"wildcard\": {\"$FIELD\": {\"value\": \"*$VALUE*\", \"boost\": 1}}}, " +
                        "{\"prefix\": {\"$FIELD\": {\"value\": \"$VALUE\", \"boost\": 4}}}, " +
                        "{\"term\": {\"$FIELD\": {\"value\": \"$VALUE\", \"boost\": 16}}}" +
                        "]" +
                    "}" +
                "}";
        String shouldMatchQueryUnit =
                "{\"bool\": " +
                    "{\"should\": " +
                        "[{\"wildcard\": {\"$FIELD\": {\"value\": \"*$VALUE*\", \"boost\": 1}}}, " +
                        "{\"prefix\": {\"$FIELD\": {\"value\": \"$VALUE\", \"boost\": 4}}}, " +
                        "{\"match\": {\"$FIELD\": {\"query\": \"$VALUE\", \"boost\": 16}}}" +
                        "]" +
                    "}" +
                "}";
        String mustNotQueryUnit = "{\"term\" : {\"$FIELD\" : \"$VALUE\"}}";

        if (dashboardInList != null && dashboardInList.size() > 0)
        {
            for(String dashboard : dashboardInList)
            {
                if (StringUtils.isNotBlank(dashboard))
                {
                    shouldValueList.add(
                            Json.parse(
                                    shouldMatchQueryUnit.replace("$FIELD", "dashboard_name").
                                            replace("$VALUE", dashboard.toLowerCase().trim())));
                }
            }
            ObjectNode shouldNode = Json.newObject();
            ObjectNode boolNode = Json.newObject();
            shouldNode.set("should", Json.toJson(shouldValueList));
            boolNode.put("bool", shouldNode);
            mustValueList.add(boolNode);
        }
        if (dashboardNotInList != null && dashboardNotInList.size() > 0)
        {
            shouldValueList.clear();
            for(String dashboard : dashboardNotInList)
            {
                if (StringUtils.isNotBlank(dashboard))
                {
                    shouldValueList.add(
                            Json.parse(
                                    mustNotQueryUnit.replace("$FIELD", "dashboard_name").
                                            replace("$VALUE", dashboard.toLowerCase().trim())));
                }
            }
            ObjectNode shouldNode = Json.newObject();
            ObjectNode boolNode = Json.newObject();
            shouldNode.set("should", Json.toJson(shouldValueList));
            boolNode.put("bool", shouldNode);
            mustNotValueList.add(boolNode);
        }
        if (groupInList != null && groupInList.size() > 0)
        {
            shouldValueList.clear();
            for(String group : groupInList)
            {
                if (StringUtils.isNotBlank(group))
                {
                    shouldValueList.add(
                            Json.parse(
                                    shouldMatchQueryUnit.replace("$FIELD", "metric_group").
                                            replace("$VALUE", group.toLowerCase().trim())));
                }
            }
            ObjectNode shouldNode = Json.newObject();
            ObjectNode boolNode = Json.newObject();
            shouldNode.set("should", Json.toJson(shouldValueList));
            boolNode.put("bool", shouldNode);
            mustValueList.add(boolNode);
        }
        if (groupNotInList != null && groupNotInList.size() > 0)
        {
            shouldValueList.clear();
            for(String group : groupNotInList)
            {
                if (StringUtils.isNotBlank(group))
                {
                    shouldValueList.add(
                            Json.parse(
                                    mustNotQueryUnit.replace("$FIELD", "metric_group").
                                            replace("$VALUE", group.toLowerCase().trim())));
                }
            }
            ObjectNode shouldNode = Json.newObject();
            ObjectNode boolNode = Json.newObject();
            shouldNode.set("should", Json.toJson(shouldValueList));
            boolNode.put("bool", shouldNode);
            mustNotValueList.add(boolNode);
        }

        if (categoryInList != null && categoryInList.size() > 0)
        {
            shouldValueList.clear();
            for(String category : categoryInList)
            {
                if (StringUtils.isNotBlank(category))
                {
                    shouldValueList.add(
                            Json.parse(
                                    shouldMatchQueryUnit.replace("$FIELD", "metric_category").
                                            replace("$VALUE", category.toLowerCase().trim())));
                }
            }
            ObjectNode shouldNode = Json.newObject();
            ObjectNode boolNode = Json.newObject();
            shouldNode.set("should", Json.toJson(shouldValueList));
            boolNode.put("bool", shouldNode);
            mustValueList.add(boolNode);
        }
        if (categoryNotInList != null && categoryNotInList.size() > 0)
        {
            shouldValueList.clear();
            for(String category : categoryNotInList)
            {
                if (StringUtils.isNotBlank(category))
                {
                    shouldValueList.add(
                            Json.parse(
                                    mustNotQueryUnit.replace("$FIELD", "metric_category").
                                            replace("$VALUE", category.toLowerCase().trim())));
                }
            }
            ObjectNode shouldNode = Json.newObject();
            ObjectNode boolNode = Json.newObject();
            shouldNode.set("should", Json.toJson(shouldValueList));
            boolNode.put("bool", shouldNode);
            mustNotValueList.add(boolNode);
        }

        if (metricInList != null && metricInList.size() > 0)
        {
            shouldValueList.clear();
            for(String name : metricInList)
            {
                if (StringUtils.isNotBlank(name))
                {
                    shouldValueList.add(
                            Json.parse(
                                    shouldMatchQueryUnit.replace("$FIELD", "metric_name").
                                            replace("$VALUE", name.toLowerCase().trim())));
                }
            }
            ObjectNode shouldNode = Json.newObject();
            ObjectNode boolNode = Json.newObject();
            shouldNode.set("should", Json.toJson(shouldValueList));
            boolNode.put("bool", shouldNode);
            mustValueList.add(boolNode);
        }
        if (metricNotInList != null && metricNotInList.size() > 0)
        {
            shouldValueList.clear();
            for(String name : metricNotInList)
            {
                if (StringUtils.isNotBlank(name))
                {
                    shouldValueList.add(
                            Json.parse(
                                    mustNotQueryUnit.replace("$FIELD", "metric_name").
                                            replace("$VALUE", name.toLowerCase().trim())));
                }
            }
            ObjectNode shouldNode = Json.newObject();
            ObjectNode boolNode = Json.newObject();
            shouldNode.set("should", Json.toJson(shouldValueList));
            boolNode.put("bool", shouldNode);
            mustNotValueList.add(boolNode);
        }

        ObjectNode boolNode = Json.newObject();
        ObjectNode queryNode = Json.newObject();
        if (mustValueList.size() > 0 && mustNotValueList.size() > 0)
        {
            boolNode.set("must", Json.toJson(mustValueList));
            boolNode.set("must_not", Json.toJson(mustNotValueList));
            queryNode.put("bool", boolNode);
        }
        else if (mustValueList.size() > 0)
        {
            boolNode.set("must", Json.toJson(mustValueList));
            queryNode.put("bool", boolNode);
        }
        else if (mustNotValueList.size() > 0)
        {
            boolNode.set("must_not", Json.toJson(mustNotValueList));
            queryNode.put("bool", boolNode);
        }

        return queryNode;
    }

    public static ObjectNode generateFlowJobAdvSearchQueryString(JsonNode searchOpt)
    {
        List<String> appcodeInList = new ArrayList<String>();
        List<String> appcodeNotInList = new ArrayList<String>();
        List<String> flowInList = new ArrayList<String>();
        List<String> flowNotInList = new ArrayList<String>();
        List<String> jobInList = new ArrayList<String>();
        List<String> jobNotInList = new ArrayList<String>();

        if (searchOpt != null && (searchOpt.isContainerNode())) {
            if (searchOpt.has("appcode")) {
                JsonNode appcodeNode = searchOpt.get("appcode");
                if (appcodeNode != null && appcodeNode.isContainerNode()) {
                    if (appcodeNode.has("in")) {
                        JsonNode appcodeInNode = appcodeNode.get("in");
                        if (appcodeInNode != null) {
                            String appcodeInStr = appcodeInNode.asText();
                            if (StringUtils.isNotBlank(appcodeInStr)) {
                                String[] appcodeInArray = appcodeInStr.split(",");
                                if (appcodeInArray != null) {
                                    for (String value : appcodeInArray) {
                                        if (StringUtils.isNotBlank(value)) {
                                            appcodeInList.add(value.trim());
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if (appcodeNode.has("not")) {
                        JsonNode appcodeNotInNode = appcodeNode.get("not");
                        if (appcodeNotInNode != null) {
                            String appcodeNotInStr = appcodeNotInNode.asText();
                            if (StringUtils.isNotBlank(appcodeNotInStr)) {
                                String[] appcodeNotInArray = appcodeNotInStr.split(",");
                                if (appcodeNotInArray != null) {
                                    for (String value : appcodeNotInArray) {
                                        if (StringUtils.isNotBlank(value)) {
                                            appcodeNotInList.add(value.trim());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if (searchOpt.has("flow")) {
                JsonNode flowNode = searchOpt.get("flow");
                if (flowNode != null && flowNode.isContainerNode()) {
                    if (flowNode.has("in")) {
                        JsonNode flowInNode = flowNode.get("in");
                        if (flowInNode != null) {
                            String flowInStr = flowInNode.asText();
                            if (StringUtils.isNotBlank(flowInStr)) {
                                String[] flowInArray = flowInStr.split(",");
                                if (flowInArray != null) {
                                    for (String value : flowInArray) {
                                        if (StringUtils.isNotBlank(value)) {
                                            flowInList.add(value.trim());
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if (flowNode.has("not")) {
                        JsonNode flowNotInNode = flowNode.get("not");
                        if (flowNotInNode != null) {
                            String flowNotInStr = flowNotInNode.asText();
                            if (StringUtils.isNotBlank(flowNotInStr)) {
                                String[] flowNotInArray = flowNotInStr.split(",");
                                if (flowNotInArray != null) {
                                    for (String value : flowNotInArray) {
                                        if (StringUtils.isNotBlank(value)) {
                                            flowNotInList.add(value.trim());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if (searchOpt.has("job")) {
                JsonNode jobNode = searchOpt.get("job");
                if (jobNode != null && jobNode.isContainerNode()) {
                    if (jobNode.has("in")) {
                        JsonNode jobInNode = jobNode.get("in");
                        if (jobInNode != null) {
                            String jobInStr = jobInNode.asText();
                            if (StringUtils.isNotBlank(jobInStr)) {
                                String[] jobInArray = jobInStr.split(",");
                                if (jobInArray != null) {
                                    for (String value : jobInArray) {
                                        if (StringUtils.isNotBlank(value)) {
                                            jobInList.add(value.trim());
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if (jobNode.has("not")) {
                        JsonNode jobNotInNode = jobNode.get("not");
                        if (jobNotInNode != null) {
                            String jobNotInStr = jobNotInNode.asText();
                            if (StringUtils.isNotBlank(jobNotInStr)) {
                                String[] jobNotInArray = jobNotInStr.split(",");
                                if (jobNotInArray != null) {
                                    for (String value : jobNotInArray) {
                                        if (StringUtils.isNotBlank(value)) {
                                            jobNotInList.add(value.trim());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        List<JsonNode> mustValueList = new ArrayList<JsonNode>();
        List<JsonNode> mustNotValueList= new ArrayList<JsonNode>();
        List<JsonNode> shouldValueList = new ArrayList<JsonNode>();

        String shouldMatchQueryUnit =
                "{\"bool\": " +
                        "{\"should\": " +
                            "[{\"wildcard\": {\"$FIELD\": {\"value\": \"*$VALUE*\", \"boost\": 1}}}, " +
                            "{\"prefix\": {\"$FIELD\": {\"value\": \"$VALUE\", \"boost\": 4}}}, " +
                            "{\"match\": {\"$FIELD\": {\"query\": \"$VALUE\", \"boost\": 16}}}" +
                            "]" +
                        "}" +
                "}";
        String mustNotQueryUnit = "{\"term\" : {\"$FIELD\" : \"$VALUE\"}}";

        if (appcodeInList != null && appcodeInList.size() > 0)
        {
            for(String appCode : appcodeInList)
            {
                if (StringUtils.isNotBlank(appCode))
                {
                    shouldValueList.add(
                            Json.parse(
                                    shouldMatchQueryUnit.replace("$FIELD", "app_code").
                                            replace("$VALUE", appCode.toLowerCase().trim())));
                }
            }
            ObjectNode shouldNode = Json.newObject();
            ObjectNode boolNode = Json.newObject();
            shouldNode.set("should", Json.toJson(shouldValueList));
            boolNode.put("bool", shouldNode);
            mustValueList.add(boolNode);
        }
        if (appcodeNotInList != null && appcodeNotInList.size() > 0)
        {
            shouldValueList.clear();
            for(String appCode : appcodeNotInList)
            {
                if (StringUtils.isNotBlank(appCode))
                {
                    shouldValueList.add(
                            Json.parse(
                                    mustNotQueryUnit.replace("$FIELD", "app_code").
                                            replace("$VALUE", appCode.toLowerCase().trim())));
                }
            }
            ObjectNode shouldNode = Json.newObject();
            ObjectNode boolNode = Json.newObject();
            shouldNode.set("should", Json.toJson(shouldValueList));
            boolNode.put("bool", shouldNode);
            mustNotValueList.add(boolNode);
        }

        if (flowInList != null && flowInList.size() > 0)
        {
            shouldValueList.clear();
            for(String flow : flowInList)
            {
                if (StringUtils.isNotBlank(flow))
                {
                    shouldValueList.add(
                            Json.parse(
                                    shouldMatchQueryUnit.replace("$FIELD", "flow_name").
                                            replace("$VALUE", flow.toLowerCase().trim())));
                }
            }
            ObjectNode shouldNode = Json.newObject();
            ObjectNode boolNode = Json.newObject();
            shouldNode.set("should", Json.toJson(shouldValueList));
            boolNode.put("bool", shouldNode);
            mustValueList.add(boolNode);
        }
        if (flowNotInList != null && flowNotInList.size() > 0)
        {
            shouldValueList.clear();
            for(String flow : flowNotInList)
            {
                if (StringUtils.isNotBlank(flow))
                {
                    shouldValueList.add(
                            Json.parse(
                                    mustNotQueryUnit.replace("$FIELD", "flow_name").
                                            replace("$VALUE", flow.toLowerCase().trim())));
                }
            }
            ObjectNode shouldNode = Json.newObject();
            ObjectNode boolNode = Json.newObject();
            shouldNode.set("should", Json.toJson(shouldValueList));
            boolNode.put("bool", shouldNode);
            mustNotValueList.add(boolNode);
        }

        if (jobInList != null && jobInList.size() > 0)
        {
            shouldValueList.clear();
            for(String job : jobInList)
            {
                if (StringUtils.isNotBlank(job))
                {
                    shouldValueList.add(
                            Json.parse(
                                    shouldMatchQueryUnit.replace("$FIELD", "jobs.job_name").
                                            replace("$VALUE", job.toLowerCase().trim())));
                }
            }
            ObjectNode nestedNode = Json.newObject();
            ObjectNode queryNode = Json.newObject();
            ObjectNode shouldNode = Json.newObject();
            ObjectNode boolNode = Json.newObject();
            shouldNode.set("should", Json.toJson(shouldValueList));
            boolNode.put("bool", shouldNode);
            queryNode.put("query", boolNode);
            queryNode.put("path", "jobs");
            nestedNode.put("nested", queryNode);

            mustValueList.add(nestedNode);
        }
        if (jobNotInList != null && jobNotInList.size() > 0)
        {
            shouldValueList.clear();
            for(String job : jobNotInList)
            {
                if (StringUtils.isNotBlank(job))
                {
                    shouldValueList.add(
                            Json.parse(
                                    mustNotQueryUnit.replace("$FIELD", "jobs.job_name").
                                            replace("$VALUE", job.toLowerCase().trim())));
                }
            }
            ObjectNode nestedNode = Json.newObject();
            ObjectNode queryNode = Json.newObject();
            ObjectNode shouldNode = Json.newObject();
            ObjectNode boolNode = Json.newObject();
            shouldNode.set("should", Json.toJson(shouldValueList));
            boolNode.put("bool", shouldNode);
            queryNode.put("query", boolNode);
            queryNode.put("path", "jobs");
            nestedNode.put("nested", queryNode);
            mustNotValueList.add(nestedNode);
        }

        ObjectNode boolNode = Json.newObject();
        ObjectNode queryNode = Json.newObject();
        if (mustValueList.size() > 0 && mustNotValueList.size() > 0)
        {
            boolNode.set("must", Json.toJson(mustValueList));
            boolNode.set("must_not", Json.toJson(mustNotValueList));
            queryNode.put("bool", boolNode);
        }
        else if (mustValueList.size() > 0)
        {
            boolNode.set("must", Json.toJson(mustValueList));
            queryNode.put("bool", boolNode);
        }
        else if (mustNotValueList.size() > 0)
        {
            boolNode.set("must_not", Json.toJson(mustNotValueList));
            queryNode.put("bool", boolNode);
        }

        return queryNode;
    }

}