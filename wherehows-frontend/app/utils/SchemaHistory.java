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

import java.util.Iterator;
import java.util.Map;

public class SchemaHistory
{

    public static int calculateDict(JsonNode node) {
        int count = 0;
        Iterator<Map.Entry<String, JsonNode>> iterator = node.fields();
        while (iterator.hasNext())
        {
            Map.Entry<String,JsonNode> element = iterator.next();
            if (element.getValue().isArray()) {
                if (element.getKey().toLowerCase() == "fields")
                {
                    count += element.getValue().size();
                }
                count += calculateList(element.getValue());
            } else if (element.getValue().isContainerNode()) {
                if (element.getKey().toLowerCase() == "fields")
                {
                    count += element.getValue().size();
                }
                count += calculateDict(element.getValue());
            }
            else if (element.getKey().toLowerCase() == "fields")
            {
                count += 1;
            }
        }
        return count;
    }

    public static int calculateList(JsonNode node) {
        int count = 0;
        Iterator<JsonNode> arrayIterator = node.elements();
        while (arrayIterator.hasNext()) {
            JsonNode element = arrayIterator.next();
            if (element.isArray()) {
                count += calculateList(element);
            } else if (element.isContainerNode()) {
                count += calculateDict(element);
            }
        }
        return count;
    }

    public static int calculateFieldCount(JsonNode node)
    {
        int count = 0;
        JsonNode schemaNode = null;

        if (node != null && node.has("schema")) {
            schemaNode = node.get("schema");
        }
        else
        {
            schemaNode = node;
        }
        if (schemaNode.isArray()) {

            Iterator<JsonNode> arrayIterator = schemaNode.elements();
            while (arrayIterator.hasNext()) {
                JsonNode element = arrayIterator.next();
                if (element.isArray()) {
                    count += calculateList(element);
                } else if (element.isContainerNode()) {
                    count += calculateDict(element);
                }
            }
        }
        else if (schemaNode.isContainerNode())
        {
            Iterator<Map.Entry<String, JsonNode>> iterator = schemaNode.fields();
            while (iterator.hasNext())
            {
                Map.Entry<String,JsonNode> element = iterator.next();
                if (element.getValue().isArray()) {
                    if (element.getKey().toLowerCase() == "fields")
                    {
                        count += element.getValue().size();
                    }
                    count += calculateList(element.getValue());
                } else if (element.getValue().isContainerNode()) {
                    if (element.getKey().toLowerCase() == "fields")
                    {
                        count += element.getValue().size();
                    }
                    count += calculateDict(element.getValue());
                }
                else if (element.getKey().toLowerCase() == "fields")
                {
                    count += 1;
                }
            }
        }
        return count;

    }
}