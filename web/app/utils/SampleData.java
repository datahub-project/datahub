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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.TextNode;

import java.util.Arrays;

public class SampleData
{
    public static JsonNode secureSampleData(JsonNode sample) {

        String[] nameArray = new String[]{"member_sk", "membersk", "member_id", "memberid", "mem_sk", "mem_id"};
        if (sample != null && sample.has("sample")) {
            JsonNode sampleNode = sample.get("sample");
            if (sampleNode != null && sampleNode.has("columnNames") && sampleNode.has("data")) {
                JsonNode namesNode = sampleNode.get("columnNames");
                JsonNode dataNode = sampleNode.get("data");
                if (namesNode != null && namesNode.isArray()) {
                    for (int i = 0; i < namesNode.size(); i++) {
                        if (Arrays.asList(nameArray).contains(namesNode.get(i).asText().toLowerCase())) {
                            if (dataNode != null && dataNode.isArray()) {
                                for (JsonNode node : dataNode) {
                                    JsonNode valueNode = node.get(i);
                                    ((ArrayNode)node).set(i, new TextNode("********"));
                                }
                            }
                        }
                    }
                }
            }
        }
        return sample;
    }
}