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
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Iterator;

public class SampleData
{
    private final static String MEMBER_ID = "memberId";
    private final static String TREE_ID = "treeId";
    private final static String TRACKING_ID = "trackingId";
    private final static String IP_AS_BYTES_1 = "ipAsBytes";
    private final static String IP_AS_BYTES = "ip_as_bytes";
    private final static String ATTACHMENTS = "attachments";
    private final static String PAYLOAD = "payload";
    private final static String MEDIA = "MEDIA";
    private final static String HEADER = "header";
    private final static String GUID = "guid";
    private final static String AUDITHEADER = "auditHeader";
    private final static String MESSAGE_ID = "messageId";
    private final static String REQUEST = "request";
    private final static String REQUEST_HEADER = "requestHeader";

    public static String convertToHexString(JsonNode node)
    {
        String result = null;
        if (node != null)
        {
            if (node.isArray())
            {
                Iterator<JsonNode> arrayIterator = node.elements();
                StringBuilder sb = new StringBuilder();
                while (arrayIterator.hasNext()) {
                    JsonNode elementNode = arrayIterator.next();
                    if (elementNode != null)
                    {
                        String text = elementNode.asText();
                        if (StringUtils.isNotBlank(text))
                        {
                            byte[] bytes = text.getBytes();
                            if (bytes != null && bytes.length > 0)
                            {
                                sb.append(String.format("%02x", text.getBytes()[0]));
                            }
                        }
                    }
                }
                result = sb.toString();
            }
            else
            {
                result = node.asText();
            }
        }

        return result;
    }

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
                                    ((ArrayNode) node).set(i, new TextNode("********"));
                                }
                            }
                        }
                    }
                }
            }
            else
            {
                int index = 0;
                Iterator<JsonNode> arrayIterator = sampleNode.elements();
                while (arrayIterator.hasNext()) {
                    JsonNode sampleRowNode = arrayIterator.next();
                    if (sampleRowNode != null)
                    {
                        if (sampleRowNode.has(MEMBER_ID))
                        {
                            ((ObjectNode) sampleRowNode).set(MEMBER_ID, new TextNode("********"));
                        }
                        if (sampleRowNode.has(TREE_ID))
                        {
                            JsonNode treeIdNode = sampleRowNode.get(TREE_ID);
                            String convertedValue = convertToHexString(treeIdNode);
                            ((ObjectNode) sampleRowNode).set(TREE_ID, new TextNode(convertedValue));
                        }
                        if (sampleRowNode.has(TRACKING_ID))
                        {
                            JsonNode trackingIdNode = sampleRowNode.get(TRACKING_ID);
                            String convertedValue = convertToHexString(trackingIdNode);
                            ((ObjectNode) sampleRowNode).set(TRACKING_ID, new TextNode(convertedValue));
                        }
                        if (sampleRowNode.has(IP_AS_BYTES))
                        {
                            JsonNode ipNode = sampleRowNode.get(IP_AS_BYTES);
                            String convertedValue = convertToHexString(ipNode);
                            ((ObjectNode) sampleRowNode).set(IP_AS_BYTES, new TextNode(convertedValue));
                        }
                        if (sampleRowNode.has(IP_AS_BYTES_1))
                        {
                            JsonNode ipNode = sampleRowNode.get(IP_AS_BYTES_1);
                            String convertedValue = convertToHexString(ipNode);
                            ((ObjectNode) sampleRowNode).set(IP_AS_BYTES_1, new TextNode(convertedValue));
                        }
                        if (sampleRowNode.has(ATTACHMENTS))
                        {
                            JsonNode attachmentNode = sampleRowNode.get(ATTACHMENTS);
                            if (attachmentNode.has(PAYLOAD))
                            {
                                JsonNode payloadNode = attachmentNode.get(PAYLOAD);
                                String value = "** " + Integer.toString(payloadNode.size()) + " bytes binary data **";
                                ((ObjectNode) attachmentNode).set(PAYLOAD, new TextNode(value));
                            }
                        }
                        if (sampleRowNode.has(MEDIA))
                        {
                            JsonNode mediaNode = sampleRowNode.get(MEDIA);
                            String convertedValue = convertToHexString(mediaNode);
                            ((ObjectNode) sampleRowNode).set(MEDIA, new TextNode(convertedValue));
                        }
                        if (sampleRowNode.has(HEADER))
                        {
                            JsonNode headerNode = sampleRowNode.get(HEADER);
                            if (headerNode != null)
                            {
                                if (headerNode.has(MEMBER_ID))
                                {
                                    ((ObjectNode) headerNode).set(MEMBER_ID, new TextNode("********"));
                                }
                                if (headerNode.has(GUID))
                                {
                                    JsonNode guidNode = headerNode.get(GUID);
                                    String convertedValue = convertToHexString(guidNode);
                                    ((ObjectNode) headerNode).set(GUID, new TextNode(convertedValue));
                                }
                                if (headerNode.has(TREE_ID))
                                {
                                    JsonNode headerTreeIdNode = headerNode.get(TREE_ID);
                                    String convertedValue = convertToHexString(headerTreeIdNode);
                                    ((ObjectNode) headerNode).set(TREE_ID, new TextNode(convertedValue));
                                }
                                if (headerNode.has(AUDITHEADER))
                                {
                                    JsonNode auditHeaderNode = headerNode.get(AUDITHEADER);
                                    if (auditHeaderNode != null && auditHeaderNode.has(MESSAGE_ID))
                                    {
                                        JsonNode messageIdNode = auditHeaderNode.get(MESSAGE_ID);
                                        String convertedValue = convertToHexString(messageIdNode);
                                        ((ObjectNode) auditHeaderNode).set(MESSAGE_ID, new TextNode(convertedValue));
                                    }
                                }
                            }
                        }
                        if (sampleRowNode.has(REQUEST))
                        {
                            JsonNode requestNode = sampleRowNode.get(REQUEST);
                            if (requestNode != null && requestNode.has(ATTACHMENTS))
                            {
                                JsonNode attachmentsNode = requestNode.get(ATTACHMENTS);
                                if (attachmentsNode != null && attachmentsNode.has(PAYLOAD))
                                {
                                    JsonNode payloadNode = attachmentsNode.get(PAYLOAD);
                                    String value = "** " +
                                        Integer.toString(payloadNode.size()) +
                                        " bytes binary data **";
                                    ((ObjectNode) attachmentsNode).set(PAYLOAD, new TextNode(value));
                                }
                            }
                        }
                        if (sampleRowNode.has(REQUEST_HEADER))
                        {
                            JsonNode requestHeaderNode = sampleRowNode.get(REQUEST_HEADER);
                            if (requestHeaderNode != null && requestHeaderNode.has(IP_AS_BYTES))
                            {
                                JsonNode ipNode = requestHeaderNode.get(IP_AS_BYTES);
                                String convertedValue = convertToHexString(ipNode);
                                ((ObjectNode) requestHeaderNode).set(IP_AS_BYTES, new TextNode(convertedValue));
                            }
                            if (requestHeaderNode != null && requestHeaderNode.has(IP_AS_BYTES_1))
                            {
                                JsonNode ipNode = requestHeaderNode.get(IP_AS_BYTES_1);
                                String convertedValue = convertToHexString(ipNode);
                                ((ObjectNode) requestHeaderNode).set(IP_AS_BYTES_1, new TextNode(convertedValue));
                            }
                        }
                    }
                }
            }
        }
        return sample;
    }
}