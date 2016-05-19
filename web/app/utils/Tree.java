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
import com.google.common.base.Strings;
import dao.FlowsDAO;
import dao.TreeViewDAO;
import play.Logger;
import play.Play;
import play.libs.Json;

import java.io.File;
import java.io.FileInputStream;
import java.util.Locale;

/**
 * @author SunZhaonan
 * @author R.Kluszczynski
 */
public class Tree {
    private static final String TREE_NAME_SOURCE_TYPE = ".tree.source";
    private static final String TREE_NAME_SUFFIX = ".tree.name";

    private static final JsonNode EMPTY_JSON = Json.parse("{}");

    public static JsonNode loadTree(String treeName) {
        if (!Strings.isNullOrEmpty(treeName)) {
            try {
                return Tree.loadTreeJsonNode(treeName);
            } catch (Exception e) {
                Logger.warn("Problem with loading tree: " + treeName, e);
            }
        }
        return EMPTY_JSON;
    }

    private static JsonNode loadTreeJsonNode(String treeName) throws Exception {
        final SourceType treeSourceType = getTreeSourceTypeEnum(treeName + TREE_NAME_SOURCE_TYPE);

        if (SourceType.DATABASE.equals(treeSourceType)) {
            if (treeName.equalsIgnoreCase("flows")) {
                return FlowsDAO.getFlowApplicationNodes();
            }
            return readTreeFromDatabase(treeName);
        } else if (SourceType.FILE.equals(treeSourceType)) {
            return readTreeFromFile(treeName);
        } else {
            Logger.warn("Unsupported tree source type. Please check key "
                    + treeName + TREE_NAME_SOURCE_TYPE + " in configuration file.");
        }
        return EMPTY_JSON;
    }

    private static JsonNode readTreeFromDatabase(String treeName) throws Exception {
        return Json.parse(TreeViewDAO.getTrieValue(treeName));
    }

    private static JsonNode readTreeFromFile(String treeName) throws Exception {
        String treeFilePath = Play.application().configuration().getString(treeName + TREE_NAME_SUFFIX);
        return Json.parse(new FileInputStream(new File(treeFilePath)));
    }

    private static SourceType getTreeSourceTypeEnum(String configurationKey) {
        final String value = Play.application().configuration().getString(configurationKey);
        if (Strings.isNullOrEmpty(value)) {
            Logger.debug("Missing value for key: " + configurationKey + ". Assuming it is a file.");
            return SourceType.FILE;
        }
        return SourceType.valueOf(value.toUpperCase(Locale.ENGLISH));
    }

    private enum SourceType {
        DATABASE,
        FILE
    }
}
