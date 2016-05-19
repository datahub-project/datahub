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
package dao;

import play.Logger;

/**
 * Class getting trees for view. It assumes that tree is already stored in database.
 *
 * @author R.Kluszczynski
 */
public class TreeViewDAO extends AbstractMySQLOpenSourceDAO {
    private final static String SELECT_MENU_JSON_CFG_SQL = "SELECT value FROM cfg_ui_trees WHERE name = ?";

    public static String getTrieValue(String treeName) {
        Logger.debug("Getting tree for: " + treeName);
        final String menuJsonResponse = getJdbcTemplate()
                .queryForObject(SELECT_MENU_JSON_CFG_SQL, new Object[]{treeName}, String.class);

        Logger.trace("Tree read for '" + treeName + "': " + menuJsonResponse);
        return menuJsonResponse;
    }
}
