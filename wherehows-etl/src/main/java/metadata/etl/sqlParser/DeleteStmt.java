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
package metadata.etl.sqlParser;

import gudusoft.gsqlparser.TCustomSqlStatement;
import gudusoft.gsqlparser.nodes.TAliasClause;
import gudusoft.gsqlparser.nodes.TTableList;

/**
 * in delete statement, their source table and target table is the same
 *
 * @author zsun
 *
 */
public class DeleteStmt extends Stmt {
    String whereClause;

    public DeleteStmt(TCustomSqlStatement stmt) throws NoSqlTypeException {
        super(stmt);
        this.Op = "DELETE";
        //System.out.print(stmt.tables.getTable(0));
        //System.out.print(checkAlias(stmt.tables, stmt.getTargetTable().getName()));
        this.targetTables.add(checkAlias(stmt.tables, stmt.getTargetTable()
                .getName()));

        for (int i = 0; i < stmt.tables.size(); i++) {

            if (! stmt.tables.getTable(i).toString().equals( this.targetTables.get(0)))
                this.sourceTables.add(stmt.tables.getTable(i).toString());
        }


        if (stmt.getWhereClause() != null) {
            whereClause = stmt.getWhereClause().toString();
            // if there is a select in "in" clause, this should also be a source
            // this expression can be exreamly complex, can't fit for all
            processWhere(stmt);
        }
    }

    /** if this is an alias, return it's original name */
    private String checkAlias(TTableList tablelist, String name) {
        for (int i = 0; i < tablelist.size(); i++) {
            TAliasClause aliasClouse = tablelist.getTable(i).getAliasClause();
            if (aliasClouse != null && aliasClouse.toString().equals(name)) {
                //this.sourceTables.add(tablelist.getTable(i).getName());
                return tablelist.getTable(i).getName();
            }
        }
        return name;
    }
}