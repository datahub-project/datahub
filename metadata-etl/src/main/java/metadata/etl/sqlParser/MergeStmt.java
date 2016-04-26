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

import gudusoft.gsqlparser.stmt.TMergeSqlStatement;

public class MergeStmt extends Stmt {

    String whereClause;
    public MergeStmt(TMergeSqlStatement stmt) throws NoSqlTypeException {
        super(stmt);
        this.Op = "MERGE";
        this.targetTables.add(stmt.getTargetTable().toString());
        for (int i = 1; i < stmt.tables.size(); i++) {
            if (stmt.tables.getTable(i).isBaseTable()) {
                this.sourceTables.add(stmt.tables.getTable(i).toString());
            } else {
                this.nestedSource
                        .add(SqlParser.analyzeStmt(stmt.tables.getTable(i).subquery));
            }
        }

        if (stmt.getWhereClause() != null) {
            whereClause = stmt.getWhereClause().toString();
            // if there is a select in "in" clause, this should also be a source
            // this expression can be exreamly complex, can't fit for all
            processWhere(stmt);

        }
    }

}