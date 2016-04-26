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

import gudusoft.gsqlparser.TBaseType;
import gudusoft.gsqlparser.stmt.TInsertSqlStatement;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;

public class InsertStmt extends Stmt {
    String whereClause;

    public InsertStmt(TInsertSqlStatement stmt) throws NoSqlTypeException {
        super(stmt);
        this.Op = "INSERT";
        this.targetTables
                .add(stmt.tables.getTable(0).getTableName().toString());
        /* in insert, the source should have it self? */
        // this.sourceTables.add(stmt.tables.getTable(0).getTableName().toString());

        if (stmt.getWhereClause() != null) {
            whereClause = stmt.getWhereClause().toString();
            // if there is a select in "in" clause, this should also be a source
            // this expression can be exreamly complex, can't fit for all
            processWhere(stmt);

        }

        switch (stmt.getValueType()) {
            case TBaseType.vt_values:
                for (int i = 0; i < stmt.getValues().size(); i++) {
                    //System.out.println(stmt.getInsertIntoValues());
                    if (stmt.getValues().getMultiTarget(i).getSubQuery() != null) {
                        TSelectSqlStatement subQuery = stmt.getValues()
                                .getMultiTarget(i).getSubQuery();
                        this.nestedSource.add(SqlParser.analyzeStmt(subQuery));
                    }
                }

                break;
            case TBaseType.vt_query:
                TSelectSqlStatement subQuery = stmt.getSubQuery();
                this.nestedSource.add(SqlParser.analyzeStmt(subQuery));
                break;
            default:
                break;

        }
    }

}