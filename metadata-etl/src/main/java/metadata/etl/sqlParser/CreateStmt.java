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

import gudusoft.gsqlparser.stmt.TCreateTableSqlStatement;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;

public class CreateStmt extends Stmt {

    boolean isVolatile;

    public CreateStmt(TCreateTableSqlStatement stmt) throws NoSqlTypeException {
        super(stmt);
        this.Op = "CREATE";
        this.targetTables.add(stmt.getTargetTable().getTableName().toString());

        //check if it is a volatile table

        for( int i=0; i< stmt.sourcetokenlist.indexOf(stmt.tables.getStartToken()); i++){
            if(stmt.sourcetokenlist.get(i).toString().equals("VOLATILE") ||
                    stmt.sourcetokenlist.get(i).toString().equals("volatile") ||
                    stmt.sourcetokenlist.get(i).toString().equals("Volatile")){
                isVolatile = true;
            }
        }

        TSelectSqlStatement subQuery = stmt.getSubQuery();
        if (subQuery != null) {
            Stmt subQstmt = SqlParser.analyzeStmt(subQuery);
            this.sourceTables = subQstmt.sourceTables;
            this.nestedSource = subQstmt.nestedSource;
        }
    }

}