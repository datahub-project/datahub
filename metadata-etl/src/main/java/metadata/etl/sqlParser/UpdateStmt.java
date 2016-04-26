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

import gudusoft.gsqlparser.EExpressionType;
import gudusoft.gsqlparser.TSourceToken;
import gudusoft.gsqlparser.nodes.TAliasClause;
import gudusoft.gsqlparser.nodes.TExplicitDataTypeConversion;
import gudusoft.gsqlparser.nodes.TExpression;
import gudusoft.gsqlparser.nodes.TExpressionList;
import gudusoft.gsqlparser.nodes.TFunctionCall;
import gudusoft.gsqlparser.nodes.TJoinList;
import gudusoft.gsqlparser.nodes.TObjectName;
import gudusoft.gsqlparser.nodes.TParseTreeNode;
import gudusoft.gsqlparser.nodes.TResultColumn;
import gudusoft.gsqlparser.nodes.TTable;
import gudusoft.gsqlparser.nodes.TTableList;
import gudusoft.gsqlparser.nodes.TWhereClause;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;
import gudusoft.gsqlparser.stmt.TUpdateSqlStatement;

public class UpdateStmt extends Stmt {

    String whereClause;
    public UpdateStmt(TUpdateSqlStatement stmt) throws NoSqlTypeException {
        super(stmt);
        this.Op = "UPDATE";
        // if the target is an alia, it means itself is an source, add it into source in checkAlias function
        this.targetTables.add(checkAlias(stmt.tables, stmt.getTargetTable()
                .getName()));

        // the first one is target, begin from second one
        for (int i = 1; i < stmt.tables.size(); i++) {
            if (stmt.tables.getTable(i).isBaseTable()) {
                this.sourceTables.add(stmt.tables.getTable(i).toString());
                //System.out.println(i+stmt.tables.getTable(i).toString());
            } else {
                this.nestedSource
                        .add(SqlParser.analyzeStmt(stmt.tables.getTable(i).subquery));
                //System.out.println(i+stmt.tables.getTable(i).toString());
            }
        }
        if (stmt.getWhereClause() != null) {
            whereClause = stmt.getWhereClause().toString();
            // if there is a select in "in" clause, this should also be a source
            // this expression can be exreamly complex, can't fit for all
            processWhere(stmt);

        }

        // check the source inside the set clause
        TResultColumn a = stmt.getResultColumnList().getResultColumn(0);
        TExpression expression = a.getExpr();
        TExpression value = expression.getRightOperand();

        value.getExpressionType();

        if( value.getExpressionType() == EExpressionType.function_t ){
            TExpressionList argList = value.getFunctionCall().getArgs();
            for(int i=0; i< argList.size(); i++){
                TExpression arg = argList.getExpression(i);
                if (arg.getExpressionType() == EExpressionType.subquery_t){
                    Stmt subqury = SqlParser.analyzeStmt(arg.getSubQuery());
                    this.nestedSource.add(subqury);
                }
            }
        }
    }

    /** if this is an alias, return it's original name */
    private String checkAlias(TTableList tablelist, String name) {
        for (int i = 0; i < tablelist.size(); i++) {
            TAliasClause aliasClouse = tablelist.getTable(i).getAliasClause();
            if (aliasClouse != null && aliasClouse.toString().equals(name)) {
                this.sourceTables.add(tablelist.getTable(i).getName());
                return tablelist.getTable(i).getName();
            }
        }
        return name;
    }
}