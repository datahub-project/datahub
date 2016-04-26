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
import gudusoft.gsqlparser.nodes.TExpression;

import java.util.ArrayList;

/**
 * the parent class of all kind of element
 *
 * @author zsun
 *
 */
public class Stmt {
    /* the common attribute for all kind of statements */
    String Op;
    ArrayList<String> sourceTables;
    ArrayList<Stmt> nestedSource;
    ArrayList<String> targetTables;
    ArrayList<Stmt> nestedTarget;

    /* used for mark the start/end line, give value in python */
    int start_line;
    int end_line;

    /*
     * the constructor of statements, should override by every specified
     * statements
     */
    public Stmt(TCustomSqlStatement stmt) {
        sourceTables = new ArrayList<String>();
        targetTables = new ArrayList<String>();

        nestedSource = new ArrayList<Stmt>();
        nestedTarget = new ArrayList<Stmt>();
    }

    /*
     * use for process with where, if there is an "in" followed by select, that
     * should also count as sources
     */
    protected void processWhere(TCustomSqlStatement stmt) throws NoSqlTypeException {

        processExpression(stmt.getWhereClause().getCondition());
    }

    protected void processExpression(TExpression expr) throws NoSqlTypeException {
        if (expr.getSubQuery() != null) {
            this.nestedSource.add(SqlParser.analyzeStmt(expr.getSubQuery()));
        }

        if (expr.getLeftOperand() != null) {
            processExpression(expr.getLeftOperand());
        }
        if (expr.getRightOperand() != null) {
            processExpression(expr.getRightOperand());
        }
    }

    @Override
    public String toString() {
        StringBuffer resultBuffer = new StringBuffer();

        resultBuffer.append("Operation:\t" + Op + "\n");
        if (sourceTables.size() > 0) {
            resultBuffer.append("sourceTables:\n");

            for (String s : sourceTables) {
                resultBuffer.append("\t" + s);
            }
            resultBuffer.append("\n");
        }
        if (nestedSource.size() > 0) {
            for (Stmt st : nestedSource) {
                resultBuffer.append("nested source: " + st.toString());
            }
        }

        if (targetTables.size() > 0 || nestedTarget.size() > 0) {
            resultBuffer.append("targetTables:\n");
            for (String s : targetTables) {
                resultBuffer.append("\t" + s);
            }

            resultBuffer.append("\n");
        }
        if (nestedTarget.size() > 0) {
            for (Stmt st : nestedTarget) {
                resultBuffer.append("nested target: " + st.toString());
            }
        }

        return resultBuffer.toString();
    }
}