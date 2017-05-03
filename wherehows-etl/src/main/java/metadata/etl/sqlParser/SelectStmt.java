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

import gudusoft.gsqlparser.stmt.TSelectSqlStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SelectStmt extends Stmt {

    protected final static Logger logger = LoggerFactory.getLogger("SelectStmt");

    String groupBy;
    String orderBy;

    String join_condition;

    String setOp;
    String whereClause;

    public SelectStmt(TSelectSqlStatement stmt) throws NoSqlTypeException {
        super(stmt);
        if (stmt.getSetOperator() != TSelectSqlStatement.setOperator_none) {
            // System.out.println("########");
            setOpProcess(stmt);
            // stmt = stmt.getRightStmt();
            this.Op = setOp;
            return;
        }

        this.Op = "SELECT";
        if (stmt.getCteList() != null) {
            for (int i = 0; i < stmt.getCteList().size(); i++) {
                Stmt subqury = SqlParser.analyzeStmt(stmt.getCteList().getCTE(i)
                        .getSubquery());
                this.nestedSource.add(subqury);
            }
        }
        /* some complex sql have nested select inside value */
        for (int i = 0; i < stmt.getResultColumnList().size(); i++) {
            //System.out.println(stmt.getResultColumnList().getResultColumn(i));
            try {

                //this suboperation can be a subquery, sub operation
                String suboperation = stmt.getResultColumnList().getResultColumn(i).toString();
                this.nestedSource.add(SqlParser.parseSelString(stmt
                        .getResultColumnList().getResultColumn(i).toString()));



            } catch (NoSqlTypeException e) {
                //e.printStackTrace();
                // if not a nested sql , ignore it
            }
            catch (Exception e)
            {
                logger.error("General exception, suboperation is:");
                logger.error(stmt.getResultColumnList().getResultColumn(i).toString());
                logger.error(e.getMessage());
            }
            // Main.analyzeStmt(stmt.getResultColumnList().getResultColumn(i));
        }

        for (int i = 0; i < stmt.tables.size(); i++) {
            if (stmt.tables.getTable(i).isBaseTable()) {
                this.sourceTables.add(stmt.tables.getTable(i).getTableName()
                        .toString());
            } else if (stmt.tables.getTable(i).isCTEName()) {
                this.sourceTables.add(stmt.tables.getTable(i).getTableName()
                        .toString());
                // System.out.println("aaaaaa"+stmt.tables.getTable(i).toString());
                // System.out.println(stmt.getCteList().getCTE(0).getTableName());
                // System.out.println(stmt.getCteList().getCTE(0).getSubquery().tables);

            } else {
                this.nestedSource
                        .add(SqlParser.analyzeStmt(stmt.tables.getTable(i).subquery));
            }

        }
        if (stmt.getGroupByClause() != null)
            groupBy = stmt.getGroupByClause().toString();
        if (stmt.getOrderbyClause() != null)
            orderBy = stmt.getOrderbyClause().toString();
        if (stmt.getWhereClause() != null) {
            whereClause = stmt.getWhereClause().toString();
            // if there is a select in "in" clause, this should also be a source
            // this expression can be exreamly complex, can't fit for all
            processWhere(stmt);

        }
        /*
         * if (stmt.joins != null) { join_condition +=
         * stmt.joins.getJoin(0).getTable(); join_condition +=
         * stmt.joins.getJoin(0).getJoinItems(); }
         */
    }

    private void setOpProcess(TSelectSqlStatement stmt)
            throws NoSqlTypeException {
        // recursion base case
        if (stmt.getSetOperator() == TSelectSqlStatement.setOperator_none) {
            this.nestedSource.add(SqlParser.analyzeStmt(stmt));
        }
        // System.out.println("&&&&"+stmt.getLeftStmt());
        if (stmt.getRightStmt() != null)
            this.nestedSource.add(SqlParser.analyzeStmt(stmt.getRightStmt()));

        // left stmt is a single stmt or a set operator connected stmts
        if (stmt.getLeftStmt() != null)
            setOpProcess(stmt.getLeftStmt());

        switch (stmt.getSetOperator()) {
            case TSelectSqlStatement.setOperator_except:
                setOp = "EXCEPT";
            case TSelectSqlStatement.setOperator_exceptall:
                setOp = "EXCEPT ALL";
            case TSelectSqlStatement.setOperator_intersect:
                setOp = "INTERSECT";
            case TSelectSqlStatement.setOperator_intersectall:
                setOp = "INTERSECT ALL";
            case TSelectSqlStatement.setOperator_minus:
                setOp = "MINUS";
            case TSelectSqlStatement.setOperator_minusall:
                setOp = "MINUS ALL";
            case TSelectSqlStatement.setOperator_union:
                setOp = "UNION";
            case TSelectSqlStatement.setOperator_unionall:
                setOp = "UNION ALL";
            case TSelectSqlStatement.setOperator_none:
                break;

            default:

        }
    }
}
