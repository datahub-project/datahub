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

import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TCustomSqlStatement;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.stmt.TCreateTableSqlStatement;
import gudusoft.gsqlparser.stmt.TDeleteSqlStatement;
import gudusoft.gsqlparser.stmt.TInsertSqlStatement;
import gudusoft.gsqlparser.stmt.TMergeSqlStatement;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;
import gudusoft.gsqlparser.stmt.TUpdateSqlStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class SqlParser {

    protected final static Logger logger = LoggerFactory.getLogger("SqlParser");

    public static Stmt parseSelString(String text) throws NoSqlTypeException{
        TGSqlParser sqlparser = new TGSqlParser(EDbVendor.dbvteradata);
        sqlparser.sqltext = text;
        int ret = sqlparser.parse();
        if(ret == 0){
            return analyzeStmt(sqlparser.sqlstatements.get(0));

        }else{
            throw new NoSqlTypeException();
        }
    }

    public static String parse(String text) throws IOException {

        TGSqlParser sqlparser = new TGSqlParser(EDbVendor.dbvteradata);
        sqlparser.sqltext = text;
        int ret = sqlparser.parse();

        Stmt st = null;
        try
        {
            if (ret == 0) {
                for (int i = 0; i < sqlparser.sqlstatements.size(); i++) {
                    try {
                        st = analyzeStmt(sqlparser.sqlstatements.get(i));
                    } catch (NoSqlTypeException e) {
                        logger.error(sqlparser.sqlstatements.get(i).toString());
                        logger.error(e.getMessage());
                    }
                }
            } else {
                logger.error("GSP parse failed.");
                logger.error(sqlparser.getErrormessage());
                return null;
            }
        }
        catch (Exception e)
        {
            logger.error("General Exception");
            logger.error(sqlparser.sqltext);
            logger.error(e.getMessage());
            return null;
        }

        return WriteJson.write(st);
    }

    public static Stmt analyzeStmt(TCustomSqlStatement stmt) throws NoSqlTypeException {
        switch (stmt.sqlstatementtype) {
            case sstselect:
                return new SelectStmt((TSelectSqlStatement) stmt);
            case sstinsert:
                return new InsertStmt((TInsertSqlStatement) stmt);
            case sstcreatetable:
                TCreateTableSqlStatement createStmt = (TCreateTableSqlStatement) stmt;
                return new CreateStmt(createStmt);
            case sstupdate:
                return new UpdateStmt((TUpdateSqlStatement) stmt);
            case sstdelete:
                return new DeleteStmt((TDeleteSqlStatement) stmt);
            case sstmerge:
                return new MergeStmt((TMergeSqlStatement) stmt);
            case sstaltertable:
                break;
            case sstcreateview:
                break;
            default:
                throw new NoSqlTypeException();
        }
        return null;
    }

}