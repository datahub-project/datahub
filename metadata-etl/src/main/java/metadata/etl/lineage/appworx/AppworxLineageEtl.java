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
package metadata.etl.lineage.appworx;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import metadata.etl.EtlJob;
import wherehows.common.Constant;


public class AppworxLineageEtl extends EtlJob {

    Connection conn;

    public AppworxLineageEtl(int appId, long whExecId) {
        super(appId, null, whExecId);
        try {
            setUp();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public AppworxLineageEtl(int appId, long whExecId, Properties properties) {
        super(appId, null, whExecId, properties);
        try {
            setUp();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void setUp()
            throws SQLException {
        String wherehowsHost = this.prop.getProperty(Constant.WH_DB_URL_KEY);
        String wherehowsUserName = this.prop.getProperty(Constant.WH_DB_USERNAME_KEY);
        String wherehowsPassWord = this.prop.getProperty(Constant.WH_DB_PASSWORD_KEY);
        conn = DriverManager.getConnection(
                wherehowsHost + "?" + "user=" + wherehowsUserName + "&password=" + wherehowsPassWord);
    }

    @Override
    public void extract()
            throws Exception {
        logger.info("In AppworxLineageEtl java launch extract jython scripts");
        InputStream inputStream = classLoader.getResourceAsStream("jython/AppworxLineageExtract.py");
        interpreter.execfile(inputStream);
        inputStream.close();
    }

    @Override
    public void transform()
            throws Exception {
        // Nothing
        logger.info("Appworx lineage metadata transform");
    }

    @Override
    public void load()
            throws Exception {
        logger.info("AppworxLineageEtl metadata load");

        String insertIntoFinalTable = "INSERT IGNORE INTO job_execution_data_lineage\n"
                + "SELECT app_id, flow_exec_id, job_exec_id, job_exec_uuid, job_name, job_start_unixtime, job_finished_unixtime,\n"
                + "db_id, abstracted_object_name, full_object_name, partition_start, partition_end, partition_type,\n"
                + "layout_id, storage_type, source_target_type, srl_no, source_srl_no, operation,\n"
                + "record_count, insert_count, delete_count, update_count, flow_path, UNIX_TIMESTAMP(NOW()), " + this.prop
                .getProperty(Constant.WH_EXEC_ID_KEY) + " FROM stg_job_execution_data_lineage where app_id = " + this.prop
                .getProperty(Constant.APP_ID_KEY);
        logger.info("Azkaban Lineage load cmd :\n" + insertIntoFinalTable);
        conn.createStatement().execute(insertIntoFinalTable);
        logger.info("Appworx lineage metadata ETL completed");
    }
}
