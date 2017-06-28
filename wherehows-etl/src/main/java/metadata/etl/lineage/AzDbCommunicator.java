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
package metadata.etl.lineage;

import java.io.IOException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.zip.GZIPInputStream;
import org.apache.commons.io.IOUtils;
import wherehows.common.Constant;


/**
 * Created by zsun on 9/21/15.
 */
public class AzDbCommunicator {

  Connection conn = null;

  public AzDbCommunicator(Properties prop)
    throws Exception {
    String url = prop.getProperty(Constant.AZ_DB_URL_KEY);
    String userName = prop.getProperty(Constant.AZ_DB_USERNAME_KEY);
    String passWord = prop.getProperty(Constant.AZ_DB_PASSWORD_KEY);
    // set up connections
    conn = DriverManager.getConnection(url, userName, passWord);
  }

  public String getExecLog(long execId, String jobName)
    throws SQLException, IOException {
    String cmd = "select log from execution_logs where exec_id = " + execId +
      " and name = '" + jobName + "'and attempt = 0 order by start_byte;";
    Statement statement = conn.createStatement();
    ResultSet rs = statement.executeQuery(cmd);
    StringBuilder sb = new StringBuilder();
    while (rs.next()) {
      Blob logBlob = rs.getBlob("log");
      GZIPInputStream gzip = new GZIPInputStream(logBlob.getBinaryStream());
      sb.append(IOUtils.toString(gzip, "UTF-8"));
    }
    statement.close();
    return sb.toString();
  }

  public void close()
    throws SQLException {
    this.conn.close();
  }
}
