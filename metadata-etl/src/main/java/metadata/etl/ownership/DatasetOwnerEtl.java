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
package metadata.etl.ownership;

/**
 * Created by zechen on 11/12/15.
 */

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Properties;
import metadata.etl.EtlJob;
import wherehows.common.Constant;


public class DatasetOwnerEtl extends EtlJob {
  @Deprecated
  public DatasetOwnerEtl(int dbId, long whExecId) {
    super(null, dbId, whExecId);
  }

  public DatasetOwnerEtl(int dbId, long whExecId, Properties prop) {
    super(null, dbId, whExecId, prop);
  }

  private static final String JAVA_FILE_NAME = "HiveJdbcClient";
  private static final String JAVA_EXT = ".java";
  private static final String HIVE_SCRIPT_FILE = "fetch_owner.hql";
  private static final String OUTPUT_FILE_NAME = "dataset_owner.csv";
  private static final String CLASSPATH = "${HIVE_HOME}/lib/*:${HIVE_CONF_DIR}:`hadoop classpath`:.";

  @Override
  public void extract() throws Exception {
    logger.info("Begin hdfs dataset ownership extract!");
    JSch jsch = new JSch();
    Session session = null;
    try {
      // set up session
      session =
        jsch.getSession(this.prop.getProperty(Constant.HDFS_REMOTE_USER_KEY), this.prop.getProperty(Constant.HDFS_REMOTE_MACHINE_KEY));
      // use private key instead of username/password
      session.setConfig("PreferredAuthentications", "publickey");
      jsch.addIdentity(this.prop.getProperty(Constant.HDFS_PRIVATE_KEY_LOCATION_KEY));
      Properties config = new Properties();
      config.put("StrictHostKeyChecking", "no");
      session.setConfig(config);
      session.connect();

      // copy file to remote
      String remoteDir = prop.getProperty(Constant.HDFS_REMOTE_WORKING_DIR);
      String localDir = prop.getProperty(Constant.WH_APP_FOLDER_KEY) + "/" + prop.getProperty(Constant.DB_ID_KEY);
      File dir = new File(localDir);
      if (!dir.exists()) {
        if (!dir.mkdirs()) {
          throw new Exception("can not create metadata directory");
        }
      }

      ChannelSftp channelSftp = (ChannelSftp) session.openChannel("sftp");
      channelSftp.connect();

      InputStream localFileStream = classLoader.getResourceAsStream("java/" + JAVA_FILE_NAME + JAVA_EXT);
      channelSftp.put(localFileStream, remoteDir + "/" + JAVA_FILE_NAME + JAVA_EXT, ChannelSftp.OVERWRITE);
      localFileStream.close();

      String hiveQuery = prop.getProperty(Constant.HDFS_OWNER_HIVE_QUERY_KEY);
      localFileStream = new ByteArrayInputStream(hiveQuery.getBytes());
      channelSftp.put(localFileStream, remoteDir + "/" + HIVE_SCRIPT_FILE, ChannelSftp.OVERWRITE);
      localFileStream.close();

      // run remote command

      StringBuilder execCmd = new StringBuilder("");
      execCmd.append("cd " + remoteDir + ";");
      execCmd.append("javac " + JAVA_FILE_NAME + JAVA_EXT + ";");
      execCmd.append("java -cp " + CLASSPATH + " " + JAVA_FILE_NAME + " " + HIVE_SCRIPT_FILE + " " + OUTPUT_FILE_NAME + ";");

      logger.info("execute remote command : " + execCmd);
      Channel execChannel = session.openChannel("exec");
      ((ChannelExec) execChannel).setCommand(execCmd.toString());

      execChannel.setInputStream(System.in);
      execChannel.setOutputStream(System.out);
      ((ChannelExec) execChannel).setErrStream(System.err);

      execChannel.connect();
      logger.debug("Debug : execChannel exit-status: " + execChannel.getExitStatus());

      while (execChannel.getExitStatus() == -1) {
        try {
          Thread.sleep(1000);
        } catch (Exception e) {
          System.out.println(e);
        }
      }

      logger.debug("execute finished!");
      execChannel.disconnect();

      // scp back the result
      String remoteOutputFile = remoteDir + "/" + OUTPUT_FILE_NAME;
      String localOutputFile = localDir + "/" + OUTPUT_FILE_NAME;
      channelSftp.get(remoteOutputFile, localOutputFile);
      logger.info("extract ownership finished");
      channelSftp.exit();
    } catch (Exception e) {
      logger.error("hdfs ownership collection error!");
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      e.printStackTrace(pw);
      logger.error(sw.toString());
      throw e;
    } finally {
      if (session != null) {
        session.disconnect();
      }
    }
  }

  @Override
  public void transform() throws Exception {
    logger.info("hdfs ownership transform");
    // call a python script to do the transformation
    InputStream inputStream = classLoader.getResourceAsStream("jython/OwnerTransform.py");
    interpreter.execfile(inputStream);
    inputStream.close();
  }

  @Override
  public void load() throws Exception {
    logger.info("hdfs ownership load");
    // load into mysql
    InputStream inputStream = classLoader.getResourceAsStream("jython/OwnerLoad.py");
    interpreter.execfile(inputStream);
    inputStream.close();
    logger.info("hdfs ownership load finished");
  }
}
