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
package metadata.etl.dataset.hdfs;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

import java.io.File;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Properties;
import metadata.etl.EtlJob;
import wherehows.common.Constant;


/**
 * Created by zsun on 7/29/15.
 */
public class HdfsMetadataEtl extends EtlJob {

  /**
   * Constructor used in test
   * @param dbId
   * @param whExecId
   */
  @Deprecated
  public HdfsMetadataEtl(Integer dbId, Long whExecId) {
    super(null, dbId, whExecId);
  }

  /**
   * Copy the jar to remote gateway, run the collecting job on remote, copy back the result.
   * @param dbId the database need to collect
   * @param whExecId
   * @param prop all the properties that needed in ETL
   */
  public HdfsMetadataEtl(int dbId, long whExecId, Properties prop) {
    super(null, dbId, whExecId, prop);
  }

  @Override
  public void extract()
    throws Exception {
    logger.info("Begin hdfs metadata extract!");
    JSch jsch = new JSch();
    //jsch.setLogger(logger);
    final Log4JOutputStream log4JOutputStream = new Log4JOutputStream();
    Session session = null;
    try {
      // set up session
      session =
        jsch.getSession(this.prop.getProperty(Constant.HDFS_REMOTE_USER_KEY), this.prop.getProperty(Constant.HDFS_REMOTE_MACHINE_KEY));
      // use private key instead of username/password
      session.setConfig(
        "PreferredAuthentications",
        "publickey,gssapi-with-mic,keyboard-interactive,password");
      jsch.addIdentity(this.prop.getProperty(Constant.HDFS_PRIVATE_KEY_LOCATION_KEY));
      java.util.Properties config = new java.util.Properties();
      config.put("StrictHostKeyChecking", "no");
      session.setConfig(config);
      session.connect();

      // copy jar file to remote

      String remoteJarFile = this.prop.getProperty(Constant.HDFS_REMOTE_JAR_KEY);

      ChannelSftp channelSftp = (ChannelSftp) session.openChannel("sftp");
      channelSftp.connect();

      InputStream localJarStream = classLoader.getResourceAsStream("jar/schemaFetch.jar");
      channelSftp.put(localJarStream, remoteJarFile, ChannelSftp.OVERWRITE);
      localJarStream.close();

      String localSchemaFile = prop.getProperty(Constant.HDFS_SCHEMA_LOCAL_PATH_KEY);
      new File(localSchemaFile).getParentFile().mkdirs();
      String localSampleDataFile = prop.getProperty(Constant.HDFS_SAMPLE_LOCAL_PATH_KEY);
      new File(localSchemaFile).getParentFile().mkdirs();
      String remoteSchemaFile = prop.getProperty(Constant.HDFS_SCHEMA_REMOTE_PATH_KEY);
      String remoteSampleDataFile = prop.getProperty(Constant.HDFS_SAMPLE_REMOTE_PATH_KEY);
      // remote execute
      String hdfsSchemaFile = remoteSchemaFile.split("/")[1];
      String sampleDataFile = remoteSampleDataFile.split("/")[1];
      String wherehowsExecFolder = remoteJarFile.split("/")[0];
      String cluster = prop.getProperty(Constant.HDFS_CLUSTER_KEY);
      String whiteList = prop.getProperty(Constant.HDFS_WHITE_LIST_KEY);
      String numOfThread = prop.getProperty(Constant.HDFS_NUM_OF_THREAD_KEY, String.valueOf(1));
      String execCmd =
        "cd " + wherehowsExecFolder + ";"
          + "export HADOOP_CLIENT_OPTS=\"-Xmx2048m $HADOOP_CLIENT_OPTS\";"
          + "hadoop jar schemaFetch.jar"
          + " -D " + Constant.HDFS_SCHEMA_REMOTE_PATH_KEY + "=" + hdfsSchemaFile
          + " -D " + Constant.HDFS_SAMPLE_REMOTE_PATH_KEY + "=" + sampleDataFile
          + " -D " + Constant.HDFS_CLUSTER_KEY + "=" + cluster
          + " -D " + Constant.HDFS_WHITE_LIST_KEY + "=" + whiteList
          + " -D " + Constant.HDFS_NUM_OF_THREAD_KEY + "=" + numOfThread;
      logger.info("executue remote command : " + execCmd);
      Channel execChannel = session.openChannel("exec");
      ((ChannelExec) execChannel).setCommand(execCmd);

      // Redirection of output stream to log4jOutputStream
      execChannel.setExtOutputStream(log4JOutputStream);
      ((ChannelExec) execChannel).setErrStream(log4JOutputStream);
      execChannel.setOutputStream(log4JOutputStream);
      //execChannel.setInputStream(System.in);
      //((ChannelExec) execChannel).setErrStream(System.err);

      execChannel.connect();

      while (execChannel.getExitStatus() == -1){
        try{Thread.sleep(1000);}catch(Exception e){logger.error(String.valueOf(e));}
      }

      logger.info("Debug : execChannel exit-status: " + execChannel.getExitStatus());


      logger.debug("execute finished!");
      execChannel.disconnect();

      // scp back the result
      channelSftp.get(remoteSchemaFile, localSchemaFile);
      channelSftp.get(remoteSampleDataFile, localSampleDataFile);

      logger.info("extract finished");
      channelSftp.exit();
    } catch (Exception e) {
      logger.error("hdfs metadata collection error!");
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      e.printStackTrace(pw);
      logger.error(sw.toString());
      throw e;
    } finally {
      session.disconnect();
    }
  }

  @Override
  public void transform()
    throws Exception {
    logger.info("hdfs metadata transform");
    // call a python script to do the transformation
    InputStream inputStream = classLoader.getResourceAsStream("jython/HdfsTransform.py");
    interpreter.execfile(inputStream);
    inputStream.close();
  }

  @Override
  public void load()
    throws Exception {
    logger.info("hdfs metadata load");
    // load into mysql
    InputStream inputStream = classLoader.getResourceAsStream("jython/HdfsLoad.py");
    interpreter.execfile(inputStream);
    inputStream.close();
    logger.info("hdfs metadata load finished");
  }
}
