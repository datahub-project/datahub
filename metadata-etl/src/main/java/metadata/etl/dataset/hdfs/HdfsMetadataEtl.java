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
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedInputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URL;
import java.util.Properties;
import java.lang.reflect.*;
import java.lang.ProcessBuilder;

import org.apache.commons.io.FileUtils;

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
    logger.info("Begin hdfs metadata extract! - " + prop.getProperty(Constant.WH_EXEC_ID_KEY));
    boolean isRemote = Boolean.valueOf(prop.getProperty(Constant.HDFS_REMOTE, "false"));
    if (isRemote) {
      extractRemote();
    } else {
      extractLocal();
    }

  }

  private void extractLocal()
      throws Exception {

    URL localJarUrl = classLoader.getResource("jar/schemaFetch.jar");
    String homeDir = System.getProperty("user.home");
    String remoteJarFile = homeDir + "/.wherehows/schemaFetch.jar";
    File dest = new File(remoteJarFile);
    try {
        FileUtils.copyURLToFile(localJarUrl, dest);
    } catch(Exception e) {
        logger.error(e.toString());
    }

    String outputSchemaFile = prop.getProperty(Constant.HDFS_SCHEMA_LOCAL_PATH_KEY);
    String outputSampleDataFile = prop.getProperty(Constant.HDFS_SAMPLE_LOCAL_PATH_KEY);
    String cluster = prop.getProperty(Constant.HDFS_CLUSTER_KEY);
    String whiteList = prop.getProperty(Constant.HDFS_WHITE_LIST_KEY);
    String numOfThread = prop.getProperty(Constant.HDFS_NUM_OF_THREAD_KEY, String.valueOf(1));
    String hdfsUser = prop.getProperty(Constant.HDFS_REMOTE_USER_KEY);
    String hdfsKeyTab = prop.getProperty(Constant.HDFS_REMOTE_KEYTAB_LOCATION_KEY);
    String hdfsExtractLogFile = outputSchemaFile + ".log";

    String[] hadoopCmd = {"hadoop", "jar", remoteJarFile,
            "-D" + Constant.HDFS_SCHEMA_REMOTE_PATH_KEY + "=" + outputSchemaFile,
            "-D" + Constant.HDFS_SAMPLE_REMOTE_PATH_KEY + "=" + outputSampleDataFile,
            "-D" + Constant.HDFS_CLUSTER_KEY + "=" + cluster,
            "-D" + Constant.HDFS_WHITE_LIST_KEY + "=" + whiteList,
            "-D" + Constant.HDFS_NUM_OF_THREAD_KEY + "=" + numOfThread,
            "-D" + Constant.HDFS_REMOTE_USER_KEY + "=" + hdfsUser,
            "-D" + Constant.HDFS_REMOTE_KEYTAB_LOCATION_KEY + "=" + hdfsKeyTab,
            "-Dlog.file.name=hdfs_schema_fetch" };

    ProcessBuilder pb = new ProcessBuilder(hadoopCmd);
    File logFile = new File(hdfsExtractLogFile);
    pb.redirectErrorStream(true);
    pb.redirectOutput(ProcessBuilder.Redirect.appendTo(logFile));
    Process process = pb.start();
    int pid = -1;
    if(process.getClass().getName().equals("java.lang.UNIXProcess")) {
        /* get the PID on unix/linux systems */
        try {
          Field f = process.getClass().getDeclaredField("pid");
          f.setAccessible(true);
          pid = f.getInt(process);
        } catch (Throwable e) {
        }
    }
    logger.info("executue command [PID=" + pid + "]: " + hadoopCmd);

    // wait until this process finished.
    int execResult = process.waitFor();

    // if the process failed, log the error and throw exception
    if (execResult > 0) {
      BufferedReader br = new BufferedReader(new InputStreamReader(process.getErrorStream()));
      String errString = "HDFS Metadata Extract Error:\n";
      String line = "";
      while((line = br.readLine()) != null)
        errString = errString.concat(line).concat("\n");
      logger.error("*** Process  failed, status: " + execResult);
      logger.error(errString);
      throw new Exception("Process + " + pid + " failed");
    }


  }

  private void extractRemote()
      throws JSchException, SftpException, IOException {
    logger.info("Remote mode!");
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
      String remoteUser = this.prop.getProperty(Constant.HDFS_REMOTE_USER_KEY);
      String remoteKeyTab = prop.getProperty(Constant.HDFS_REMOTE_KEYTAB_LOCATION_KEY, null);
      String execCmd =
        "cd " + wherehowsExecFolder + ";"
          + "export HADOOP_CLIENT_OPTS=\"-Xmx2048m $HADOOP_CLIENT_OPTS\";"
          + "hadoop jar schemaFetch.jar"
          + " -D " + Constant.HDFS_SCHEMA_REMOTE_PATH_KEY + "=" + hdfsSchemaFile
          + " -D " + Constant.HDFS_SAMPLE_REMOTE_PATH_KEY + "=" + sampleDataFile
          + " -D " + Constant.HDFS_CLUSTER_KEY + "=" + cluster
          + " -D " + Constant.HDFS_WHITE_LIST_KEY + "=" + whiteList
          + " -D " + Constant.HDFS_NUM_OF_THREAD_KEY + "=" + numOfThread
          + " -D " + Constant.HDFS_REMOTE_USER_KEY + "=" + remoteUser;
      if (remoteKeyTab != null)
        execCmd += " -D " + Constant.HDFS_REMOTE_KEYTAB_LOCATION_KEY + "=" + remoteKeyTab;
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

      logger.info("ExecChannel exit-status: " + execChannel.getExitStatus());

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
    logger.info("Begin hdfs metadata transform : " + prop.getProperty(Constant.WH_EXEC_ID_KEY));
    // call a python script to do the transformation
    InputStream inputStream = classLoader.getResourceAsStream("jython/HdfsTransform.py");
    interpreter.execfile(inputStream);
    inputStream.close();
  }

  @Override
  public void load()
    throws Exception {
    logger.info("Begin hdfs metadata load : " + prop.getProperty(Constant.WH_EXEC_ID_KEY));
    // load into mysql
    InputStream inputStream = classLoader.getResourceAsStream("jython/HdfsLoad.py");
    interpreter.execfile(inputStream);
    inputStream.close();
    logger.info("hdfs metadata load finished : " + prop.getProperty(Constant.WH_EXEC_ID_KEY));
  }
}
