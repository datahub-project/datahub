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
package wherehows;

import java.io.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.pig.data.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wherehows.common.Constant;
import wherehows.common.schemas.DatasetJsonRecord;
import wherehows.common.schemas.SampleDataRecord;
import wherehows.common.writers.FileWriter;


/**
 * Fetch hdfs datasets schema, sample data and other metadata.
 * <p>
 * This is a standalone program that should build to a jar file, copy to hadoop gateway and run from there.
 * The parameter contains :
 * <ul>
 * <li> {@value wherehows.common.Constant#HDFS_SCHEMA_REMOTE_PATH_KEY} : The hfds metadata file location store on remote hadoop gateway </li>
 * <li> {@value wherehows.common.Constant#HDFS_SAMPLE_REMOTE_PATH_KEY} : The hfds sample data file location store on remote hadoop gateway </li>
 * <li> {@value wherehows.common.Constant#HDFS_WHITE_LIST_KEY} : The list of directories as a start point to fetch metadata. (include all of their sub directories) </li>
 * <li> {@value wherehows.common.Constant#HDFS_NUM_OF_THREAD_KEY} : Number of thread to do the metadata collecting </li>
 * </ul>
 *
 * This program could be scheduled through the internal AKKA scheduler, also could run as a independent program.
 *
 */
public class SchemaFetch {
  private static FileWriter schemaFileWriter;
  private static FileWriter sampleFileWriter;
  private static FileAnalyzerFactory fileAnalyzerFactory;
  static Logger logger;
  private static Configuration conf;
  private static FileSystem fs;

  public SchemaFetch(Configuration conf)
    throws IOException, InterruptedException {
    logger = LoggerFactory.getLogger(getClass());
    this.conf = conf;

    schemaFileWriter = new FileWriter(this.conf.get(Constant.HDFS_SCHEMA_REMOTE_PATH_KEY));
    sampleFileWriter = new FileWriter(this.conf.get(Constant.HDFS_SAMPLE_REMOTE_PATH_KEY));

    // login from kerberos, get the file system
    String principal = this.conf.get(Constant.HDFS_REMOTE_USER_KEY);
    String keyLocation = this.conf.get(Constant.HDFS_REMOTE_KEYTAB_LOCATION_KEY, null);


    if (keyLocation == null) {
      System.out.println("No keytab file location specified, will ignore the kerberos login process");
      fs = FileSystem.get(new Configuration());
    } else {
      try {
        Configuration hdfs_conf = new Configuration();
        hdfs_conf.set("hadoop.security.authentication", "Kerberos");
        hdfs_conf.set("dfs.namenode.kerberos.principal.pattern", "*");
        UserGroupInformation.setConfiguration(hdfs_conf);
        UserGroupInformation.loginUserFromKeytab(principal, keyLocation);
        fs = FileSystem.get(hdfs_conf);
      } catch (IOException e) {
        System.out
            .println("Failed, Try to login through kerberos. Priciple: " + principal + " keytab location : " + keyLocation);
        e.printStackTrace();
        System.out.println("Use default, assume no kerbero needed");
        fs = FileSystem.get(new Configuration());
      }
    }

    // TODO Write to hdfs
    // String sampleDataFolder = "/projects/wherehows/hdfs/sample_data";
    // String cluster = this.conf.get("hdfs.cluster");
    // sampleDataAvroWriter = new AvroWriter(this.fs, sampleDataFolder + "/" + cluster, SampleDataRecord.class);
    // String schemaFolder = this.conf.get("hdfs.schema_location");

    fileAnalyzerFactory = new FileAnalyzerFactory(this.fs);
  }

  /**
   * Decide whether this is a dataset by it's sub directories format
   *
   * @param path
   * @return 1 : empty dataset or lowest level dataset
   * 0 : may have sub dataset
   * < 0 : error
   * @throws java.io.IOException
   * @throws AccessControlException
   */
  private static int isTable(Path path, FileSystem fs)
    throws IOException, AccessControlException {
    int hiddenFileCount = 0;
    int datePartitionCount = 0;
    int dataSetCount = 0;
    int fileCount = 0;
    int i = 0;
    String objName;

    try {
      // System.err.println("  Probing " + path.toString());
      for (FileStatus fstat : fs.listStatus(path)) {
        objName = fstat.getPath().getName();

        if (!fstat.isDirectory()) {
          // file
          fileCount++;
        } else if (objName.matches("(_|\\.|tmp|temp|_distcp|backup|\\*|test|trash).*")) {
          // hidden/temporary fs object
          hiddenFileCount++;
        } else if (objName.matches("daily|hourly|hourly.deduped|monthly|weekly|(ds|dt|datepartition|year|month|date)=[0-9-]+")) {
          // temporal partition type
          datePartitionCount++;
        } else if (objName.matches(
          "[0-9\\-_]+\\w+[0-9\\-_]+|\\w+_day=[0-9\\-_]+|\\p{Alnum}+=[0-9\\-_]+|[0-9\\-_]+|[0-9]{14}_\\w+|[0-9]{8}_\\w+|[0-9]{4}-[0-9]{2}-[0-9]{2}.*")) {
          // temporal
          datePartitionCount++;
        } else {
          // sub directory
          dataSetCount++;
        }
        i++;
      }  // end of for fstat
    }  // end of try
    catch (AccessControlException e) {
      return -1;  // Usually there is a permission issue
    } catch (IOException e) {
      return -2;
    } catch (Exception e) {
      return -3;
    }

    // System.err.println("  -- isTable(" + path.toString() + ") i=" + i + " datePartition=" + datePartitionCount + " dataSet=" + dataSetCount);
    if (i == 0 || dataSetCount == 0) {
      return 1;
    } else if (i > 0 && datePartitionCount > 0) {
      return 1;
    } else {
      return 0;
    }
  }

  // assume path contains all the tables

  private static void scanPathHelper(Path path, FileSystem scanFs)
    throws IOException, InterruptedException, SQLException {
    String curPath = path.toUri().getPath();
    Path n = path;
    if (path.getName().matches("^(\\.|_|tmp|temp|test|trash|backup|archive|ARCHIVE|storkinternal).*"))
        return;

    logger.info("  -- scanPath(" + curPath + ")\n");
    int x = isTable(path, scanFs);
    if (x > 0) {
      // System.err.println("  traceTable(" + path.toString() + ")");
      traceTableInfo(path, scanFs);
    } else if (x == 0) { // iterate over each table
      // FileStatus[] fslist = scanFs.listStatus(path);
      // System.err.println(" => " + fslist.length + " subdirs");
      for (FileStatus fstat : scanFs.listStatus(path)) {
        n = fstat.getPath();
        curPath = n.toUri().getPath();
        // System.err.println("  traceSubDir(" + curPath + ")");
        if (n == path) {
          continue;
        }
        try {
          if (isTable(n, scanFs) > 0) {
            traceTableInfo(n, scanFs);
          } else if (scanFs.listStatus(n).length > 0 || scanFs.getContentSummary(n).getLength() > 0) {
            scanPath(n, scanFs);
          } else {
            logger.info("* scanPath() size = 0: " + curPath);
          }
        } catch (AccessControlException e) {
          logger.error("* scanPath(e) Permission denied. Cannot access: " + curPath +
              " owner:" + fstat.getOwner() + " group: " + fstat.getGroup() + "with current user " +
              UserGroupInformation.getCurrentUser());
          // System.err.println(e);
          continue;
        } // catch
      } // end of for
    } // end else
  }

  /**
   * Main entrance of scan a path
   * Change user based on the folder owner, create the FileSystem based on the user
   *
   * @param path
   * @throws java.io.IOException
   */
  private static void scanPath(Path path, FileSystem fs)
    throws IOException, InterruptedException, SQLException {

    // TODO create a new filesystem use current user
    //FileSystem scanFs = FileSystem.newInstance(SchemaFetch.conf);
    FileSystem scanFs = fs;
    System.out.println("Now reading data as:" + UserGroupInformation.getCurrentUser());
    if (!scanFs.exists(path)) {
      logger.info("path : " + path.getName() + " doesn't exist!");
    }
    scanPathHelper(path, scanFs);
  }

  /**
   * Collect one dataset's metadata
   *
   * @param path
   * @throws java.io.IOException
   */
  private static void traceTableInfo(Path path, FileSystem tranceFs)
    throws IOException, SQLException {
    logger.info("trace table : " + path.toUri().getPath());
    // analyze the pattern of the name
    String tbl_name = path.getName();
    if (tbl_name.matches("(_|\\.|tmp|temp|stg|test|\\*).*")) // skip _temporary _schema.avsc
    {
      return;
    }

    FileStatus[] fstat_lst;
    FileStatus fstat = tranceFs.getFileStatus(path);
    String fullPath = path.toUri().getPath();
    String xName = "";
    long data_size = -1;
    long sample_data_size = -1;
    int i, x;
    // String data_source = checkDataSource(fullPath);

    // TODO this part need to rewrite
    try {
      while (fstat.isDirectory()) {

        fstat_lst = tranceFs.listStatus(fstat.getPath()); // list all children
        if (fstat_lst.length == 0) { // empty directory
          logger.info(fstat.getPath().toUri().getPath() + " is empty.");
          return;
        }

        int is_fstat_visible = 0;
        for (i = fstat_lst.length - 1; i >= 0; i--) { // iterate from the last item back to the first
          fstat = fstat_lst[i]; // start from the last file in the list
          xName = fstat.getPath().getName();

          if (xName.matches("\\.pig_schema|.*\\.avsc|\\.dataset")) {
            is_fstat_visible = 1;
            break;
          } else if (xName.equals("hourly") && i > 0 && fstat_lst[i - 1].getPath().getName().equals("daily")) {
            continue; // try to traverse "daily" instead of "hourly" when possible
          } else if (xName.matches("(_|\\.|tmp|temp).*")) {
            continue;
          }

          try { // sub directory may be inaccessible
            sample_data_size =
              fstat.isDirectory() ? tranceFs.getContentSummary(fstat.getPath()).getLength() : fstat.getLen();
          } catch (AccessControlException e) {
            if (tranceFs.listStatus(fstat.getPath()).length > 0) {
              is_fstat_visible = 1;
              break;
            } else {
              continue;
            }
          }

          if (fstat.isDirectory() == false
            && xName.matches("(_|\\.).*|.*\\.(jar|json|txt|csv|tsv|zip|gz|lzo)") == false) {
            is_fstat_visible = 1;
            break;
          }

          // if fstat is a Directory
          if (fstat.isDirectory() == true && xName.matches("(_|\\.).*") == false) {
            is_fstat_visible = 1;
            break;
          }
        }
        // logger.info(fstat.getPath() + "is_fstat_visible : " + is_fstat_visible);
        if (is_fstat_visible == 0) {
          return;
        }
      }
    } catch (AccessControlException e) {
      logger.error("* TblInfo() Cannot access " + fstat.getPath().toUri().getPath());
      return;
    }

    // get schema and sample data
    DatasetJsonRecord datasetSchemaRecord = fileAnalyzerFactory.getSchema(fstat.getPath(), path.toUri().getPath());
    if (datasetSchemaRecord != null) {
      schemaFileWriter.append(datasetSchemaRecord);
    } else {
      logger.error("* Cannot resolve the schema of " + fullPath);
    }

    SampleDataRecord sampleDataRecord = fileAnalyzerFactory.getSampleData(fstat.getPath(), path.toUri().getPath());
    if (sampleDataRecord != null) {
      sampleFileWriter.append(sampleDataRecord);
    } else {
      System.err.println("* Cannot fetch sample data of " + fullPath);
    }
  }

  public static void main(String[] args)
    throws Exception {
    System.out.println("SchemaFetch begins");

    Configuration conf = new Configuration();
    // put extra config into conf
    new GenericOptionsParser(conf, args).getRemainingArgs();

    SchemaFetch s = new SchemaFetch(conf);
    s.run();
    System.out.println("SchemaFetch exit");
    System.exit(0);
  }

  /**
   * entry point
   *
   * @throws Exception
   */
  public void run()
      throws Exception {
    // read the white list
    final List<Path> folders = new ArrayList<>();
    String whiteList = this.conf.get(Constant.HDFS_WHITE_LIST_KEY);
    for (String s : whiteList.split(",")) {
      Path p = new Path(s);
      folders.add(p);
    }

    final int size = folders.size();
    int numOfThread = Integer.valueOf(this.conf.get(Constant.HDFS_NUM_OF_THREAD_KEY, "1"));
    Thread[] threads = new Thread[numOfThread];
    final int granularity = (size / numOfThread == 0) ? 1 : size / numOfThread;
    for (int i = 0; i < numOfThread; i++) {
      final int finalI = i;
      final FileSystem finalFs = fs;
      Thread thread = new Thread() {
        public void run() {
          logger.info("Thread " + finalI + " Running, size of record : " + size);
          for (int j = finalI * (granularity); j < (finalI + 1) * (granularity) && j < size; j++) {
            Path p = folders.get(j);
            logger.info("begin processing " + p.toUri().getPath());
            try {
              scanPath(p, finalFs);
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
        }
      };
      threads[i] = thread;
      thread.start();
    }

    // wait for all thread are done, continue close the file writers
    for (Thread t : threads) {
      t.join();
    }

    sampleFileWriter.close();
    schemaFileWriter.close();
  }

}
