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
import wherehows.common.Constant;
import wherehows.common.schemas.DatasetJsonRecord;
import wherehows.common.schemas.SampleDataRecord;
import wherehows.common.writers.FileWriter;


public class SchemaFetch {
  private static FileWriter schemaFileWriter;
  private static FileWriter sampleFileWriter;
  private static FileAnalyzerFactory fileAnalyzerFactory;
  private Configuration conf;

  public SchemaFetch(Configuration conf)
    throws IOException, InterruptedException {
    this.conf = conf;

    schemaFileWriter = new FileWriter(this.conf.get(Constant.HDFS_SCHEMA_REMOTE_PATH_KEY));
    sampleFileWriter = new FileWriter(this.conf.get(Constant.HDFS_SAMPLE_REMOTE_PATH_KEY));
    FileSystem writefs = FileSystem.get(new Configuration()); // create a new filesystem use current user
    // String sampleDataFolder = "/projects/wherehows/hdfs/sample_data";
    // String cluster = this.conf.get("hdfs.cluster");
    // sampleDataAvroWriter = new AvroWriter(this.fs, sampleDataFolder + "/" + cluster, SampleDataRecord.class);

    // String schemaFolder = this.conf.get("hdfs.schema_location");

    fileAnalyzerFactory = new FileAnalyzerFactory(writefs);
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
        } else if (objName.matches("(_|\\.|tmp|temp|_distcp|\\*|test).*")) {
          // hidden/temporary fs object
          hiddenFileCount++;
        } else if (objName.matches("daily|hourly|monthly|weekly|year=[0-9]+|month=[0-9]+|country=.*")) {
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
    //if (path.getName().matches("^(\\.|_|tmp|temp|test|\\*|archive|ARCHIVE|storkinternal).*"))
    //    return;

    System.out.print("  -- scanPath(" + curPath + ")\n");
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
            scanPath(n);
          } else {
            System.err.println("* scanPath() size = 0: " + curPath);
          }
        } catch (AccessControlException e) {
          System.err.println("* scanPath(e) Permission denied. Cannot access: " + curPath +
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
  private static void scanPath(Path path)
    throws IOException, InterruptedException, SQLException {

    FileSystem scanFs = FileSystem.newInstance(new Configuration()); // create a new filesystem use current user
    System.out.println("Now reading data as:" + UserGroupInformation.getCurrentUser());
    if (!scanFs.exists(path)) {
      System.out.println("path : " + path.getName() + " doesn't exist!");
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
    System.out.println("trace table : " + path.toUri().getPath());
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
          System.out.println(fstat.getPath().toUri().getPath() + " is empty.");
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
        // System.out.println(fstat.getPath() + "is_fstat_visible : " + is_fstat_visible);
        if (is_fstat_visible == 0) {
          return;
        }
      }
    } catch (AccessControlException e) {
      System.err.println("* TblInfo() Cannot access " + fstat.getPath().toUri().getPath());
      return;
    }

    // get schema and sample data
    DatasetJsonRecord datasetSchemaRecord = fileAnalyzerFactory.getSchema(fstat.getPath(), path.toUri().getPath());
    if (datasetSchemaRecord != null) {
      schemaFileWriter.append(datasetSchemaRecord);
    } else {
      System.err.println("* Cannot resolve the schema of " + fullPath);
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
    System.out.println("hadoop job begins");

    Configuration conf = new Configuration();
    // put extra config into conf
    new GenericOptionsParser(conf, args).getRemainingArgs();

    SchemaFetch s = new SchemaFetch(conf);
    s.run();
    System.out.println("schemaFetch exit");
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
    // TODO configure from database
    // InputStream in = getClass().getResourceAsStream("/dir-white-list.txt");

    // BufferedReader inputStream = new BufferedReader(new InputStreamReader(in));

    // for each folder, call scanPath()
    // multi threading
    /*
    String line;

    while ((line = inputStream.readLine()) != null) {
      line = line.trim();
      if (line.length() < 2 || line.matches("(#|-| |\\.).*")) {
        continue;
      }
      Path p = new Path(line);
      folders.add(p);
    }
    */
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
      Thread thread = new Thread() {
        public void run() {
          System.out.println("Thread " + finalI + " Running, size of record : " + size);
          for (int j = finalI * (granularity); j < (finalI + 1) * (granularity) && j < size; j++) {
            Path p = folders.get(j);
            System.out.println("begin processing " + p.toUri().getPath());
            try {
              scanPath(p);
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
