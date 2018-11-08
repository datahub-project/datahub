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
package metadata.etl.dataset.druid;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.json.JSONArray;
import org.json.JSONObject;
import wherehows.common.Constant;


@Slf4j
public class DruidMetadataExtractor {

  private String druid_host;
  private String druid_ds_metadata_csv_file;
  private String druid_col_metadata_csv_file;
  private String[] datasources;
  private String minTime;
  private String maxTime;
  private final int TIMEOUT = 1000;
  private final String SEGMENT_QUERY =
      "{\"queryType\":\"segmentMetadata\",\"dataSource\":\"$DATASOURCE\", \"intervals\":[\"$INTERVAL\"]}";
  private final String INTERVAL_QUERY = "{\"queryType\":\"timeBoundary\",\"dataSource\":\"$DATASOURCE\"}";
  private final org.joda.time.format.DateTimeFormatter DATETIME_FORMAT =
      DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

  public DruidMetadataExtractor(Properties prop) throws Exception {
    druid_host = prop.getProperty(Constant.DRUID_HOST_URL);
    druid_ds_metadata_csv_file = prop.getProperty(Constant.DRUID_DATASOURCE_METADATA_CSV_FILE);
    druid_col_metadata_csv_file = prop.getProperty(Constant.DRUID_FIELD_METADATA_CSV_FILE);
  }

  public DruidMetadataExtractor(String host, String ds_csv_file, String col_csv_file) throws Exception {
    if (host == null || host.length() == 0) {
      throw new Exception("Target host is not specified");
    }
    if (ds_csv_file == null || ds_csv_file.length() == 0) {
      throw new Exception("CSV file is not specified");
    }
    druid_host = host;
    druid_ds_metadata_csv_file = ds_csv_file;
    druid_col_metadata_csv_file = col_csv_file;
  }

  public void run() throws Exception {
    log.info("Remove existing metadata files:" + druid_ds_metadata_csv_file + "," + druid_col_metadata_csv_file);
    Files.deleteIfExists(FileSystems.getDefault().getPath(druid_ds_metadata_csv_file));
    Files.deleteIfExists(FileSystems.getDefault().getPath(druid_col_metadata_csv_file));
    getDatasources();
    for (int i = 0; i < datasources.length; i++) {
      String ds = datasources[i];
      getSegmentInterval(ds);
      JSONArray seg_metadata = getSegmentMetadata(ds, maxTime, minTime);
      format_dataset_metadata(ds, seg_metadata);
      format_field_metadata(ds, seg_metadata);
    }
  }

  public void getDatasources() throws Exception {
    String datasources_url = druid_host + "/" + "datasources";
    URL url = new URL(datasources_url);
    HttpURLConnection con = (HttpURLConnection) url.openConnection();
    con.setConnectTimeout(TIMEOUT);
    con.setRequestMethod("GET");
    int responseCode = con.getResponseCode();

    if (responseCode == 200) {
      BufferedReader br = new BufferedReader(new InputStreamReader(con.getInputStream()));
      String line = "";
      StringBuffer response = new StringBuffer();

      while ((line = br.readLine()) != null) {
        response.append(line);
      }
      String response_str = response.toString().replace("[", "").replace("]", "").replace("\"", "");
      if (response_str.length() > 0) {
        datasources = response_str.split(",");
        log.info(response_str.length() + " datasources found");
      } else {
        log.warn("Cannot find data source on Druid: " + druid_host);
      }
      br.close();
      con.disconnect();
    } else {
      con.disconnect();
      throw new Exception("Fail to get response from " + datasources_url + ", Response Code: " + responseCode);
    }
  }

  public void getSegmentInterval(String datasource) throws Exception {
    String query = INTERVAL_QUERY.replace("$DATASOURCE", datasource);
    URL url = new URL(druid_host);
    HttpURLConnection con = (HttpURLConnection) url.openConnection();
    con.setRequestMethod("POST");
    con.setConnectTimeout(TIMEOUT);
    con.setDoOutput(true);
    con.setRequestProperty("Content-Type", "application/json");
    DataOutputStream out = new DataOutputStream(con.getOutputStream());
    out.writeBytes(query);
    out.flush();
    out.close();
    int responde_code = con.getResponseCode();

    if (responde_code == 200) {
      BufferedReader br = new BufferedReader(new InputStreamReader(con.getInputStream()));
      String line = "";
      StringBuffer response = new StringBuffer();
      while ((line = br.readLine()) != null) {
        response.append(line);
      }
      br.close();
      con.disconnect();
      JSONArray intervals = new JSONArray(response.toString());
      if (intervals.length() > 0) {
        JSONObject json = new JSONObject(intervals.get(0).toString()).getJSONObject("result");
        maxTime = json.getString("maxTime");
        minTime = json.getString("minTime");
      } else {
        log.warn("No meta data for data source: )" + datasource);
      }
    }
  }

  public JSONArray getSegmentMetadata(String datasource, String maxTime, String minTime) throws Exception {
    log.info("Extract metadata of segment from " + maxTime + " to " + minTime);
    DateTime max_datetime = DATETIME_FORMAT.parseDateTime(maxTime);
    String interval = DATETIME_FORMAT.print(max_datetime.minusHours(1)) + "/" + maxTime;
    String query = SEGMENT_QUERY.replace("$DATASOURCE", datasource).replace("$INTERVAL", interval);
    URL url = new URL(druid_host);
    HttpURLConnection con = (HttpURLConnection) url.openConnection();
    con.setConnectTimeout(TIMEOUT);
    con.setRequestMethod("POST");
    con.setDoOutput(true);
    con.setRequestProperty("Content-Type", "application/json");
    DataOutputStream out = new DataOutputStream(con.getOutputStream());
    out.writeBytes(query);
    out.flush();
    out.close();
    int response_code = con.getResponseCode();

    if (response_code == 200) {
      BufferedReader br = new BufferedReader(new InputStreamReader(con.getInputStream()));
      String line = "";
      StringBuffer response = new StringBuffer();
      while ((line = br.readLine()) != null) {
        response.append(line);
      }
      br.close();
      con.disconnect();
      JSONArray seg_metadata = new JSONArray(response.toString());
      if (seg_metadata.length() > 0) {
        return seg_metadata;
      } else {
        log.info("No Segment Metadata for Data Source: " + datasource);
        return null;
      }
    } else {
      con.disconnect();
      throw new Exception("Fail to get response from " + druid_host + ", Response Code: " + response_code);
    }
  }

  public void format_field_metadata(String datasource, JSONArray seg_metadata) throws Exception {
    FileWriter druid_col_csv_file = new FileWriter(druid_col_metadata_csv_file, true);
    for (int i = 0; i < seg_metadata.length(); i++) {
      JSONObject segment = new JSONObject(seg_metadata.get(i).toString());
      JSONObject input_columns = segment.getJSONObject("columns");
      Iterator itr = input_columns.keys();
      while (itr.hasNext()) {
        String col_name = itr.next().toString();
        HashMap<String, String> dict_field = new HashMap<String, String>();
        JSONObject col = input_columns.getJSONObject(col_name);
        dict_field.put("datasource", datasource);
        dict_field.put("intervals", segment.getString("intervals"));
        dict_field.put("name", col_name);
        dict_field.put("type", col.getString("type"));
        dict_field.put("size", col.getString("size"));
        dict_field.put("cardinality", col.getString("cardinality"));
        dict_field.put("errorMessage", col.getString("errorMessage"));
        dict_field.put("urn", "druid:///" + "datasources/" + datasource);

        String out =
            dict_field.get("datasource") + "\t" + dict_field.get("urn") + "\t" + dict_field.get("intervals") + "\t"
                + dict_field.get("name") + "\t" + dict_field.get("type") + "\t" + dict_field.get("size") + "\t"
                + dict_field.get("cardinality") + "\t" + dict_field.get("errorMessage") + "\n";
        druid_col_csv_file.write(out);
      }
    }
    druid_col_csv_file.close();
  }

  public void format_dataset_metadata(String datasource, JSONArray seg_metadata) throws Exception {
    FileWriter druid_ds_csv_file = new FileWriter(druid_ds_metadata_csv_file, true);

    for (int i = 0; i < seg_metadata.length(); i++) {
      JSONObject segment = new JSONObject(seg_metadata.get(i).toString());
      HashMap<String, String> dict_dataset = new HashMap<String, String>();

      JSONObject schema = new JSONObject();
      schema.put("type", "record");
      schema.put("uri", "druid:///" + "datasources/" + datasource);
      schema.put("name", datasource);
      JSONObject input_columns = segment.getJSONObject("columns");
      ArrayList<JSONObject> fields = new ArrayList<JSONObject>();
      Iterator itr = input_columns.keys();
      while (itr.hasNext()) {
        JSONObject field = new JSONObject();
        String col_name = itr.next().toString();
        JSONObject col = input_columns.getJSONObject(col_name);

        field.put("type", col.getString("type"));
        field.put("name", col_name);
        fields.add(field);
      }
      schema.put("fields", fields);

      dict_dataset.put("name", datasource);
      dict_dataset.put("schema", schema.toString());
      dict_dataset.put("schema_type", "JSON");
      dict_dataset.put("properties", minTime + "/" + maxTime);
      dict_dataset.put("fields", fields.toString());
      dict_dataset.put("urn", "druid:///" + "datasources/" + datasource);
      dict_dataset.put("source", "Druid");
      dict_dataset.put("storage_type", "Table");
      dict_dataset.put("is_partitioned", "T");

      String out =
          dict_dataset.get("name") + "\t" + dict_dataset.get("schema") + "\t" + dict_dataset.get("schema_type") + "\t"
              + dict_dataset.get("properties") + "\t" + dict_dataset.get("fields") + "\t" + dict_dataset.get("urn")
              + "\t" + dict_dataset.get("source") + "\t" + dict_dataset.get("storage_type") + "\t" + dict_dataset.get(
              "is_partitioned") + "\n";
      druid_ds_csv_file.write(out);
    }
    druid_ds_csv_file.close();
  }
}
