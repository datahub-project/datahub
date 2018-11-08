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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import wherehows.common.Constant;


@Slf4j
public class DruidMetadataLoader {

  private String JDBC_URL;
  private String JDBC_DRIVER;
  private String JDBC_USERNAME;
  private String JDBC_PASSWORD;
  private String DB_ID;
  private String WH_ETL_EXEC_ID;
  private String druid_ds_metadata_csv_file;
  private String druid_col_metadata_csv_file;

  private String DROP_DS_METADATA = "DROP TABLE IF EXISTS wherehows.druid_ds_metadata_tbl; \n";
  private String CREATE_DS_METADAT =
      "CREATE TABLE wherehows.druid_ds_metadata_tbl " + "(" + "id INT AUTO_INCREMENT PRIMARY KEY,"
          + "name VARCHAR(100) NOT NULL," + "schema_desc VARCHAR(10000)," + "schema_type VARCHAR(20),"
          + "properties VARCHAR(500)," + "fields VARCHAR(10000)," + "urn VARCHAR(500)," + "source VARCHAR(50),"
          + "storage_type VARCHAR(20)," + "is_partitioned VARCHAR(20)"
          + ") ENGINE = InnoDB DEFAULT CHARSET = latin1; \n ";
  private String LOAD_DS_METADATA = "LOAD DATA LOCAL INFILE \"" + "$DRUID_DS_METADATA_CSV_FILE" + "\""
      + " INTO TABLE wherehows.druid_ds_metadata_tbl " + "FIELDS TERMINATED BY '\\t' " + "LINES TERMINATED BY '\\n'"
      + "(@name, @schema_desc, @schema_type, @properties, @fields, @urn, @source, @storage_type, @is_partitioned)"
      + "SET name=@name," + "schema_desc=@schema_desc," + "schema_type=@schema_type," + "properties=@properties,"
      + "fields=@fields," + "urn=@urn," + "source=@source," + "storage_type=@storage_type,"
      + "is_partitioned=@is_partitioned" + "\n;";

  private String DELETE_STG_DATASET_META = "DELETE FROM wherehows.stg_dict_dataset WHERE db_id = $DB_ID;\n";
  private String LOAD_STG_DATASET_META =
      "INSERT INTO wherehows.stg_dict_dataset" + "(db_id, name, `schema`, schema_type, properties, fields, urn, source,"
          + "location_prefix, parent_name, storage_type, dataset_type, is_partitioned, "
          + "source_created_time, source_modified_time, wh_etl_exec_id)" + "SELECT "
          + "$DB_ID, d.name, schema_desc, schema_type, properties, fields, urn, 'Druid',"
          + "NULL, NULL, 'TABLE', 'DRUID', is_partitioned, "
          + "UNIX_TIMESTAMP(STR_TO_DATE( substring_index(substring_index(properties, '/', 1), '.', 1), '%Y-%m-%dT%TZ')) , "
          + "UNIX_TIMESTAMP(STR_TO_DATE( substring_index(substring_index(properties, '/', -1), '.', 1), '%Y-%m-%dT%TZ')), "
          + "$WH_ETL_EXEC_ID  " + "FROM druid_ds_metadata_tbl d, "
          + "(SELECT MAX(id) id, name FROM wherehows.druid_ds_metadata_tbl GROUP BY name) t " + "WHERE t.id=d.id";
  private String DUMP_DS_METADATA =
      "INSERT INTO wherehows.dict_dataset " + "(" + "name, `schema`, schema_type, properties, fields, urn, source, "
          + "location_prefix, parent_name, storage_type, ref_dataset_id, " + "dataset_type, is_partitioned, "
          + "partition_layout_pattern_id, sample_partition_full_path, "
          + "source_created_time, source_modified_time, created_time, modified_time, wh_etl_exec_id, is_active " + ")"
          + "SELECT " + "name, `schema`, schema_type, properties, fields, urn, source, "
          + "location_prefix, parent_name, storage_type, ref_dataset_id, " + "dataset_type, is_partitioned, "
          + "partition_layout_pattern_id, sample_partition_full_path, "
          + "source_created_time, source_modified_time, created_time, UNIX_TIMESTAMP(now()), wh_etl_exec_id, 1 "
          + "FROM stg_dict_dataset s\n" + "WHERE db_id = $DB_ID " + "ON DUPLICATE KEY UPDATE "
          + "name = s.name, `schema`=s.schema, schema_type=s.schema_type, fields=s.fields, "
          + "properties=s.properties, source=s.source, location_prefix=s.location_prefix, "
          + "parent_name=s.parent_name, storage_type=s.storage_type, ref_dataset_id=s.ref_dataset_id, "
          + "dataset_type=s.dataset_type, hive_serdes_class=s.hive_serdes_class, "
          + "is_partitioned=s.is_partitioned, partition_layout_pattern_id = s.partition_layout_pattern_id, "
          + "sample_partition_full_path=s.sample_partition_full_path, source_created_time=s.source_created_time, "
          + "source_modified_time=s.source_modified_time, modified_time=UNIX_TIMESTAMP(NOW()), wh_etl_exec_id=s.wh_etl_exec_id;";
  private String DROP_COL_METADATA = "DROP TABLE IF EXISTS wherehows.druid_col_metadata_tbl; \n";
  private String CREATE_COL_METADAT =
      "CREATE TABLE wherehows.druid_col_metadata_tbl " + "(" + "id INT AUTO_INCREMENT PRIMARY KEY,"
          + "ds_name VARCHAR(100) NOT NULL," + "urn VARCHAR(500) NOT NULL," + "properties VARCHAR(200),"
          + "field_name VARCHAR(100) NOT NULL," + "data_type VARCHAR(200)," + "data_size VARCHAR(50),"
          + "cardinality VARCHAR(50)," + "error_message VARCHAR(500)"
          + ") ENGINE = InnoDB DEFAULT CHARSET = latin1; \n ";
  private String LOAD_COL_METADATA = "LOAD DATA LOCAL INFILE \"" + "$DRUID_COL_METADATA_CSV_FILE" + "\""
      + " INTO TABLE wherehows.druid_col_metadata_tbl " + "FIELDS TERMINATED BY '\\t' " + "LINES TERMINATED BY '\\n'"
      + "(@ds_name, @urn, @properties, @field_name, @data_type, @data_size, @cardinality, @error_message)"
      + "SET ds_name=@ds_name," + "urn=@urn," + "properties=@properties," + "field_name=@field_name,"
      + "data_type=@data_type," + "data_size=@data_size," + "cardinality=@cardinality," + "error_message=@error_message"
      + "\n;";
  private String DELETE_STG_FIELD_META = "DELETE FROM wherehows.stg_dict_field_detail WHERE db_id = $DB_ID";
  private String LOAD_STG_FIELD_META =
      "INSERT INTO stg_dict_field_detail" + "(" + "db_id, urn, sort_id, parent_sort_id,field_name, "
          + "data_type, data_size, " + "is_nullable, is_indexed, is_partitioned, " + "namespace, last_modified" + ") "
          + "SELECT " + "$DB_ID, urn, id, 1, field_name, " + "data_type, data_size," + "'F', 'T', 'T',"
          + "ds_name, NOW() " + "FROM druid_col_metadata_tbl " + ";\n";
  private String UPDATE_STG_FIELD_META =
      "UPDATE stg_dict_field_detail sf, dict_dataset d " + "SET sf.dataset_id=d.id WHERE sf.urn=d.urn "
          + "AND sf.db_id = $DB_ID;\n";
  private String DUMP_NON_EXIST_FIELD =
      "CREATE TEMPORARY TABLE IF NOT EXISTS t_deleted_fields (primary key (field_id))\n" + "SELECT x.field_id "
          + "FROM stg_dict_field_detail s " + "join dict_dataset i " + "ON s.urn=i.urn AND s.db_id=$DB_ID "
          + "RIGHT JOIN dict_field_detail x " + "ON i.id=x.dataset_id AND s.field_name = x.field_name "
          + "WHERE s.field_name IS NULL " + "AND x.dataset_id IN " + "("
          + "SELECT d.id dataset_id FROM stg_dict_field_detail k join dict_dataset d ON k.urn=d.urn AND k.db_id=$DB_ID"
          + ");\n";
  private String DELETE_NON_EXIST_FIELD =
      "DELETE FROM dict_field_detail WHERE field_id IN (SELECT field_id FROM t_deleted_fields)\n;";
  private String UPDATE_FIELD_METADATA = "UPDATE dict_field_detail t join " + "(" + "select x.field_id, s.*  "
      + "from stg_dict_field_detail s join dict_field_detail x "
      + "on s.field_name=x.field_name and s.namespace=x.namespace and s.dataset_id=x.dataset_id "
      + "where s.db_id=$DB_ID " + "and ( " + "x.data_type<>s.data_type " + "or x.data_size<>s.data_size " + ") "
      + ") p " + "on t.field_id=p.field_id " + "set t.data_type=p.data_type, " + "t.data_size=p.data_size, "
      + "t.modified = NOW();\n";
  private String INSERT_NEW_FIELD_METADATA =
      "INSERT IGNORE INTO dict_field_detail" + "(" + "dataset_id, sort_id, parent_sort_id, "
          + "field_name, namespace, data_type, data_size, " + "is_nullable, is_indexed, is_partitioned, is_distributed,"
          + "modified" + ")" + "SELECT " + "dataset_id, sort_id, parent_sort_id, "
          + "field_name, namespace, data_type, data_size, "
          + "is_nullable, is_indexed, is_partitioned, is_distributed, " + "last_modified "
          + "FROM stg_dict_field_detail sf " + "WHERE sf.db_id = $DB_ID AND sf.dataset_id is not NULL "
          + "AND (sf.urn, sf.db_id) not in (select urn, db_id from t_deleted_fields)"
          + "AND (sf.field_name, sf.dataset_id, namespace) not in (select field_name, dataset_id, namespace from dict_field_detail)"
          + ";\n";

  public DruidMetadataLoader(Properties prop) throws Exception {
    druid_ds_metadata_csv_file = prop.getProperty(Constant.DRUID_DATASOURCE_METADATA_CSV_FILE);
    druid_col_metadata_csv_file = prop.getProperty(Constant.DRUID_FIELD_METADATA_CSV_FILE);
    JDBC_URL = prop.getProperty(Constant.WH_DB_URL_KEY);
    JDBC_DRIVER = prop.getProperty(Constant.WH_DB_DRIVER_KEY);
    JDBC_USERNAME = prop.getProperty(Constant.WH_DB_USERNAME_KEY);
    JDBC_PASSWORD = prop.getProperty(Constant.WH_DB_PASSWORD_KEY);
    DB_ID = prop.getProperty(Constant.JOB_REF_ID_KEY);
    WH_ETL_EXEC_ID = prop.getProperty(Constant.WH_EXEC_ID_KEY);
    log.debug("druid_ds_metadata_csv_file=" + druid_ds_metadata_csv_file);
    log.debug("druid_col_metadata_csv_file=" + druid_col_metadata_csv_file);
    log.debug("JDBC_URL=" + JDBC_URL);
    log.debug("JDBC_DRIVER=" + JDBC_DRIVER);
    log.debug("JDBC_USERNAME=" + JDBC_USERNAME);
    log.debug("JDBC_PASSWORD=" + JDBC_PASSWORD);
    log.debug("DB_ID=" + DB_ID);
    log.debug("WH_ETL_EXEC_ID=" + WH_ETL_EXEC_ID);
  }

  public DruidMetadataLoader(String ds_csv_file, String col_csv_file, String db_id, String exec_id, String url,
      String driver, String usr, String pwd) throws Exception {
    if (ds_csv_file == null || ds_csv_file.length() == 0) {
      throw new Exception("CSV file is invalid");
    }
    if (url == null || url.length() == 0) {
      throw new Exception("JDBC URL is invalid");
    }
    if (driver == null || driver.length() == 0) {
      throw new Exception("JDBC Driver is invalid");
    }
    if (usr == null || usr.length() == 0) {
      throw new Exception("JDBC Username is invalid");
    }
    if (pwd == null || pwd.length() == 0) {
      throw new Exception("JDBC Password is invalid");
    }

    druid_ds_metadata_csv_file = ds_csv_file;
    druid_col_metadata_csv_file = col_csv_file;
    DB_ID = db_id;
    JDBC_URL = url;
    JDBC_DRIVER = driver;
    JDBC_USERNAME = usr;
    JDBC_PASSWORD = pwd;
    WH_ETL_EXEC_ID = exec_id;
  }

  public void run() throws Exception {
    loadDatasourceMetadata();
  }

  public void loadDatasourceMetadata() throws Exception {
    Class.forName(JDBC_DRIVER);
    Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USERNAME, JDBC_PASSWORD);
    Statement stmt = conn.createStatement();

    log.info("Load file " + druid_ds_metadata_csv_file + " to table 'dict_dataset'");
    log.debug("Running query:" + DROP_DS_METADATA);
    stmt.executeUpdate(DROP_DS_METADATA);
    log.debug("Running query:" + CREATE_DS_METADAT);
    stmt.executeUpdate(CREATE_DS_METADAT);
    log.debug("Running query:" + LOAD_DS_METADATA.replace("$DRUID_DS_METADATA_CSV_FILE", druid_ds_metadata_csv_file));
    stmt.executeUpdate(LOAD_DS_METADATA.replace("$DRUID_DS_METADATA_CSV_FILE", druid_ds_metadata_csv_file));
    log.debug("Running query:" + DELETE_STG_DATASET_META.replace("$DB_ID", DB_ID));
    stmt.executeUpdate(DELETE_STG_DATASET_META.replace("$DB_ID", DB_ID));
    log.debug(
        "Running query:" + LOAD_STG_DATASET_META.replace("$DB_ID", DB_ID).replace("$WH_ETL_EXEC_ID", WH_ETL_EXEC_ID));
    stmt.executeUpdate(LOAD_STG_DATASET_META.replace("$DB_ID", DB_ID).replace("$WH_ETL_EXEC_ID", WH_ETL_EXEC_ID));
    log.debug("Running query:" + DUMP_DS_METADATA.replace("$DB_ID", DB_ID));
    stmt.executeUpdate(DUMP_DS_METADATA.replace("$DB_ID", DB_ID));

    log.info("Load file " + druid_col_metadata_csv_file + " to table 'dict_field'");
    log.debug("Running query:" + DROP_COL_METADATA);
    stmt.executeUpdate(DROP_COL_METADATA);
    log.debug("Running query:" + CREATE_COL_METADAT);
    stmt.executeUpdate(CREATE_COL_METADAT);
    log.debug(
        "Running query:" + LOAD_COL_METADATA.replace("$DRUID_COL_METADATA_CSV_FILE", druid_col_metadata_csv_file));
    stmt.executeUpdate(LOAD_COL_METADATA.replace("$DRUID_COL_METADATA_CSV_FILE", druid_col_metadata_csv_file));
    log.debug("Running query:" + DELETE_STG_FIELD_META.replace("$DB_ID", DB_ID));
    stmt.executeUpdate(DELETE_STG_FIELD_META.replace("$DB_ID", DB_ID));
    log.debug("Running query:" + LOAD_STG_FIELD_META.replace("$DB_ID", DB_ID));
    stmt.executeUpdate(LOAD_STG_FIELD_META.replace("$DB_ID", DB_ID));
    log.debug("Running query:" + UPDATE_STG_FIELD_META.replace("$DB_ID", DB_ID));
    stmt.executeUpdate(UPDATE_STG_FIELD_META.replace("$DB_ID", DB_ID));
    log.debug("Running query:" + DUMP_DS_METADATA.replace("$DB_ID", DB_ID));
    stmt.executeUpdate(DUMP_NON_EXIST_FIELD.replace("$DB_ID", DB_ID));
    log.debug("Running query:" + DELETE_NON_EXIST_FIELD);
    stmt.executeUpdate(DELETE_NON_EXIST_FIELD);
    log.debug("Running query:" + UPDATE_FIELD_METADATA.replace("$DB_ID", DB_ID));
    stmt.executeUpdate(UPDATE_FIELD_METADATA.replace("$DB_ID", DB_ID));
    log.debug("Running query:" + INSERT_NEW_FIELD_METADATA.replace("$DB_ID", DB_ID));
    stmt.executeUpdate(INSERT_NEW_FIELD_METADATA.replace("$DB_ID", DB_ID));

    stmt.close();
  }
}
