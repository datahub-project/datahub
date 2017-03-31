package metadata.etl.dataset.druid;

import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.Connection;

import org.hsqldb.persist.ScriptRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import wherehows.common.Constant;

public class DruidMetadataLoader {

	private String JDBC_URL;
	private String JDBC_DRIVER;
	private String JDBC_USERNAME;
	private String JDBC_PASSWORD;
	private String DB_ID;
	private String WH_ETL_EXEC_ID;
	private String druid_ds_metadata_csv_file;
	private String druid_col_metadata_csv_file;
	protected final Logger logger = LoggerFactory.getLogger(getClass());
	
	
	private String DROP_DS_METADATA = "DROP TABLE IF EXISTS wherehows.druid_ds_metadata_tbl; \n";
	private String CREATE_DS_METADAT = 	"CREATE TABLE wherehows.druid_ds_metadata_tbl " 
										  + "("
										  + "id INT AUTO_INCREMENT PRIMARY KEY,"
										  + "name VARCHAR(100) NOT NULL,"
										  + "schema_desc VARCHAR(10000),"
										  + "schema_type VARCHAR(20),"
										  + "properties VARCHAR(500),"
										  + "fields VARCHAR(10000),"
										  + "urn VARCHAR(500),"
										  + "source VARCHAR(50),"
										  + "storage_type VARCHAR(20),"
										  + "is_partitioned VARCHAR(20)"
										  + ") ENGINE = InnoDB DEFAULT CHARSET = latin1; \n ";
	private String LOAD_DS_METADATA = "LOAD DATA INFILE \"" + "$DRUID_DS_METADATA_CSV_FILE" + "\""
										  + " INTO TABLE wherehows.druid_ds_metadata_tbl "
										  + "FIELDS TERMINATED BY '\\t' "
										  + "LINES TERMINATED BY '\\n'"
										  + "(@name, @schema_desc, @schema_type, @properties, @fields, @urn, @source, @storage_type, @is_partitioned)"
										  + "SET name=@name,"
										  + "schema_desc=@schema_desc,"
										  + "schema_type=@schema_type,"
										  + "properties=@properties,"
										  + "fields=@fields,"
										  + "urn=@urn,"
										  + "source=@source,"
										  + "storage_type=@storage_type,"
										  + "is_partitioned=@is_partitioned"
										  + "\n;";
	
	private String DELETE_STG_META = "DELETE FROM wherehows.stg_dict_dataset WHERE db_id = $DB_ID;\n";
	private String LOAD_STG_META = "INSERT INTO wherehows.stg_dict_dataset"
									+ "(db_id, name, `schema`, schema_type, properties, fields, urn, "
									+ "location_prefix, parent_name, storage_type, dataset_type, is_partitioned, "
									+ "source_created_time, source_modified_time, wh_etl_exec_id)"
									+ "SELECT "
									+ "$DB_ID, d.name, schema_desc, schema_type, properties, fields, urn, "
									+ "NULL, NULL, 'TABLE', 'DRUID', is_partitioned, "
									+ "UNIX_TIMESTAMP(STR_TO_DATE( substring_index(substring_index(properties, '/', 1), '.', 1), '%Y-%m-%dT%TZ')) , "
									+ "UNIX_TIMESTAMP(STR_TO_DATE( substring_index(substring_index(properties, '/', -1), '.', 1), '%Y-%m-%dT%TZ')), "
									+ "$WH_ETL_EXEC_ID  "
									+ "FROM druid_ds_metadata_tbl d, "
									+ "(SELECT MAX(id) id, name FROM wherehows.druid_ds_metadata_tbl GROUP BY name) t "
									+ "WHERE t.id=d.id";
	private String DUMP_DS_METADATA = "INSERT INTO wherehows.dict_dataset "
									+ "("
									+ "name, `schema`, schema_type, properties, fields, urn, source, "
									+ "location_prefix, parent_name, storage_type, ref_dataset_id, "
									+ "status_id, dataset_type, is_partitioned, "
									+ "partition_layout_pattern_id, sample_partition_full_path, "
									+ "source_created_time, source_modified_time, created_time, modified_time, wh_etl_exec_id "
									+ ")"
									+ "SELECT "
									+ "name, `schema`, schema_type, properties, fields, urn, source, "
									+ "location_prefix, parent_name, storage_type, ref_dataset_id, "
									+ "status_id, dataset_type, is_partitioned, "
									+ "partition_layout_pattern_id, sample_partition_full_path, "
									+ "source_created_time, source_modified_time, created_time, UNIX_TIMESTAMP(now()), wh_etl_exec_id "
									+ "FROM stg_dict_dataset s\n"
									+ "WHERE db_id = $DB_ID "
									+ "ON DUPLICATE KEY UPDATE "
									+ "name = s.name, `schema`=s.schema, schema_type=s.schema_type, fields=s.fields, "
									+ "properties=s.properties, source=s.source, location_prefix=s.location_prefix, "
									+ "parent_name=s.parent_name, storage_type=s.storage_type, ref_dataset_id=s.ref_dataset_id, "
									+ "status_id=s.status_id, dataset_type=s.dataset_type, hive_serdes_class=s.hive_serdes_class, "
									+ "is_partitioned=s.is_partitioned, partition_layout_pattern_id = s.partition_layout_pattern_id, "
									+ "sample_partition_full_path=s.sample_partition_full_path, source_created_time=s.source_created_time, "
									+ "source_modified_time=s.source_modified_time, modified_time=UNIX_TIMESTAMP(NOW()), wh_etl_exec_id=s.wh_etl_exec_id;";
	
	public DruidMetadataLoader() throws Exception{
		druid_ds_metadata_csv_file = Constant.DRUID_DATASOURCE_METADATA_CSV_FILE;
		druid_col_metadata_csv_file = Constant.DRUID_FIELD_METADATA_CSV_FILE;
		JDBC_URL = Constant.WH_DB_URL_KEY;
		JDBC_DRIVER = Constant.WH_DB_DRIVER_KEY;
		JDBC_USERNAME = Constant.WH_DB_USERNAME_KEY;
		JDBC_PASSWORD = Constant.WH_DB_PASSWORD_KEY;
		DB_ID = Constant.DB_ID_KEY;
		WH_ETL_EXEC_ID = Constant.WH_EXEC_ID_KEY;
		
	}
	
	public DruidMetadataLoader(String ds_csv_file, String col_csv_file, String db_id, String exec_id, String url, String driver, String usr, String pwd) throws Exception{
		if (ds_csv_file==null || ds_csv_file.length()==0){
			throw new Exception("CSV file is not specified");
		}
		if (url==null || url.length()==0){
			throw new Exception("JDBC URL is not specified");
		}
		if (driver==null || driver.length()==0){
			throw new Exception("JDBC Driver is not specified");
		}
		if (usr==null || usr.length()==0){
			throw new Exception("JDBC Username is not specified");
		}
		if (pwd==null || pwd.length()==0){
			throw new Exception("JDBC Password is not specified");
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
	
	public void run() throws Exception{
		loadDatasourceMetadata();
	}
	
	public void loadDatasourceMetadata() throws Exception {
		Class.forName(JDBC_DRIVER);
		Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USERNAME, JDBC_PASSWORD);
		Statement stmt = conn.createStatement();
		stmt.executeUpdate(DROP_DS_METADATA);
		stmt.executeUpdate(CREATE_DS_METADAT);
		stmt.executeUpdate(LOAD_DS_METADATA.replace("$DRUID_DS_METADATA_CSV_FILE", druid_ds_metadata_csv_file));
		stmt.executeUpdate(DELETE_STG_META.replace("$DB_ID", DB_ID));
		stmt.executeUpdate(LOAD_STG_META.replace("$DB_ID", DB_ID).replace("$WH_ETL_EXEC_ID", WH_ETL_EXEC_ID));
		System.out.println(DUMP_DS_METADATA.replace("$DB_ID", DB_ID));
		stmt.executeUpdate(DUMP_DS_METADATA.replace("$DB_ID", DB_ID));
		stmt.executeBatch();
		stmt.close();
		
	}
	
	
	
}
