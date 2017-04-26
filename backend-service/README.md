# Linkedin Wherehows - a Metadata data warehouse

Wherehows works by sending out ‘crawlers’ to capture metadata from databases, hdfs, directory services, schedulers, and data integration tools. The collected metadata is loaded into an integrated data warehouse. Wherehows provides a web-ui service and a backend service.

Wherehows comes in three operational components:
- A web-ui service
- Backend service
- Database schema for MySQL

The backend service provides the RESTful api but more importantly runs the ETL jobs that go and gather the metadata. The backend service relies heavily on the mysql wherehows database instance for configuration information and as a location for where the metadata will land.

The Web UI provides navigation between the bits of information and the ability to annotate the collected data with comments, ownership and more. The example below is for collecting Hive metadata collected from the Cloudera Hadoop VM


Configuration notes:
MySQL database for the Wherehows metadata database
```
host:	<mysqlhost>
db:     wherehows
user:	wherehows
pass:	wherehows
```
Wherehows application directory (in test):
```
Host:	<edge node>
Folder:	/opt/wherehows
```

# Key notes:

Please become familiar with these pages:
- https://github.com/linkedin/WhereHows/wiki/Architecture (Nice tech overview)
- https://github.com/linkedin/WhereHows
- https://github.com/LinkedIn/Wherehows/wiki/Getting-Started

### Build:
```
./gradlew build dist
```

### Install:
Download/upload the distribution binaries, unzip to
```
/opt/wherehows/wherehows-backend
```

Create temp space for wherehows
```
sudo mkdir /var/tmp/wherehows
sudo chmod a+rw /var/tmp/wherehows
sudo mkdir /var/tmp/wherehows/resource
```

```
cd /opt/wherehows/wherehows-backend
```
Ensure that wherehows configuration tables are initialized by running the insert scripts (download 1.9 KB wherehows.dump ). Please note, to change the mysql host property for wherehows database (on <mysqlhost>). The initial SQL:
~~~~
--
-- Dumping data for table `wh_etl_job`
--

INSERT INTO `wh_etl_job` VALUES (21,'HIVE_DATASET_METADATA_ETL','DATASET','5 * * * * ?',61,'DB',NULL,1470390365,'comments','','Y');



--
-- Dumping data for table `wh_etl_job_property`
--

INSERT INTO `wh_etl_job_property` VALUES (117,'HIVE_DATASET_METADATA_ETL',61,'DB','hive.metastore.jdbc.url','jdbc:mysql://10.153.252.111:3306/metastore','N','url to connect to hive metastore'),(118,'HIVE_DATASET_METADATA_ETL',61,'DB','hive.metastore.jdbc.driver','com.mysql.jdbc.Driver','N',NULL),(119,'HIVE_DATASET_METADATA_ETL',61,'DB','hive.metastore.password','hive','N',NULL),(120,'HIVE_DATASET_METADATA_ETL',61,'DB','hive.metastore.username','hive','N',NULL),(121,'HIVE_DATASET_METADATA_ETL',61,'DB','hive.schema_json_file','/var/tmp/wherehows/hive_schema.json','N',NULL),(122,'HIVE_DATASET_METADATA_ETL',61,'DB','hive.schema_csv_file','/var/tmp/wherehows/hive_schema.csv','N',NULL),(123,'HIVE_DATASET_METADATA_ETL',61,'DB','hive.field_metadata','/var/tmp/wherehows/hive_field_metadata.csv','N',NULL);

--
-- Table structure for table `wh_property`
--


--
-- Dumping data for table `wh_property`
--

INSERT INTO `wh_property` VALUES ('wherehows.app_folder','/var/tmp/wherehows','N',NULL),('wherehows.db.driver','com.mysql.jdbc.Driver','N',NULL),('wherehows.db.jdbc.url','jdbc:mysql://localhost/wherehows','N',NULL),('wherehows.db.password','wherehows','N',NULL),('wherehows.db.username','wherehows','N',NULL),('wherehows.ui.tree.dataset.file','/var/tmp/wherehows/resource/dataset.json','N',NULL),('wherehows.ui.tree.flow.file','/var/tmp/wherehows/resource/flow.json','N',NULL);


~~~~

The hive metastore (as MySQL database) properties need to match the hadoop cluster:
```
Host	 <metastore host>
Port	 3306
Username hive
Password hive
URL	 jdbc:mysql://<metastore host>:3306/metastore
```
Set the hive metastore driver class to ```com.mysql.jdbc.Driver```
other properties per configuration.

Ensure these JAR files are present
```
 lib/jython-standalone-2.7.0.jar
 lib/mysql-connector-java-5.1.36.jar
```

### Run
To run the backend service:

Set the variables in application.env to configure the application.

To Run backend service application on port 19001 (from the backend-service folder):
```
./runBackend
```

Open browser to ```http://<edge node>:19001/```
This will show ‘TEST’. This is the RESTful api endpoint


## Next steps
Once the Hive ETL is fully flushed out, look at the HDFS metadata ETL
Configure multiple Hive & HDFS jobs to gather data from all Hadoop clusters
Add additional crawlers, for Oracle, Teradata, ETL and schedulers

### Troubleshooting
To check the configuration properties:
```
select * from wh_etl_job;
select * from wh_etl_job_property;
select * from wh_property;

select distinct wh_etl_job_name from wh_etl_job;

select j.wh_etl_job_name, j.ref_id_type, j.ref_id,
       coalesce(d.db_code, a.app_code) db_or_app_code,
       j.cron_expr, p.property_name, p.property_value
from wh_etl_job j join wh_etl_job_property p
  on j.wh_etl_job_name = p.wh_etl_job_name
 and j.ref_id_type = p.ref_id_type
 and j.ref_id = p.ref_id
     left join cfg_database d
  on j.ref_id = d.db_id
 and j.ref_id_type = 'DB'
     left join cfg_application a
  on j.ref_id = a.app_id
 and j.ref_id_type = 'APP'
where j.wh_etl_job_name = 'HIVE_DATASET_METADATA_ETL'
/*  AZKABAN_EXECUTION_METADATA_ETL
    AZKABAN_LINEAGE_METADATA_ETL
    ELASTICSEARCH_EXECUTION_INDEX_ETL
    HADOOP_DATASET_METADATA_ETL
    HADOOP_DATASET_OWNER_ETL
    HIVE_DATASET_METADATA_ETL
    KAFKA_CONSUMER_ETL
    LDAP_USER_ETL
    OOZIE_EXECUTION_METADATA_ETL
    ORACLE_DATASET_METADATA_ETL
    PRODUCT_REPO_METADATA_ETL
    TERADATA_DATASET_METADATA_ETL */
--  and j.ref_id = 123
/*  based on cfg_database or cfg_application */
order by j.wh_etl_job_name, db_or_app_code, p.property_name;
```
To log in the first time to the web UI:

You have to create an account. In the upper right corner there is a "Not a member yet? Join Now" link. Click on that and get a form to fill out.
