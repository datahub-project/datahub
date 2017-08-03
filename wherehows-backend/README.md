# Linkedin Wherehows - a Metadata data warehouse

Wherehows works by sending out ‘crawlers’ to capture metadata from databases, hdfs, directory services, schedulers, and data integration tools. The collected metadata is loaded into an integrated data warehouse. Wherehows provides a web-ui service and a backend service.

Wherehows comes in three operational components:
- **Backend service**
- [A web-ui service](../wherehows-frontend/README.md)
- Database schema for MySQL

The backend service provides the RESTful api but more importantly runs the ETL jobs that go and gather the metadata. The backend service relies heavily on the mysql wherehows database instance for configuration information and as a location for where the metadata will land.

Configuration notes
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

## Key notes
Please become familiar with these pages:
- https://github.com/linkedin/WhereHows/wiki/Architecture (Nice tech overview)
- https://github.com/linkedin/WhereHows
- https://github.com/linkedin/WhereHows/blob/master/wherehows-docs/getting-started.md

### Build
```
$ ./gradlew build dist
```

### Install (In Production)
Download/upload the distribution binaries, unzip to
```
/opt/wherehows/wherehows-backend
```

Create temp space for wherehows
```
$ sudo mkdir /var/tmp/wherehows
$ sudo chmod a+rw /var/tmp/wherehows
$ sudo mkdir /var/tmp/wherehows/resource
```

```
$ cd /opt/wherehows/wherehows-backend
```

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


### Run
To run the backend service:

Set the variables in application.env to configure the application.

To Run backend service application on port 9001 (from the wherehows-backend folder):
```
$ ./runBackend
```

Open browser to ```http://<edge node>:9001/```
This will show ‘TEST’. This is the RESTful api endpoint


## Next steps
Once the Hive ETL is fully flushed out, look at the HDFS metadata ETL
Configure multiple Hive & HDFS jobs to gather data from all Hadoop clusters
Add additional crawlers, for Oracle, Teradata, ETL and schedulers

## Troubleshooting

- Compile error with the below messages:
   ```
   TAliasClause aliasClouse = tablelist.getTable(i).getAliasClause();
   ^
   symbol:   class TAliasClause
   location: class UpdateStmt
   ...
   * What went wrong:
   Execution failed for task ':wherehows-etl:compileJava'.
   > Compilation failed; see the compiler error output for details.
   
   ```
   You should install extra libs:  [Install extra libs](https://github.com/linkedin/WhereHows/tree/master/wherehows-etl/extralibs)
   
-  Other Running library failure:  
   Ensure these JAR files are present in **wherehows-backend/build/stage/wherehows-backend/lib**
   ```
   ...
   gsp.jar
   hsqldb-hsqldb-1.8.0.10.jar
   mysql-mysql-connector-java-5.1.40.jar
   ojdbc7.jar
   terajdbc4.jar
   ...
   ```