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
```

```
cd /opt/wherehows/wherehows-backend
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

Ensure these JAR files are present
```
 lib/jython-standalone-2.7.0.jar
 lib/mysql-connector-java-5.1.36.jar
```

### Run
To run the backend service:

Set the variables in application.env to configure the application.

To Run backend service application on port 19001 (from the wherehows-backend folder):
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
To log in the first time to the web UI:

You have to create an account. In the upper right corner there is a "Not a member yet? Join Now" link. Click on that and get a form to fill out.
