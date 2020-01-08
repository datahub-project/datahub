> This doc is for older versions (v0.2.1 and before) of WhereHows. Please refer to [this](../wherehows-docs/getting-started.md) for the latest version.


1. [External Systems](#external-systems)
2. [Preparation](#preparation)
3. [Set up database](#set-up-your-database)
4. [Set up elasticsearch index](#set-up-elasticsearch-index)
5. [Set up backend](#set-up-backend)
6. [Set up UI](#set-up-ui)

# External Systems
WhereHows runs in an complete data ecosystem. Before you run the WhereHows application, ensure that the corresponding external systems are running. For information about how to run those systems, refer to the specific system guide.
* [Hadoop](https://hadoop.apache.org/)
* [Teradata](http://www.teradata.com/)
* [Azkaban](https://azkaban.github.io/)
* [Oozie](http://oozie.apache.org/)

# Preparation
### Dependencies
* [Play][play] = 2.4.8
* [Java](https://www.java.com/en/download/) = 1.8
* [MySQL](https://www.mysql.com/) >= 5.6

### Download Play (Activator)
```bash
wget https://downloads.typesafe.com/typesafe-activator/1.3.11/typesafe-activator-1.3.11-minimal.zip
```

### Unzip and set ACTIVATOR_HOME
```bash
# Unzip, Remove zipped folder, move activator (play) folder to $HOME
unzip typesafe-activator-1.3.11-minimal.zip && rm typesafe-activator-1.3.11-minimal.zip && mv activator-1.3.11-minimal $HOME/

# Add ACTIVATOR_HOME to Path
echo 'export ACTIVATOR_HOME="$HOME/activator-1.3.11-minimal"' >> ~/.bashrc
source ~/.bashrc
```

### Update Play build option

In the next build step, you might see 'StackOverflowError'. That's because the JVM stack size is too small for the Play application compiling using SBT. You need to update the SBT max heap size to compile.
```bash
echo 'export SBT_OPTS="-Xms1G -Xmx1G -Xss2M"' >> ~/.bashrc
source ~/.bashrc
```

You may also see the following warning in the compile log, but it can be ignored.

```Java HotSpot(TM) 64-Bit Server VM warning: ignoring option MaxPermSize=512M; support was removed in 8.0```

### Download source code

Clone the [WhereHows](https://github.com/linkedin/WhereHows.git) source code
```
git clone https://github.com/linkedin/WhereHows.git
```

### Download Third-party JAR files
To connect to certain data sources to fetch metadata, you might need to download the corresponding JDBC drivers that are not included in WhereHows source code repository. You then need to copy these JARs into the **extralibs** directory under the **metadata-etl** folder.
* Teradata JDBC
<br>Download from https://downloads.teradata.com/download/connectivity/jdbc-driver and put tdgssconfig.jar and terajdbc4.jar into the **extralibs** directory
* Oracle JDBC
<br>(Not used yet.) Download ojdbc7.jar from http://www.oracle.com/technetwork/database/features/jdbc/default-2280470.html and put it into the **extralibs** directory

Reference the above third-party JAR files in the dependencies section inside **build.gradle**

### Build the source code
```bash
./gradlew build
```
>\* Some tests are disabled as they need extra configurations, such as connection information. Refer to [Coding Guidelines](Coding-Guidelines.md) for details about how to set up a local unit test.

#### Edit the source code in your IDE
* For IntelliJ
  * run ```./gradlew idea```
  * in IntelliJ "File -> Open" : WhereHows/build.gradle
  * "File -> Project Structure -> Project -> Project language level" : 8
* For Eclipse:
  * run ```./gradlew eclipse```
  * in Eclipse "File -> Import -> Gradle -> Gradle Project"

# Set up your database
Set up your MySQL service. Create all tables in the DDL folder.
* Create database "wherehows"

In a MySQL command shell:
```bash
CREATE DATABASE wherehows
  DEFAULT CHARACTER SET utf8
  DEFAULT COLLATE utf8_general_ci;
```

* Create a new database user "wherehows" with the password set as "wherehows". Grant all privileges for the "wherehows" database to this user.
```bash
CREATE USER 'wherehows'@'localhost' IDENTIFIED BY 'wherehows';
CREATE USER 'wherehows'@'%' IDENTIFIED BY 'wherehows';
GRANT ALL ON wherehows.* TO 'wherehows'@'wherehows';
GRANT ALL ON wherehows.* TO 'wherehows'@'%';

CREATE USER 'wherehows_ro'@'localhost' IDENTIFIED BY 'readmetadata';
CREATE USER 'wherehows_ro'@'%' IDENTIFIED BY 'readmetadata';
GRANT SELECT ON wherehows.* TO 'wherehows_ro'@'localhost';
GRANT SELECT ON wherehows.* TO 'wherehows_ro'@'%';
```
* Run the [sql files][wherehows DDL] to create the required database tables.

# Set up elasticsearch index
By default WhereHows uses mySQL FULL TEXT index as search engine, and user can configure Elasticsearch as search engine.

Install Elasticsearch and start the server.
Please refer to the [Elasticsearch installation guide](https://www.elastic.co/guide/en/elasticsearch/guide/current/running-elasticsearch.html).

Set up your Elasticsearch index.
* Create 'dataset', 'comment' and 'field' mappings.

In a command shell:
```bash
curl -XPUT '$YOUR_INDEX_URL:9200/wherehows' -d '
{
  "mappings": {
    "dataset": {},
    "comment": {
      "_parent": {
        "type": "dataset"
      }
    },
    "field": {
      "_parent": {
        "type": "dataset"
      }
    }
  }
}
'
```

* Create 'flow_job' nested object mappings.

In a command shell:
```bash
curl -XPUT '$YOUR_INDEX_URL:9200/wherehows/flow_jobs/_mapping' -d '
{
  "flow_jobs": {
    "properties": {
      "jobs": {
        "type": "nested",
        "properties": {
          "job_name":    { "type": "string"  },
          "job_path": { "type": "string"  },
          "job_type": { "type": "string"  },
          "pre_jobs": { "type": "string"  },
          "post_jobs": { "type": "string"  },
          "is_current": { "type": "string"  },
          "is_first": { "type": "string"  },
          "is_last": { "type": "string"  },
          "job_type_id": { "type": "short"   },
      "app_id": { "type": "short"   },
      "flow_id": { "type": "long"   },
      "job_id": { "type": "long"   }
        }
      }
    }
  }
}
'
```

* Build the Elasticsearch index

Elasticsearch index building configured as an ETL job in WhereHows, it will be automatically built when data loaded into mySQL database.

User can manually run [ElasticSearchIndex.py](../wherehows-etl/src/main/resources/jython/ElasticSearchIndex.py) to build the index.

# Set up UI
UI and backend services are independent services, so you can start each of them separately.

UI services is a standard play web application, reference [play documentation](https://www.playframework.com/documentation) for more detail of how to run a play application.

### Configure the search engine
If user wants to use Elasticsearch as search engine, please add below properties into application configuration file.

```bash
search.engine = "elasticsearch"
elasticsearch.dataset.url = "$YOUR_DATASET_INDEX_URL"
elasticsearch.flow.url = "$YOUR_FLOW_INDEX_URL"
```

### Start the web application server
Make sure your `web/conf/application.conf` file have the correct connection info for your mysql database. You can either edit it in your code or run with a specific configuration file location with '-Dconfig.file=$YOUR_CONFIG_FILE' option.

```bash
cd web
$ACTIVATOR_HOME/bin/activator run
```
WhereHows UI now is available at default: http://localhost:9000
You may need to specify the port if you have address conflict. For example ```activator run -Dhttp.port=9008``` will use port 9008 to serve UI. Detail reference [play tutorial](https://www.playframework.com/documentation/2.4.x/ProductionConfiguration)

### Deploying the web application server
```bash
cd web
./gradlew dist
```
The zip file is generated in target/universal.
Copy the zip file to your target machine. Then run the script in `bin/wherehows`.


# Set up backend
### Set up database
Backend service is also a play application.

Change the configuration file, fill in your connection info:
`backend-service/conf/application.conf`

### Start the backend server
```bash
cd backend-service
$ACTIVATOR_HOME/bin/activator run
```
Same as UI, you may need to customize the port by specify port parameter.

### Deploy
```bash
./gradlew build dist;
```
The zip file is generated in `target/universal`.
Copy the zip file to your target machine. Then run the script in `bin/backend-service`.

### Add jobs
All backend jobs need to set up the connection/security configurations. Refer to [Set Up New Metadata ETL Jobs](https://github.com/linkedin/WhereHows/wiki/Set-Up-New-Metadata-ETL-Jobs).


[Set-Up-A-New-Job]: Set-Up-New-Metadata-ETL-Jobs.md
[contributing_guide]: ../wherehows-docs/contributing.md
[gradle]: https://gradle.org/gradle-download/
[play]: https://www.playframework.com/download
[wherehows DDL]: ../wherehows-data-model/DDL
