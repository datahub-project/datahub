# Getting Started

## External Systems
WhereHows runs in an complete data ecosystem. Before you run the WhereHows application, ensure that the corresponding external systems are running. For information about how to run those systems, refer to the specific system guide.

* [Hadoop](https://hadoop.apache.org/)
* [Hive](https://hive.apache.org/)
* [Oracle](https://www.oracle.com/database/index.html)
* [Teradata](http://www.teradata.com/)
* [Azkaban](http://oozie.apache.org/)


## Preparation

### Download source code
Clone the WhereHows source code

    git clone https://github.com/linkedin/WhereHows.git

### Download third-party JAR files
In order to fetch metadata from certain data sources, you need to download their proprietary JDBC drivers separately. These JAR files should be placed in `wherehows-etl/extralibs` folder to be automatically included as dependencies in [build.gralde](https://github.com/linkedin/WhereHows/blob/master/wherehows-etl/build.gradle). See [this file](https://github.com/linkedin/WhereHows/tree/master/wherehows-etl/extralibs) for more details.

### Build the source code
  
    ./gradlew build

### Edit the source code in your IDE

For IntelliJ
* Run `./gradlew idea`
* In IntelliJ "File -> Open", choose [WhereHows/build.gradle](https://github.com/linkedin/WhereHows/blob/master/build.gradle). Make sure to use JDK 1.8.

For Eclipse
* Run `./gradlew eclipse`
* In Eclipse "File -> Import -> Gradle -> Gradle Project"


## Database Setup
[Setup](https://dev.mysql.com/doc/refman/5.6/en/installing.html) your MySQL service, then run the following command in MySQL shell to create the "wherehows" database

    CREATE DATABASE wherehows
        DEFAULT CHARACTER SET utf8
        DEFAULT COLLATE utf8_general_ci;

Create a new database user "wherehows" with the password set as "wherehows". Grant all privileges for the "wherehows" database to this user.

    CREATE USER 'wherehows'@'localhost' IDENTIFIED BY 'wherehows';
    CREATE USER 'wherehows'@'%' IDENTIFIED BY 'wherehows';
    GRANT ALL ON wherehows.* TO 'wherehows'@'wherehows';
    GRANT ALL ON wherehows.* TO 'wherehows'@'%';
    
    CREATE USER 'wherehows_ro'@'localhost' IDENTIFIED BY 'readmetadata';
    CREATE USER 'wherehows_ro'@'%' IDENTIFIED BY 'readmetadata';
    GRANT SELECT ON wherehows.* TO 'wherehows_ro'@'localhost';
    GRANT SELECT ON wherehows.* TO 'wherehows_ro'@'%';

Run [this sql script](https://github.com/linkedin/WhereHows/blob/master/wherehows-data-model/DDL/create_all_tables_wrapper.sql) to create the required database tables.


## Elasticsearch Setup
WhereHows uses Elasticsearch as its search engine. Please refer to the [Elasticsearch installation guide](https://www.elastic.co/guide/en/elasticsearch/guide/current/running-elasticsearch.html) for setup instructions.

Once you have configured Elasticsearch, run the following command to create `dataset`, `comment` and `field` mappings.

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

Run the following command to create `flow_job` nested object mappings.

    curl -XPUT '$YOUR_INDEX_URL:9200/wherehows/flow_jobs/_mapping' -d '
    {
      "flow_jobs": {
        "properties": {
          "jobs": {
            "type": "nested",
            "properties": {
              "job_name": { "type": "string"  },
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

Elasticsearch index building is configured as an ETL job in WhereHows. It can be manually triggered by running [ElasticSearchIndex.py](https://github.com/linkedin/WhereHows/blob/master/wherehows-etl/src/main/resources/jython/ElasticSearchIndex.py) script.


## Frontend Setup
WhereHows frontend and backend are independent services and need to be started separately.

Frontend is a standard Ember.js web app running on top of a Play server. Please refer to [Ember](https://www.emberjs.com/) & [Play](https://www.playframework.com/documentation) documents for more details on these frameworks.

### Configuration
All the frontend-specific configurations are stored in [wherehows-frontend/conf/application.conf](https://github.com/linkedin/WhereHows/blob/master/wherehows-frontend/conf/application.conf). You'll need to change it to match your environment.

### Development
Run the following command to launch an instance of frontend service locally for development.

    cd wherehows-frontend
    ../gradlew runPlayBinary

Note that it may appear that gradle is stuck when running the `runPlayBinary` command, but the app is actually running as soon as `Running at http://localhost:<port>/` is printed in the console. See [Running a Play Application](https://docs.gradle.org/4.1/userguide/play_plugin.html#play_continuous_build) for more details.


### Deployment
Run the following command to generate a distribution.

    cd wherehows-frontend
    ../gradlew dist

This generates a zip file in `wherehows-frontend/build/distributions`, which can be deployed to the target machine. Unzip and use `bin/playBinary` script to launch the service.


## Backend Setup
The backend service is also built on top of Play, so the setup is very similar to the frontend. You'll need to change the backend's [application.conf](https://github.com/linkedin/WhereHows/blob/master/wherehows-backend/conf/application.conf), and run the same gradle commands in `wherehows-backend` directory for development and deployment.

For configuring ETL jobs, please refer to [Set Up New Metadata ETL Jobs](https://github.com/linkedin/WhereHows/wiki/Set-Up-New-Metadata-ETL-Jobs).
