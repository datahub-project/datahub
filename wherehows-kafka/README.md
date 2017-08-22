# Wherehows Kafka metadata event listener/consumer
The module is a deployable listener/consumer can receive and process Kafka event.
The module doesn't reply on wherehows-backend or wherehows-frontend, it can run indenpendently.

## Key notes
Please become familiar with these pages:
- https://github.com/linkedin/WhereHows/wiki/Architecture (Nice tech overview)
- https://github.com/linkedin/WhereHows
- https://github.com/linkedin/WhereHows/blob/master/wherehows-docs/getting-started.md


## Build
```
$ ./gradlew build
  
Starting a Gradle Daemon (subsequent builds will be faster)

BUILD SUCCESSFUL in 19s
12 actionable tasks: 5 executed, 7 up-to-date
```

## Configuration
Kafka module has a separate configuration file in **wherehows-kafka/application.env**
```
  
# Database Connection
WHZ_DB_NAME="wherehows"
WHZ_DB_USERNAME="wherehows"
WHZ_DB_PASSWORD="wherehows"
  
# Hibernate HIKARICP connection driver name
# HikariCP is an open source JDBC connection pooling library
hikaricp.dataSourceClassName = "com.mysql.jdbc.jdbc2.optional.MysqlDataSource"

# dialect for DB
# example values for different db: org.hibernate.dialect.MySQL5InnoDBDialect, org.hibernate.dialect.H2Dialect
hikaricp.dialect = "org.hibernate.dialect.MySQL5InnoDBDialect"
hikaricp.dialect = ${?WHZ_DB_DIALECT}

# Directory for kafka consumers
kafka.consumer.dir = "/var/tmp/consumers"
```


## Run
Use gradle command "gradle run" to start the application
```
> Task :wherehows-kafka:compileJava
Note: Some input files use unchecked or unsafe operations.
Note: Recompile with -Xlint:unchecked for details.

> Task :wherehows-kafka:run
Aug 21, 2017 6:26:32 PM org.hibernate.jpa.internal.util.LogHelper logPersistenceUnitInformation
INFO: HHH000204: Processing PersistenceUnitInfo [
....
INFO kafka.client.ClientUtils$ - Fetching metadata from broker BrokerEndPoint(649) 
with correlation id 0 for 1 topic(s) .....

```

## Troubleshooting

