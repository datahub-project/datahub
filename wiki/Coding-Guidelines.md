# Code formatting
Use standard LinkedIn code style in Eclipse/Idea to format the code.

# Code component

* backend-service
`Backend Service. Include the Akka scheduler and all backend APIs`
* data-model
`DDL for data model. This module contains all table-created statements you should run in your MySQL service`
* hadoop-dataset-extractor-standalone
`HDFS crawler that runs on the (remote) Hadoop gateway.`
* metadata-etl
`All other backend ETL processes. This module contains Java and Jython code. Jython files are in the resource folder.`
* web
`Web UI. A standalone Play service`
* wherehows-common
`Utility functions and constant values`

# Code component dependency
![](media/code-components.png)

# Testing
In the metadata-etl model for local testing, it might have too many steps to set up a database and make all the configurations. So we provide a way to read all configurations from a local file. There is already a template of the configuration file in [local_test.properties](../wherehows-etl/src/main/resources/local_test.properties.template). You can find detail of properties in each job type's wiki.

If you are running the test through IDE, make sure the `local_test.properties` file location is correct (default at `~/.wherehows/local_test.properties`)

Also, each ETL job can be run as a standalone job. The command line format is :

`java -Dconfig=/path/to/config/file -cp "all the classpaths" metadata.etl.Launcher`

| parameter | meaning |
| -----|-----|
| -Dconfig | Config file location is the configs that you want to use to test the program. You can also sepcify each properties by using '-D', but it's more tedious|
| -cp | Class path is all jar files location after you build the project. Normally after `gradle build`, the folder `backend-service/lib` contain all jar files |

You can also add any JVM parameters such as `-Xms512M -Xmx1024M` for your testing purpose.

Example :
`java -Dconfig=/path/to/config/file -cp "lib/*" metadata.etl.Launcher`