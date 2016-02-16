# WhereHows

WhereHows is a data discovery and lineage tool built at LinkedIn. It integrates with all the major data processing systems and collects both catalog and operational metadata from them. 

Within the central metadata repository, WhereHows curates, associates, and surfaces the metadata information through two interfaces: 
* a web application that enables data & linage discovery, and community collaboration
* an API endpoint that empowers automation of data processes/applications 

WhereHows serves as the single platform that:
* links data objects with people and processes
* enables crowdsourcing for data knowledge
* provides data governance and provenance based on ownership and lineage
 
## Documentation

The detailed information can be found in the [Wiki][wiki]


## Examples in VM

There is a pre-built vmware image (about 11GB) to quickly demonstrate the functionality of WhereHows. Check out the [VM Guide][VM]


## Getting Started

New to Wherehows? Check out the [Getting Started Guide][GS]

### Preparation

First, please get Play Framework and Gradle in place.
```
wget http://downloads.typesafe.com/play/2.2.4/play-2.2.4.zip
wget http://services.gradle.org/distributions/gradle-2.4-bin.zip

# Unzip, Remove zipped folder, move gradle/play folder to $HOME
unzip gradle-2.4-bin.zip && rm gradle-2.4-bin.zip && mv gradle-2.4 $HOME/
unzip play-2.2.4.zip && rm play-2.2.4.zip && mv play-2.2.4 $HOME/

# Add PLAY_HOME, GRADLE_HOME. Update Path to include new gradle, alias to counteract issues
echo 'export PLAY_HOME="$HOME/play-2.2.4"' >> ~/.bashrc
echo 'export GRADLE_HOME="$HOME/gradle-2.4"' >> ~/.bashrc
echo 'export PATH=$PATH:$GRADLE_HOME/bin' >> ~/.bashrc
echo 'alias gradle=$GRADLE_HOME/bin/gradle' >> ~/.bashrc
source ~/.bashrc
```

You need to update the file $PLAY_HOME/framework/build to increase the **JVM stack size** (-Xss1M) to 2M or more.

Second, please [setup the metadata repository][DB] in MySQL. 
```
CREATE DATABASE wherehows
  DEFAULT CHARACTER SET utf8
  DEFAULT COLLATE utf8_general_ci;

CREATE USER 'wherehows';
SET PASSWORD FOR 'wherehows' = PASSWORD('wherehows');
GRANT ALL ON wherehows.* TO 'wherehows'
```

Execute the [DDL files][DDL] to create the required repository tables in **wherehows** database.


### Build

1. Get the source code: ```git clone git@github.com:linkedin/WhereHows.git```
2. Put a few 3rd-party jar files to **metadata-etl/extralibs** directory. Some of these jar files may not be available in Maven Central or Artifactory. See [the download instrucitons][EXJAR] for more detail. ```cd WhereHows/metadata-etl/extralibs``` 
3. Go back to the **WhereHows** root directory and build all the modules: ```gradle build```
4. Go back to the **WhereHows** root directory and start the metadata ETL and API service: ```cd backend-service ; $PLAY_HOME/play run```
5. Go back to the **WhereHows** root directory and start the web front-end: ```cd web ; $PLAY_HOME/play run``` Then WhereHows UI is available at http://localhost:9000 by default. For example, ```play run -Dhttp.port=19001``` will use port 19001 to serve UI.

## Contribute

Want to contribute? Check out the [Contributors Guide][CON]

[wiki]: https://github.com/LinkedIn/Wherehows/wiki
[GS]: https://github.com/LinkedIn/Wherehows/wiki/Getting-Started
[CON]: https://github.com/LinkedIn/Wherehows/wiki/Contributing
[VM]: https://github.com/LinkedIn/Wherehows/wiki/Quick-Start-With-VM
[EXJAR]: https://github.com/LinkedIn/Wherehows/wiki/Getting-Started#download-third-party-jar-files
[DDL]: https://github.com/linkedin/WhereHows/tree/master/data-model/DDL
[DB]: https://github.com/LinkedIn/Wherehows/wiki/Getting-Started#set-up-your-database
