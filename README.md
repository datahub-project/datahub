# WhereHows [![Build Status](https://travis-ci.org/linkedin/WhereHows.svg?branch=master)](https://travis-ci.org/linkedin/WhereHows) [![latest](https://img.shields.io/badge/latest-1.0.0-blue.svg)](https://github.com/linkedin/WhereHows/releases) [![Gitter](https://img.shields.io/gitter/room/nwjs/nw.js.svg)](https://gitter.im/wherehows) [![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/LinkedIn/Wherehows/wiki/Contributing)


WhereHows is a data discovery and lineage tool built at LinkedIn. It integrates with all the major data processing systems and collects both catalog and operational metadata from them.

Within the central metadata repository, WhereHows curates, associates, and surfaces the metadata information through two interfaces:
* a web application that enables data & linage discovery, and community collaboration
* an API endpoint that empowers automation of data processes/applications

WhereHows serves as the single platform that:
* links data objects with people and processes
* enables crowdsourcing for data knowledge
* provides data governance and provenance based on ownership and lineage


## Who Uses WhereHows?
Here is a list of companies known to use WhereHows. Let us know if we missed your company!

* [LinkedIn](http://www.linkedin.com)
* [Overstock.com](http://www.overstock.com)
* [Fitbit](http://www.fitbit.com)
* [Carbonite](https://www.carbonite.com)


## How Is WhereHows Used?
How WhereHows is used inside of LinkedIn and other potential [use cases][USE].


## Documentation
The detailed information can be found in the [Wiki][wiki]


## Examples in VM (Deprecated)
There is a pre-built vmware image (about 11GB) to quickly demonstrate the functionality of WhereHows. Check out the [VM Guide][VM]

## WhereHows Docker
Docker can provide configuration free dev/production setup quickly, please check out [Docker Getting Start Guide](https://github.com/linkedin/WhereHows/tree/master/wherehows-docker/README.md)

## Getting Started
New to Wherehows? Check out the [Getting Started Guide][GS]


### Preparation
First, please [setup the metadata repository][DB] in MySQL.
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
1. Get the source code: ```git clone https://github.com/linkedin/WhereHows.git```
2. Put a few 3rd-party jar files to **wherehows-etl/extralibs** directory. Some of these jar files may not be available in Maven Central or Artifactory. See [the download instrucitons][EXJAR] for more detail. ```cd WhereHows/wherehows-etl/extralibs```
3. From the **WhereHows** root directory and build all the modules: ```./gradlew build```
4. Start the metadata ETL and API service: ```./gradlew wherehows-backend:runPlayBinary```
5. In a new terminal, start the web front-end: ```./gradlew wherehows-frontend:runPlayBinary```. The WhereHows UI is available at http://localhost:9001 by default. You can change the port number by editing the value of ```project.ext.httpPort``` in ```wherehows-frontend/build.gradle```.


## Roadmap
Check out the current [roadmap][RM] for WhereHows.


## Contribute
Want to contribute? Check out the [Contributors Guide][CON]


## Community
Want help? Check out the [Gitter chat room][GITTER] and [Google Groups][LIST]


[wiki]: https://github.com/LinkedIn/Wherehows/wiki
[GS]: https://github.com/linkedin/WhereHows/blob/master/wherehows-docs/getting-started.md
[CON]: https://github.com/linkedin/WhereHows/blob/master/wherehows-docs/contributing.md
[USE]: https://github.com/linkedin/WhereHows/blob/master/wherehows-docs/use-cases.md
[RM]: https://github.com/linkedin/WhereHows/blob/master/wherehows-docs/roadmap.md
[VM]: https://github.com/LinkedIn/Wherehows/wiki/Quick-Start-With-VM
[EXJAR]: https://github.com/linkedin/WhereHows/tree/master/wherehows-etl/extralibs
[DDL]: https://github.com/linkedin/WhereHows/tree/master/wherehows-data-model/DDL
[DB]: https://github.com/linkedin/WhereHows/blob/master/wherehows-docs/getting-started.md#database-setup
[LIST]: https://groups.google.com/forum/#!forum/wherehows
[GITTER]: https://gitter.im/wherehows
