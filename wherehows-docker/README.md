# WhereHows Docker 
The docker directory contains all docker related source code.

## Quick Installation
1. install docker and docker-compose (http://www.docker.com)  
1. From the wherehows-docker, run ```build.sh 1```
1. Edit .env to match your environment
1. From the docker directory: ```$ docker-compose up```
1. In your local browser, open localhost:9000
1. Also, the backend app is hosted on localhost:9001

## About Docker?
Docker is a bit like a virtual machine. But unlike a virtual machine, rather than creating a whole virtual operating system, Docker allows applications to use the same Linux kernel as the system that they're running on and only requires applications be shipped with things not already running on the host computer. This gives a significant performance boost and reduces the size of the application.  

## Prerequisites
- Mac OS: Mac must be a 2010 or newer model, with Intel’s hardware support for memory management unit (MMU) virtualization; i.e., Extended Page Tables (EPT) and Unrestricted Mode.
- Windows: Docker for Windows requires Microsoft Hyper-V to run.
- Linux: A 64-bit installation, Linux kernel version 3.10 or higher (ie. RHEL 7.x above)

> First ensure that docker and docker-compose are installed, and that the user running this is a member of the docker group, or is root.
> The docker compose script uses version 3, so be sure that the version you install supports that.
## Installation
https://docs.docker.com/engine/installation/


## Set up your environment
Edit ```.env``` in wherehows-docker
```
# Secret
WHZ_SECRET=your_crpto_secret
  
# MySQL
WHZ_DB_NAME=wherehows
WHZ_DB_USERNAME=wherehows
WHZ_DB_PASSWORD=wherehows
  
# Elasticsearch
WHZ_SEARCH_ENGINE=elasticsearch
WHZ_ES_DATASET_URL=http://localhost:9200/wherehows/dataset/_search
WHZ_ES_METRIC_URL=http://localhost:9200/wherehows/metric/_search
WHZ_ES_FLOW_URL=http://localhost:9200/wherehows/flow_jobs/_search
  
# LDAP
WHZ_LDAP_URL=your_ldap_url
WHZ_LDAP_PRINCIPAL_DOMAIN=your_ldap_principal_domain
WHZ_LDAP_SEARCH_BASE=your_ldap_search_base
```

## Build Docker Container
> Usage: ./build.sh <:version>  
```
$ cd wherehows-docker
$ ./build.sh 1
Downloading https://services.gradle.org/distributions/gradle-4.0.2-bin.zip
................................................................
Unzipping /Users/anpark/.gradle/wrapper/dists/gradle-4.0.2-bin/be99721vbppnjuga6pbz1stgj/gradle-4.0.2-bin.zip to /Users/anpark/.gradle/wrapper/dists/gradle-4.0.2-bin/be99721vbppnjuga6pbz1stgj
Set executable permissions for: /Users/anpark/.gradle/wrapper/dists/gradle-4.0.2-bin/be99721vbppnjuga6pbz1stgj/gradle-4.0.2/bin/gradle
Starting a Gradle Daemon (subsequent builds will be fast  
  
...  
  
Step 17/19 : EXPOSE 9200 9300
 ---> Using cache
 ---> 2c00a373a30f
Step 18/19 : USER elasticsearch
 ---> Using cache
 ---> 982392d6568a
Step 19/19 : CMD elasticsearch
 ---> Using cache
 ---> abf88bc0cb74
Successfully built abf88bc0cb74
Successfully tagged linkedin/wherehows-elasticsearch:latest
now run this to start the application:
docker-compose up
``` 
 
 
## Run Docker Container
```
$ docker-compose up
Removing wherehowsdocker_wherehows-mysql_1
wherehowsdocker_wherehows-elasticsearch_1 is up-to-date
Recreating 78a95a2e6d00_wherehowsdocker_wherehows-mysql_1 ... 
Recreating 78a95a2e6d00_wherehowsdocker_wherehows-mysql_1 ... done
Recreating wherehowsdocker_wherehows-backend_1 ... 
Recreating wherehowsdocker_wherehows-frontend_1 ... 
Recreating wherehowsdocker_wherehows-backend_1
Recreating wherehowsdocker_wherehows-frontend_1 ... done
Attaching to wherehowsdocker_wherehows-elasticsearch_1, wherehowsdocker_wherehows-mysql_1, wherehowsdocker_wherehows-backend_1, wherehowsdocker_wherehows-frontend_1
  
...
  
wherehows-backend_1        | 2017-08-01 16:57:16 INFO  application:146 - Terminating KafkaConsumerMaster...
wherehows-backend_1        | 2017-08-01 16:57:16 INFO  application:65 - Enabled jobs: []
wherehows-backend_1        | 2017-08-01 16:57:16 INFO  p.a.Play:97 - Application started (Prod)
wherehows-backend_1        | 2017-08-01 16:57:17 INFO  p.c.s.NettyServer:165 - Listening for HTTP on /0.0.0.0:9000   

```

## Running Servers
Ensure all docker container instances are up and running
- Frontend app: 
  >http://localhost:9000
- Backend app: 
  >http://localhost:9001
- Elasticsearch Server: 
  >localhost:9200  
- MySQL: 
  >host:localhost port:3306


If any step fails in the script, you can run individual steps in it, the script is pretty intuitive and has comments.

