# WhereHows Frontend UI 
The Web UI provides navigation between the bits of information and the ability to annotate the collected data with comments, ownership and more. The example below is for collecting Hive metadata collected from the Cloudera Hadoop VM

Wherehows comes in three operational components:
- [Backend service](../wherehows-backend/README.md)
- **A web-ui service**
- Database schema for MySQL

## Key notes
Please become familiar with these pages:
- https://github.com/linkedin/WhereHows/wiki/Architecture (Nice tech overview)
- https://github.com/linkedin/WhereHows
- https://github.com/linkedin/WhereHows/blob/master/wherehows-docs/getting-started.md


## Build
```
$ ../gradlew build dist  
  
Starting a Gradle Daemon (subsequent builds will be faster)

> Task :wherehows-web:bowerInstall
bower ember-cli-shims extra-resolution Unnecessary resolution: ember-cli-shims#0.1.3
bower bootstrap       extra-resolution Unnecessary resolution: bootstrap#3.3.7

> Task :wherehows-web:emberBuild
...
 - dist/legacy-app/vendors/toastr/toastr.min-c4d50504a82305d607ae5ff7b33e0c39.css: 5.85 KB (2.68 KB gzipped)
 - dist/legacy-app/vendors/toastr/toastr.min-d59436971aa13b0e0c24d4332543fbef.js: 4.87 KB (1.91 KB gzipped)


BUILD SUCCESSFUL in 56s
21 actionable tasks: 9 executed, 12 up-to-date
```

## Install (In Production)
Download/upload the distribution binaries, unzip to
**/opt/wherehows/wherehows-frontend**


Create temp space for wherehows
```
$ sudo mkdir /var/tmp/wherehows
$ sudo chmod a+rw /var/tmp/wherehows
$ sudo mkdir /var/tmp/wherehows/resource
```

```
$ cd /opt/wherehows/wherehows-frontend
```

## Configuration
Forntend has a seperate configuration file in **wherehows-frontend/application.env**
```
# Secret Key
WHZ_SECRET="change_me"
  
# Database Connection
WHZ_DB_NAME="wherehows"
WHZ_DB_USERNAME="wherehows"
WHZ_DB_PASSWORD="wherehows"
  
# Fully qualified jdbc url
WHZ_DB_URL="jdbc:mysql://localhost/wherehows"
  
# Serach Engine
WHZ_SEARCH_ENGINE=elasticsearch
  
# Elasticsearch (Change "localhost" to your Es host )
WHZ_ES_DATASET_URL="http://localhost:9200/wherehows/dataset/_search"
WHZ_ES_METRIC_URL="http://localhost:9200/wherehows/metric/_search"
WHZ_ES_FLOW_URL="http://localhost:9200/wherehows/flow_jobs/_search"
  
# LDAP
WHZ_LDAP_URL=your_ldap_url
WHZ_LDAP_PRINCIPAL_DOMAIN=your_ldap_principal_domain
WHZ_LDAP_SEARCH_BASE=your_ldap_search_base

# Piwik tracking configuration
PIWIK_SITE_ID="0000" # change_to_your_piwik_id
PIWIK_URL="change_to_your_piwik_url"

```


## Run
To run frontend app, go to **wherehows-frontend**
```
$ ./runFrontend
   
NettyServer.main is deprecated. Please start your Play server with the
2017-08-02 14:19:58 INFO  p.a.Play:97 - Application started (Prod)
2017-08-02 14:19:58 INFO  p.c.s.NettyServer:165 - Listening for HTTP on /0:0:0:0:0:0:0:0:9001

```

Open browser to ```http://<edge node>:9001/```
This will show WhereHows login page. 

## Troubleshooting
- To log in the first time to the web UI:
   You have to create an account. In the upper right corner there is a "Not a member yet? Join Now" link. Click on that and get a form to fill out.
 
