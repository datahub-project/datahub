# DataHub Frontend
DataHub frontend is a [Play](https://www.playframework.com/) service written in Java. It is served as a mid-tier
between [DataHub GMS](../gms) which is the backend service and [DataHub Web](../datahub-web).

## Pre-requisites
* You need to have [JDK8](https://www.oracle.com/java/technologies/jdk8-downloads.html) 
installed on your machine to be able to build `DataHub Frontend`.
* You need to have [Chrome](https://www.google.com/chrome/) web browser 
installed to be able to build because UI tests have a dependency on `Google Chrome`.

## Build
`DataHub Frontend` is already built as part of top level build:
```
./gradlew build
```
However, if you only want to build `DataHub Frontend` specifically:
```
./gradlew :datahub-frontend:build
```

## Dependencies
Before starting `DataHub Frontend`, you need to make sure that [DataHub GMS](../gms) and 
all its dependencies have already started and running.

Also, user information should already be registered into the DB,
otherwise user will not be able to sign in. 
To do that, first create a file named `user.dat` containing below line and filling the parts `<<something>>` 
with your information:
```
{"auditHeader": None, "proposedSnapshot": ("com.linkedin.pegasus2avro.metadata.snapshot.CorpUserSnapshot", {"urn": "urn:li:corpuser:<<username>>", "aspects": [{"active": True, "fullName": "<<Full Name>>", "email": "<<e-mail address>>"}, {}]}), "proposedDelta": None}
```
And run `mce producer` script as below:
```
python metadata-ingestion/mce_cli.py produce -d user.dat
```

Or, you can run the script without providing any data file. In this case, the script will use `bootstrap_mce.dat` file
to bootstrap some sample users and datasets:
```
python metadata-ingestion/mce_cli.py produce
```
This will create a default user with username `datahub`. You can sign in to the app using `datahub` as your username.

## Start via Docker image
Quickest way to try out `DataHub Frontend` is running the [Docker image](../docker/frontend).

## Start via command line
If you do modify things and want to try it out quickly without building the Docker image, you can also run
the application directly from command line after a successful [build](#build):
```
cd datahub-frontend/run && ./run-local-frontend
```

## Checking out DataHub UI
After starting your application in one of the two ways mentioned above, you can connect to it by typing below 
into your favorite web browser:
```
http://localhost:9001
```
To be able to sign in, you need to provide your user name. You don't need to type any password.

## Sample API calls
All APIs for the application are defined in [routes](conf/routes) file. Below, you can find sample curl calls to these APIs
and their responses.

### Browse APIs
#### Getting current browse path
```
http://localhost:9001/api/v2/browsePaths?type=dataset&urn=urn:li:dataset:(urn:li:dataPlatform:kafka,pageviews,PROD)
[
"/PROD/kafka/pageviews"
]
```

#### Browsing datasets
```
http://localhost:9001/api/v2/browse?type=dataset&path=/prod
{
    "elements": [],
    "start": 0,
    "count": 0,
    "total": 1,
    "metadata": {
        "totalNumEntities": 1,
        "path": "",
        "groups": [{
            "name": "prod",
            "count": 1
        }]
    }
}
```

### Search APIs
#### Search query
```
http://localhost:9001/api/v2/search?type=dataset&input=page
{
    "elements": [{
        "origin": "PROD",
        "name": "pageviews",
        "platform": "urn:li:dataPlatform:kafka"
    }],
    "start": 0,
    "count": 10,
    "total": 1,
    "searchResultMetadatas": [{
        "name": "platform",
        "aggregations": {
            "kafka": 1
        }
    }, {
        "name": "origin",
        "aggregations": {
            "prod": 1
        }
    }]
}
```

#### Autocomplete query
```
http://localhost:9001/api/v2/autocomplete?type=dataset&field=name&input=page
{
    "query": "page",
    "suggestions": ["pageviews"]
}
```

### Dataset APIs
#### Getting basic dataset metadata
```
http://localhost:9001/api/v2/datasets/urn:li:dataset:(urn:li:dataPlatform:kafka,pageviewsevent,PROD)
{
    "dataset": {
        "platform": "kafka",
        "nativeName": "pageviewsevent",
        "fabric": "PROD",
        "uri": "urn:li:dataset:(urn:li:dataPlatform:kafka,pageviewsevent,PROD)",
        "description": "",
        "nativeType": null,
        "properties": null,
        "tags": [],
        "removed": null,
        "deprecated": null,
        "deprecationNote": null,
        "decommissionTime": null,
        "createdTime": null,
        "modifiedTime": null
    }
}
```

#### Getting dataset schema
```
http://localhost:9001/api/v2/datasets/urn:li:dataset:(urn:li:dataPlatform:kafka,pageviews,PROD)/schema
{
    "schema": {
        "schemaless": false,
        "rawSchema": "{\"type\":\"record\",\"name\":\"MetadataChangeEvent\",\"namespace\":\"com.linkedin.pegasus2avro.mxe\",\"doc\":\"Kafka event for proposing a metadata change for an entity.\",\"fields\":[{\"name\":\"auditHeader\",\"type\":{\"type\":\"record\",\"name\":\"KafkaAuditHeader\",\"namespace\":\"com.linkedin.pegasus2avro.avro2pegasus.events\",\"doc\":\"Header\"}}]}",
        "keySchema": null,
        "columns": [{
            "id": null,
            "sortID": 0,
            "parentSortID": 0,
            "fieldName": "foo",
            "parentPath": null,
            "fullFieldPath": "foo",
            "dataType": "string",
            "comment": "Bar",
            "commentCount": null,
            "partitionedStr": null,
            "partitioned": false,
            "nullableStr": null,
            "nullable": false,
            "indexedStr": null,
            "indexed": false,
            "distributedStr": null,
            "distributed": false,
            "treeGridClass": null
        }],
        "lastModified": 0
    }
}
```

#### Getting owners of a dataset
```
http://localhost:9001/api/v2/datasets/urn:li:dataset:(urn:li:dataPlatform:kafka,pageviews,PROD)/owners
{
    "owners": [{
        "userName": "ksahin",
        "source": "UI",
        "namespace": "urn:li:corpuser",
        "name": "Kerem Sahin",
        "email": "ksahin@linkedin.com",
        "isGroup": false,
        "isActive": true,
        "idType": "USER",
        "type": "DataOwner",
        "subType": null,
        "sortId": null,
        "sourceUrl": null,
        "confirmedBy": "UI",
        "modifiedTime": 0
    }, {
        "userName": "datahub",
        "source": "UI",
        "namespace": "urn:li:corpuser",
        "name": "Data Hub",
        "email": "datahub@linkedin.com",
        "isGroup": false,
        "isActive": true,
        "idType": "USER",
        "type": "DataOwner",
        "subType": null,
        "sortId": null,
        "sourceUrl": null,
        "confirmedBy": "UI",
        "modifiedTime": 0
    }],
    "fromUpstream": false,
    "datasetUrn": "urn:li:dataset:(urn:li:dataPlatform:kafka,pageviews,PROD)",
    "lastModified": 0,
    "actor": "ksahin"
}
```

#### Getting dataset documents
```
http://localhost:9001/api/v2/datasets/urn:li:dataset:(urn:li:dataPlatform:kafka,pageviews,PROD)/institutionalmemory
{
    "elements": [{
        "description": "Sample doc",
        "createStamp": {
            "actor": "urn:li:corpuser:ksahin",
            "time": 0
        },
        "url": "https://www.linkedin.com"
    }]
}
```

#### Getting upstreams of a dataset
```
http://localhost:9001/api/v2/datasets/urn:li:dataset:(urn:li:dataPlatform:kafka,pageviews,PROD)/upstreams
[{
    "dataset": {
        "platform": "kafka",
        "nativeName": "pageViewsUpstream",
        "fabric": "PROD",
        "uri": "urn:li:dataset:(urn:li:dataPlatform:kafka,pageViewsUpstream,PROD)",
        "description": "",
        "nativeType": null,
        "properties": null,
        "tags": [],
        "removed": null,
        "deprecated": null,
        "deprecationNote": null,
        "decommissionTime": null,
        "createdTime": null,
        "modifiedTime": null
    },
    "type": "TRANSFORMED",
    "actor": "urn:li:corpuser:ksahin",
    "modified": null
}]
```

### User APIs
#### Getting basic user metadata
```
http://localhost:9001/api/v1/user/me
{
    "user": {
        "id": 0,
        "userName": "ksahin",
        "departmentNum": 0,
        "email": "ksahin@linkedin.com",
        "name": "Kerem Sahin",
        "pictureLink": "https://content.linkedin.com/content/dam/me/business/en-us/amp/brand-site/v2/bg/LI-Bug.svg.original.svg",
        "userSetting": null
    },
    "status": "ok"
}
```

#### Getting all users
```
http://localhost:9001/api/v1/party/entities
{
    "status": "ok",
    "userEntities": [{
        "label": "ksahin",
        "category": "person",
        "displayName": null,
        "pictureLink": "https://content.linkedin.com/content/dam/me/business/en-us/amp/brand-site/v2/bg/LI-Bug.svg.original.svg"
    }, {
        "label": "datahub",
        "category": "person",
        "displayName": null,
        "pictureLink": "https://content.linkedin.com/content/dam/me/business/en-us/amp/brand-site/v2/bg/LI-Bug.svg.original.svg"
    }]
}
```

## Authentication
DataHub frontend leverages [Java Authentication and Authorization Service (JAAS)](https://docs.oracle.com/javase/7/docs/technotes/guides/security/jaas/JAASRefGuide.html) to perform the authentication. By default we provided a [DummyLoginModule](app/security/DummyLoginModule.java) which will accept any username/password combination. You can update [jaas.conf](conf/jaas.conf) to match your authentication requirement. For example, use the following config for LDAP-based authentication,

```
WHZ-Authentication {
  com.sun.security.auth.module.LdapLoginModule sufficient
  userProvider="ldaps://<host>:636/dc=<domain>"
  authIdentity="{USERNAME}"
  userFilter="(&(objectClass=person)(uid={USERNAME}))"
  java.naming.security.authentication="simple"
  debug="false"
  useSSL="true";
};
```

Note that the special keyword `USERNAME` will be substituted by the actual username.  

### API Debugging
Most DataHub frontend API endpoints are protected using [Play Authentication](https://www.playframework.com/documentation/2.1.0/JavaGuide4), which means it requires authentication information stored in the cookie for the request to go through. This makes debugging using curl difficult. One option is to first make a curl call against the `/authenticate` endpoint and stores the authentication info in a cookie file like this

```
curl -c cookie.txt -d '{"username":"datahub", "password":"datahub"}' -H 'Content-Type: application/json' http://localhost:9001/authenticate
```

You can then make all subsequent calls using the same cookie file to pass the authentication check.

```
curl -b cookie.txt "http://localhost:9001/api/v2/search?type=dataset&input=page"
```
