---
title: "datahub-frontend"
---

# DataHub Frontend Proxy
DataHub frontend is a [Play](https://www.playframework.com/) service written in Java. It is served as a mid-tier
between [DataHub GMS](../metadata-service) which is the backend service and [DataHub Web](../datahub-web-react/README.md).

## Pre-requisites
* You need to have [JDK11](https://openjdk.org/projects/jdk/11/)
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
./gradlew :datahub-frontend:dist
```

## Dependencies
Before starting `DataHub Frontend`, you need to make sure that [DataHub GMS](../metadata-service) and
all its dependencies have already started and running.

## Start via Docker image
Quickest way to try out `DataHub Frontend` is running the [Docker image](../docker/datahub-frontend).

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
http://localhost:9002
```

To be able to sign in, you need to provide your user name. The default account is `datahub`, password `datahub`.

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

### Authentication in React
The React app supports both JAAS as described above and separately OIDC authentication. To learn about configuring OIDC for React,
see the [OIDC in React](../docs/authentication/guides/sso/configure-oidc-react.md) document.


### API Debugging
Most DataHub frontend API endpoints are protected using [Play Authentication](https://www.playframework.com/documentation/2.1.0/JavaGuide4), which means it requires authentication information stored in the cookie for the request to go through. This makes debugging using curl difficult. One option is to first make a curl call against the `/authenticate` endpoint and stores the authentication info in a cookie file like this

```
curl -c cookie.txt -d '{"username":"datahub", "password":"datahub"}' -H 'Content-Type: application/json' http://localhost:9002/authenticate
```

You can then make all subsequent calls using the same cookie file to pass the authentication check.

```
curl -b cookie.txt "http://localhost:9001/api/v2/search?type=dataset&input=page"
```
