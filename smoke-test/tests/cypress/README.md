# Quick Run Tests with UI

1. Run quickstart
   `./gradlew quickstartDebug`

2. Load Cypress fixture data and run Cypress UI
   `./gradlew cypressDev`

## Run Cypress Tests Against Remote Instance

Warning: this does not load any data.
For now, either load data onto the remote instance manually, or only run tests that do not require preset data.

```
./gradlew :smoke-test:cypressTestRemote -PbaseUrl=<url> -Pusername=<username> -Ppassword=<password> -PtestDir=<path>
```

The `baseUrl` should be the URL of the remote instance with a trailing slash, e.g. `https://dev01.acryl.io/`.

The `testDir` path should be relative to the upper `cypress` directory, e.g. `cypress/customers/figma`.

### Open Cypress Against Remote Instance

`./gradlew :smoke-test:cypressRemote -PbaseUrl=<url> -Pusername=<username> -Ppassword=<password>`

### Open Cypress Against Remote Instance with Local Frontend

1. Build frontend

```
./gradlew :datahub-web-react:yarnBuild
```

2. Start frontend preview against remote instance

```
./gradlew :datahub-web-react:yarnPreview -Pproxy="<remote-instance-url>"
```

3. Start cypress against localhost:4173

```
./gradlew :smoke-test:cypressRemote -Pusername=<username> -Ppassword=<password>
```
