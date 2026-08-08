# Quick Run Tests with UI

1. Run quickstart
   `./gradlew quickstartDebug`

2. Load Cypress fixture data and run Cypress UI
   `./gradlew cypressDev`

## Run Tests Against Remote Instance

Warning: this does not load any data.
For now, either load data onto the remote instance manually, or only run tests that do not require preset data.

`./gradlew :smoke-test:cypressRemote -PbaseUrl=<url> -Pusername=<username> -Ppassword=<password>`

### Run Tests Against Remote Instance with Local Frontend

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
