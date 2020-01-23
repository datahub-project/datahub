./gradlew build -x datahub-web:emberBuild -x datahub-web:emberWorkspaceTest -x datahub-frontend:unzipAssets
cd datahub-frontend/run && ./run-local-frontend