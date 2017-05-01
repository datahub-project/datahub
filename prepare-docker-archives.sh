#!/bin/bash

# assumption: docker is already installed

# ensure the SBT opts are sufficient so the application will build
if [ -z "SBT_OPTS" ]; then
  export SBT_OPTS='-Xms1G -Xmx1G -Xss2M'
fi

if [ -d "web/target" ]; then
  # sometimes this directory causes problems if it exists when the build starts.
  rm -rf web/target
fi

# build the application's distribution zip
./gradlew dist

# move those to a directory where we will work on them more.
mv backend-service/target/universal/*.zip docker/archive-factory/originals/backend.zip
mv web/target/universal/*.zip docker/archive-factory/originals/web.zip

cd docker/archive-factory

# unfortunately those zip files are not quite what we want to start with in docker,
# run these other scripts that will revise them.
./build-backend.sh
./build-web.sh

# build the docker images
cd ../mysql
docker build -t wherehows/mysql .
cd ../backend-service
docker build -t wherehows/backend .
cd ../web
docker build -t wherehows/web .

cd ..
echo "now run this to start the application:"
echo "docker-compose up"
echo "you may need to edit the .env file first."
