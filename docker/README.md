
This directory contains the docker build directories and docker compose files.  It lets you build
standalone/isolated instances of mysql (configured with all the WhereHows tables), and 
another with the WhereHows web services.

In the steps that follow, a leading $ sign represents the shell, you do not need to type it.
All steps assume you are in the same directory as this readme, unless directed to change 
directories.

Setup
=====
To use this, follow these steps
1. $ cp env-template.txt .env
2. Edit .env as necessary.

Using Working Images
====================
If you want to re-use working images:

$ docker pull wherehows/mysql:1

$ docker pull wherehows/wherehows:1

You may skip down to the 'running' section.

Building Docker Images
======================

If you have code or SQL updates, you will need to re-build:

1. Build WhereHows by normal methods.
2. From the WhereHows source root:
   $ tar zcvf wherehows-built-lite.tar.gz backend-service web
3. Download jdk 8u101 for linux x64
4. Download activator 1.3.11 minimal

Some archives should be copied into directories.  These are ignored by git to keep 
the size down.

1. $ cp ~/Downloads/jdk-8u101-linux-x64.tar.gz wherehows/archives 
2. $ cp ~/Downloads/activator-1.3.11-minimal.tar.gz wherehows/archives 
3. $ cp $WORKSPACE/WhereHows/wherehows-built-lite.tar.gz wherehows/archives 

Building With SQL Updates
=========================
If you want to build the docker images with updated SQL scripts:
first make sure that WORKSPACE is set in your environment, and $WORKSPACE/WhereHows exists

1. $ cd mysql
2. $ ./copy-ddl.sh
3. $ cd ..
4. You should be in the 'server' directory now.
5. $ docker-compose build

Building With Code Updates
==========================
First re-build the WhereHows code, then do this from the WhereHows source root:

$ tar zcvf wherehows-built-lite.tar.gz backend-service web

IVY Updates
===========
Sometimes dependencies change, and the app needs those.  If necessary do this:
1. $ cd $HOME
2. $ tar zcvf ivy2.tar.gz .ivy2
3. $ mv ivy2.tar.gz $WORKSPACE/WhereHows/docker/wherehows/archives
4. re-build the docker wherehows image.

Running
========

1. Optional: $ source .env; rm -rf $EXTERNAL_DATA_DIR/mysql/*
2. Optional: clear out the logs directory on your host machine: $ source .env; rm -rf $EXTERNAL_LOGS_DIR/wherehows
3. $ docker-compose up

NOTE: If you are running the app after the first time, then 
I strongly recommend that you clear the mysql data directory before running. 
Sometimes the data directory is correct from a previous run, so you do not notice 
a new bug cropped up since then.  Sometimes the mysql data directory could be corrupted 
from a previous run, in that case you definitely need to delete that.  However, in production
you want to be sure that the application can resume with data produced from a past run.  So
you should also test by running without clearing the mysql data directory.

Troubleshooting
===============

You can monitor the logs from your local $EXTERNAL_LOGS_DIR folder.

You can log into the running docker instance like so:
1. $ docker ps
2. $ docker exec -u <user> -it <instance-name> /bin/bash

the user could be root or wherehows.  Keep in mind that the environment variables may 
not be set right if you log in as root to the wherehows container.

Alternatively, you can start the docker container as just a shell

$ docker run -it -u <user> <image_id>:<version> /bin/bash

