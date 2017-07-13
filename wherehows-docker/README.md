The docker directory contains all docker related source code.

Here is how to use it:
1. First ensure that docker and docker-compose are installed, and that the user running this is a member of the docker group, or is root.
The docker compose script uses version 3, so be sure that the version you install supports that.
1. From the wherehows-docker, run build.sh
1. Edit .env to match your environment
1. From the docker directory: $ docker-compose up
1. In your local browser, open localhost:9000
1. Also, the backend app is hosted on localhost:9001

If any step fails in the script, you can run individual steps in it, the script is pretty intuitive and has comments.

Hope that helps.
