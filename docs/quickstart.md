# DataHub Quickstart Guide

1. Install [docker](https://docs.docker.com/install/) and [docker-compose](https://docs.docker.com/compose/install/) (if using Linux). Make sure to allocate enough hardware resources for Docker engine. Tested & confirmed config: 2 CPUs, 8GB RAM, 2GB Swap area.
2. Open Docker either from the command line or the desktop app and ensure it is up and running.
3. Clone this repo and `cd` into the root directory of the cloned repository.
4. Run the following command to download and run all Docker containers locally:
    ```
    ./docker/quickstart.sh
    ```
    This step takes a while to run the first time, and it may be difficult to tell if DataHub is fully up and running from the combined log. Please use [this guide](debugging.md#how-can-i-confirm-if-all-docker-containers-are-running-as-expected-after-a-quickstart) to verify that each container is running correctly.
5. At this point, you should be able to start DataHub by opening [http://localhost:9001](http://localhost:9001) in your browser. You can sign in using `datahub` as both username and password. However, you'll notice that no data has been ingested yet.
6. To ingest provided [sample data](https://github.com/linkedin/datahub/blob/master/metadata-ingestion/mce-cli/bootstrap_mce.dat) to DataHub, switch to a new terminal window, `cd` into the cloned `datahub` repo, and run the following command:
    ```
    ./docker/ingestion/ingestion.sh
    ```
   After running this, you should be able to see and search sample datasets in DataHub.

Please refer to the [debugging guide](debugging.md) if you encounter any issues during the quickstart.
