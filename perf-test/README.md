# Load testing with Locust

[Locust](https://locust.io/) is an open-source, python-based, easy-to-use load testing tool. It provides an interface to
spawn multiple users (swarm) that behave according to pre-defined python code.

In this directory, we provide some example locust configs that send common requests to DataHub GMS (ingest, search,
browse, and graph).

## Prerequisites

To run the example configs, you need to first install locust by running

```shell
pip3 install locust
```

Note that it supports python versions 3.6 and up. Refer to
this [guide](https://docs.locust.io/en/stable/installation.html) for more details.

You will also need to import requirements in order to run the Locustfile scripts:
```shell
pip3 install -r requirements.txt
```

## Locustfiles

[Locustfiles](./locustfiles) define how the users will behave once they are spawned. Refer to
this [doc](https://docs.locust.io/en/stable/writing-a-locustfile.html) on how to write one.

Here, we have defined 4 common requests

- Ingest: ingests a dataset with a random URN with properties, browse paths, and ownership aspects filled out
- Search: searches datasets with query "test"
- Browse: browses datasets with path "/perf/test"
- Graph: gets datasets owned by user "common"

We will continue adding more as more use cases arise, but feel free to play around with the default behavior to create a
load test that matches your request pattern.

## Load testing

There are two ways to run locust. One is through the web interface, and the other is on the command line.

### Web interface

To run through the web interface, you can run the following

```shell
locust -f <<path-to-locustfile>>
```

For instance, to run ingest load testing, run the following from root of repo.

```shell
locust -f perf-test/locustfiles/ingest.py
```

This will set up the web interface in http://localhost:8089 (unless the port is already taken). Once you click into it,
you should see the following

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/locust-example.png"/>
</p>

Input the number of users you would like to spawn and the spawn rate. Point the host to the deployed DataHub GMS (
locally, it should be http://localhost:8080). Click on the "Start swarming" button to start the load test.

The web interface should give you statistics on number of requests, latency, response rate, etc.

### Command Line

To run on the command line, run the following

```shell
locust -f <<path-to-locustfile>> --headless -H <<host>> -u <<num-users>> -r <<spawn-rate>>
```

For instance, to replicate the setting in the previous section, run the following

```shell
locust -f perf-test/locustfiles/ingest.py --headless -H http://localhost:8080 -u 100 -r 100
```

It should start the load test and print out statistics on the command line.

## Reference

For more details on how to run locust and various configs, refer to
this [doc](https://docs.locust.io/en/stable/configuration.html)

To customize the user behavior by modifying the locustfiles, refer to
this [doc](https://docs.locust.io/en/stable/writing-a-locustfile.html)