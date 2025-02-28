# How to Extract Logs from DataHub Containers

DataHub containers, datahub GMS (backend server) and datahub frontend (UI server), write log files to the local container filesystem. To extract these logs, you'll need to get them from inside the container where the services are running.

You can do so easily using the Docker CLI if you're deploying with vanilla docker or compose, and kubectl if you're on K8s. 

## Step 1: Find the id of the container you're interested in

You'll first need to get the id of the container that you'd like to extract logs for. For example, datahub-gms.

### Docker & Docker Compose

To do so, you can view all containers that Docker knows about by running the following command:

```
johnjoyce@Johns-MBP datahub-fork % docker container ls
CONTAINER ID   IMAGE                                   COMMAND                  CREATED      STATUS                  PORTS                                                      NAMES
6c4a280bc457   acryldata/datahub-frontend-react   "datahub-frontend/bi…"   5 days ago   Up 46 hours (healthy)   0.0.0.0:9002->9002/tcp                                     datahub-frontend-react
122a2488ab63   acryldata/datahub-gms              "/bin/sh -c /datahub…"   5 days ago   Up 5 days (healthy)     0.0.0.0:8080->8080/tcp                                     datahub-gms
7682dcc64afa   confluentinc/cp-schema-registry:5.4.0   "/etc/confluent/dock…"   5 days ago   Up 5 days               0.0.0.0:8081->8081/tcp                                     schema-registry
3680fcaef3ed   confluentinc/cp-kafka:5.4.0             "/etc/confluent/dock…"   5 days ago   Up 5 days               0.0.0.0:9092->9092/tcp, 0.0.0.0:29092->29092/tcp           broker
9d6730ddd4c4   neo4j:4.0.6                             "/sbin/tini -g -- /d…"   5 days ago   Up 5 days               0.0.0.0:7474->7474/tcp, 7473/tcp, 0.0.0.0:7687->7687/tcp   neo4j
c97edec663af   confluentinc/cp-zookeeper:5.4.0         "/etc/confluent/dock…"   5 days ago   Up 5 days               2888/tcp, 0.0.0.0:2181->2181/tcp, 3888/tcp                 zookeeper
150ba161cf26   mysql:8.2                               "docker-entrypoint.s…"   5 days ago   Up 5 days               0.0.0.0:3306->3306/tcp, 33060/tcp                          mysql
4b72a3eab73f   elasticsearch:7.9.3                     "/tini -- /usr/local…"   5 days ago   Up 5 days (healthy)     0.0.0.0:9200->9200/tcp, 9300/tcp                           elasticsearch
```

In this case, the container id we'd like to note is `122a2488ab63`, which corresponds to the `datahub-gms` service.

### Kubernetes & Helm

Find the name of the pod you're interested in using the following command:

```
kubectl get pods

...
default   datahub-frontend-1231ead-6767                        1/1     Running     0          42h
default   datahub-gms-c578b47cd-7676                              1/1     Running     0          13d
...
```

In this case the pod name we'd like to note is `datahub-gms-c578b47cd-7676` , which contains the GMS backend service.

## Step 2: Find the log files

The second step is to view all log files. Log files will live inside the container under the following directories for each service:

- **datahub-gms:** `/tmp/datahub/logs/gms`
- **datahub-frontend**: `/tmp/datahub/logs/datahub-frontend`

There are 2 types of logs that are collected:

1. **Info Logs**: These include info, warn, error log lines. They are what print to stdout when the container runs.
2. **Debug Logs**: These files have shorter retention (past 1 day) but include more granular debug information from the DataHub code specifically. We ignore debug logs from external libraries that DataHub depends on.

### Docker & Docker Compose

Since log files are named based on the current date, you'll need to use "ls" to see which files currently exist. To do so, you can use the `docker exec` command, using the container id recorded in step one:

```
docker exec --privileged <container-id> <shell-command> 
```

For example:

```
johnjoyce@Johns-MBP datahub-fork % docker exec --privileged 122a2488ab63 ls -la /tmp/datahub/logs/gms 
total 4664
drwxr-xr-x    2 datahub  datahub       4096 Jul 28 05:14 .
drwxr-xr-x    3 datahub  datahub       4096 Jul 23 08:37 ..
-rw-r--r--    1 datahub  datahub    2001112 Jul 23 23:33 gms.2021-23-07-0.log
-rw-r--r--    1 datahub  datahub      74343 Jul 24 20:29 gms.2021-24-07-0.log
-rw-r--r--    1 datahub  datahub      70252 Jul 25 17:56 gms.2021-25-07-0.log
-rw-r--r--    1 datahub  datahub     626985 Jul 26 23:36 gms.2021-26-07-0.log
-rw-r--r--    1 datahub  datahub     712270 Jul 27 23:59 gms.2021-27-07-0.log
-rw-r--r--    1 datahub  datahub     867707 Jul 27 23:59 gms.debug.2021-27-07-0.log
-rw-r--r--    1 datahub  datahub       3563 Jul 28 05:26 gms.debug.log
-rw-r--r--    1 datahub  datahub     382443 Jul 28 16:16 gms.log
```

Depending on your issue, you may be interested to view both debug and normal info logs.

### Kubernetes & Helm

Since log files are named based on the current date, you'll need to use "ls" to see which files currently exist. To do so, you can use the `kubectl exec` command, using the pod name recorded in step one:

```
kubectl exec datahub-gms-c578b47cd-7676 -n default -- ls -la /tmp/datahub/logs/gms

total 36388
drwxr-xr-x    2 datahub  datahub       4096 Jul 29 07:45 .
drwxr-xr-x    3 datahub  datahub         17 Jul 15 08:47 ..
-rw-r--r--    1 datahub  datahub     104548 Jul 15 22:24 gms.2021-15-07-0.log
-rw-r--r--    1 datahub  datahub      12684 Jul 16 14:55 gms.2021-16-07-0.log
-rw-r--r--    1 datahub  datahub    2482571 Jul 17 14:40 gms.2021-17-07-0.log
-rw-r--r--    1 datahub  datahub      49120 Jul 18 14:31 gms.2021-18-07-0.log
-rw-r--r--    1 datahub  datahub      14167 Jul 19 23:47 gms.2021-19-07-0.log
-rw-r--r--    1 datahub  datahub      13255 Jul 20 22:22 gms.2021-20-07-0.log
-rw-r--r--    1 datahub  datahub     668485 Jul 21 19:52 gms.2021-21-07-0.log
-rw-r--r--    1 datahub  datahub    1448589 Jul 22 20:18 gms.2021-22-07-0.log
-rw-r--r--    1 datahub  datahub      44187 Jul 23 13:51 gms.2021-23-07-0.log
-rw-r--r--    1 datahub  datahub      14173 Jul 24 22:59 gms.2021-24-07-0.log
-rw-r--r--    1 datahub  datahub      13263 Jul 25 21:11 gms.2021-25-07-0.log
-rw-r--r--    1 datahub  datahub      13261 Jul 26 19:02 gms.2021-26-07-0.log
-rw-r--r--    1 datahub  datahub    1118105 Jul 27 21:10 gms.2021-27-07-0.log
-rw-r--r--    1 datahub  datahub     678423 Jul 28 23:57 gms.2021-28-07-0.log
-rw-r--r--    1 datahub  datahub    1776274 Jul 28 07:19 gms.debug.2021-28-07-0.log
-rw-r--r--    1 datahub  datahub   27576533 Jul 29 09:55 gms.debug.log
-rw-r--r--    1 datahub  datahub    1195940 Jul 29 14:54 gms.log
```

In the next step, we'll save specific log files to our local filesystem.

## Step 3: Save Container Log File to Local

This step involves saving a copy of the container log files to your local filesystem for further investigation.

### Docker & Docker Compose

Simply use the `docker exec` command to "cat" the log file(s) of interest and route them to a new file.

```
docker exec --privileged 122a2488ab63 cat /tmp/datahub/logs/gms/gms.debug.log > my-local-log-file.log
```

Now you should be able to view the logs locally.

### Kubernetes & Helm

There are a few ways to get files out of the pod and into a local file. You can either use `kubectl cp` or simply `cat` and pipe the file of interest. We'll show an example using the latter approach:

```
kubectl exec datahub-gms-c578b47cd-7676 -n default -- cat /tmp/datahub/logs/gms/gms.log > my-local-gms.log
```