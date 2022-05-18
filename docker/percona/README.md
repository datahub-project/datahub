# To build and push
```
docker build -t percona:1.2 .
docker tag testx:1.0 dockdevx/percona:1.2
docker push dockdevx/percona:1.2
```