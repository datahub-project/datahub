# Quickstart
To start all Docker containers at once, please run below command:
```
cd docker/quickstart && docker-compose up
```
After `elasticsearch` container is initialized, run below to create the search indices:
```
cd docker/elasticsearch && bash init.sh
```