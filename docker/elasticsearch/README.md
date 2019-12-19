# Elasticsearch & Kibana

DataHub uses Elasticsearch as a search engine. Elasticsearch powers search, typeahead and browse functions for DataHub.
[Official Elasticsearch Docker image](https://hub.docker.com/_/elasticsearch) found in Docker Hub is used without 
any modification.

## Run Docker container
Below command will start the Elasticsearch and Kibana containers. `DataHub` uses Elasticsearch release `5.6.8`. Newer
versions of Elasticsearch are not tested and you might experience compatibility issues.
```
cd docker/elasticsearch && docker-compose pull && docker-compose up --build
```
You can connect to Kibana on your web browser to monitor Elasticsearch via below link:
```
http://localhost:5601
```

## Container configuration
### External Port
If you need to configure default configurations for your container such as the exposed port, you will do that in
`docker-compose.yml` file. Refer to this [link](https://docs.docker.com/compose/compose-file/#ports) to understand
how to change your exposed port settings.
```
ports:
  - "9200:9200"
```

### Docker Network
All Docker containers for DataHub are supposed to be on the same Docker network which is `datahub_network`. 
If you change this, you will need to change this for all other Docker containers as well.
```
networks:
  default:
    name: datahub_network
```