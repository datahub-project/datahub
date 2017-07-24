FROM docker.elastic.co/elasticsearch/elasticsearch:5.5.0

ENV ES_JAVA_OPTS "-Xms1g -Xmx1g"
ENV ELASTICSEARCH_SERVER_URL "http://localhost:9200"

ADD index_mapping.json /tmp

COPY init.sh /docker-entrypoint.sh