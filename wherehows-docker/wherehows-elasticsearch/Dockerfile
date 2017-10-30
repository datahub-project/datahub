FROM openjdk:8

RUN apt-get update && \
    apt-get install -y wget curl

# Variables
ENV ES_PKG_NAME elasticsearch-5.5.1
ENV ELASTICSEARCH_SERVER_URL "http://localhost:9200"
ENV PATH /usr/share/elasticsearch/bin:$PATH
ENV ES_JAVA_OPTS "-Xms1g -Xmx1g"

# Elasticsearch Install
RUN cd / && \
    wget https://artifacts.elastic.co/downloads/elasticsearch/$ES_PKG_NAME.tar.gz && \
    tar xvzf $ES_PKG_NAME.tar.gz && \
    rm -f $ES_PKG_NAME.tar.gz && \
    mv /$ES_PKG_NAME /usr/share/elasticsearch && \
    mkdir -p /usr/share/elasticsearch/data && \
    mkdir -p /usr/share/elasticsearch/logs

# Linux memory
RUN echo "elasticsearch soft memlock unlimited" >> /etc/security/limits.conf && \
    echo "elasticsearch hard memlock unlimited" >> /etc/security/limits.conf
CMD sysctl -w vm.max_map_count=262144

# Elasticsearch Index schema & config
ADD index_mapping.json /tmp
ADD init.sh /docker-entrypoint.d
ADD elasticsearch.yml /usr/share/elasticsearch/config
RUN chmod 755 /docker-entrypoint.d

# Elasticsearch group & user
RUN groupadd elasticsearch && \
    useradd -g elasticsearch elasticsearch && \
    chown -R elasticsearch:elasticsearch /usr/share/elasticsearch

WORKDIR /usr/share/elasticsearch
VOLUME /usr/share/elasticsearch/data

# Expose ports.
#   - 9200: HTTP
#   - 9300: transport
EXPOSE 9200 9300

USER elasticsearch
CMD elasticsearch
