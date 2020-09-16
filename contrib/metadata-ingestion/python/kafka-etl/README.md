# Kafka ETL

## Ingest metadata from Kafka to DataHub
The kafka_etl provides you ETL channel to communicate with your kafka.
```
➜  Config your kafka environmental variable in the file.
    ZOOKEEPER      # Your zookeeper host.
    
➜  Config your Kafka broker environmental variable in the file.
    AVROLOADPATH   # Your model event in avro format.
    KAFKATOPIC     # Your event topic.
    BOOTSTRAP      # Kafka bootstrap server.
    SCHEMAREGISTRY # Kafka schema registry host.

➜  python kafka_etl.py
```
This will bootstrap DataHub with your metadata in the kafka as a dataset entity.
