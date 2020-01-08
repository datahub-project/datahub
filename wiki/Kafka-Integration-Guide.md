1. <a href="#introduction">Introduction</a>
2. <a href="#kafka">Kafka Client</a>
3. <a href="#listen">Listen to Kafka Messages</a>
3. <a href="#config">Add Kafka Topics to Consume</a>

<a name="introduction">

# Introduction

Apache Kafka is a distributed publish-subscribe messaging system. It can be used for metrics, tracking data, and intra-application queuing. WhereHows uses Kafka clients to listen and track the dataset ETL, schema and ownership related messages. The received Kafka messages give us near instant information about the events, updates and changes. This is different from the ETL jobs based information ingestion. ETL jobs are doing batch processing and are processed at pre-defined time of day. Kafka processing is done as soon as the Kafka client get one message.

There are some kafka processors for Gobblin emitted dataset ETL related events. See the detail in [Gobblin Kafka Integration](Gobblin-Kafka-Integration.md). We plan to add more listened Kafka topics and corresponding processors to WhereHows to track dataset schema changes and other useful information.

A developer can also use this guide to integrate Kafka and write customized processors as needed. 

<a name="kafka">

# Kafka Clients

Before starting using Kafka, the introduction and documentation of Apache Kafka can be found here: http://kafka.apache.org/documentation.html#introduction

We imported Confluent Kafka Client into WhereHows but also extended the supported SchemaId type. 
(See [SchemaId.java](../wherehows-common/src/main/java/wherehows/common/kafka/SchemaId.java)) 
Based on the Kafka system setting, developer can choose the matching SchemaId type in 
[AbstractKafkaAvroSerDe.java](../wherehows-common/src/main/java/wherehows/common/kafka/serializers/AbstractKafkaAvroSerDe.java)

<a name="listen">

# Listen to Kafka Messages
 
Major Related Code: [KafkaConsumerMaster](../wherehows-backend/app/actors/KafkaConsumerMaster.java) [KafkaConsumerWorker](../wherehows-backend/app/actors/KafkaConsumerMaster.java)

At startup, the KafkaConsumerMaster will initiate and check the field "kafka.consumer.etl.jobid=[]" in backend application.conf and then get the configuration of from database much like an ETL job. Then for each topic it will contact zookeeper to get the connection and start a KafkaConsumerWorker Akka actor to listen to the connection. Whenever a message is received by the worker, a topic-specific processor is invoked to process the message and writes the result to database or feed to other APIs.

<a name="config">

# Add Kafka Topics to Consume

We are making progress in integrating more Kafka topics and processors into WhereHows. But each different use case may have different message format and require different processor. Developer can easily set up Kafka configurations and add new Kafka topics to consume. 

*  Prepare processor and database table

Each topic needs a processor to handle the received messages, and the final results and information and often write to database immediately. The abstract [KafkaConsumerProcessor.java](../wherehows-etl/src/main/java/metadata/etl/kafka/KafkaConsumerProcessor.java) can be a guideline to write specific processor.

* Config Kafka client

Kafka client needs several configurations to function properly. The following is a set of properties for a Kafka consumer group.

| configuration key | description|
|---|---|
| group.id | kafka consumer group id |
| schemaRegistryUrl | the schema register url for getting schema to deserialize messages |
| zookeeper.connect | zookeeper url of the kafka system |
| zookeeper.session.timeout.ms | zookeeper timeout interval in milliseconds |
| zookeeper.sync.time.ms | zookeeper synchronize period in milliseconds |
| auto.commit.interval.ms | zookeeper auto commit interval in milliseconds |
| kafka.topics | kafka topics to consume |
| kafka.processors | the processor class for each kafka topics |
| kafka.db.tables | the tables to save the kafka message processed results |

Note that each Kafka consumer group can have several topics, so the last three properties "kafka.topics", "kafka.processors", "kafka.db.tables" are comma separated strings, and they must have equal number of elements. The first topic will use the first processor and the first db table, and so on. 

* Write results to staging table

The processed kafka messages generate java records after retrieving the information from the messages. The format and content of the corresponding records can be found in [wherehows-common schemas](../wherehows-common/src/main/java/wherehows/common/schemas). The records are then stored in corresponding staging tables for later analysis or merge with final tables. The staging table DDL is in [kafka_tracking.sql](../wherehows-data-model/DDL/ETL_DDL/kafka_tracking.sql). 


