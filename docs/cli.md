# DataHub CLI Reference

DataHub provides a command line utility for managing it out of the box. The CLI provides capabilities like

- Deploying DataHub Locally (via Docker) 
- Running metadata ingestion
- Viewing Ingestion Runs
- Reverting an Ingestion Run
- Fetching an Entity
- Fetching an Entity Aspect
- Fetching a list of Entities
- Fetching a list of Entity Aspects
- Soft Deleting an Entity
- Hard Deleting an Entity

with more coming soon!
  
# Initializing the CLI

The first part of using the CLI involves initializing it to locate your DataHub deployment.

By default, the CLI will assume the following:

- DataHub API (GMS) is located at `locahost:8081`
- DataHub Kafka Broker is located at `localhost:9001`

If this is not the case, as is usually the case for production installations, you can configure the DataHub CLI to point to a remote deployment:

```aidl
datahub init
```

Follow the prompts to configure the location of the DataHub API (GMS) and your Kafka Broker. This will save a configuration file to `~/.datahubenv` 
with your settings. If you ever need to change this, simply run `datahub init` again. 


# Deploying DataHub Locally (via Docker) 

# Running metadata ingestion

# Viewing Ingestion Runs 

# Reverting an Ingestion Run

# Fetching an Entity

# Fetching an Entity Aspect

# Fetching a list of Entities

# Fetching a list of Entity Aspects

# Soft Deleting an Entity

# Hard Deleting an Entity

# FAQ