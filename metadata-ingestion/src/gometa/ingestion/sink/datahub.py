from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, TypeVar, Type
from pydantic import BaseModel, Field, ValidationError, validator
from enum import Enum
from pathlib import Path
import requests
from gometa.ingestion.api.sink import Sink
from gometa.ingestion.sink.kafka import KafkaSinkConfig, KafkaSink
from gometa.metadata.model import *

import logging
logger = logging.getLogger(__name__)

class TransportEnum(str, Enum):
    kafka = 'kafka'
    rest    = 'rest'

class DataHubHttpSinkConfig(BaseModel):
    """Configuration class for holding connectivity to datahub gms"""
    
    server: Optional[str] = "localhost:8080"


import urllib, json
class DataHubHttpSink(Sink):

    def configure(self, config_dict:dict):
        self.config = DataHubHttpSinkConfig.parse_obj(config_dict)
        # TODO verify that config points to a valid server
        #response = requests.get(f"http://{self.config.server}/")
        #assert response.status_code == 200
        return self

    resource_locator = {
        SnapshotType.dataset: "datasets"
    }
    headers = {'X-RestLi-Protocol-Version' : '2.0.0'}

    def get_ingest_endpoint(self, snapshot_type: SnapshotType):
        snapshot_resource = self.resource_locator.get(snapshot_type, None)
        if not snapshot_resource:
            raise ValueError("Failed to locate a snapshot resource for {snapshot_type}")

        return f'http://{self.config.server}/{snapshot_resource}?action=ingest'

    def write_record_async(self, record_envelope, write_callback):
        record : MetadataChangeEvent = record_envelope.record
        snapshot_type, serialized_snapshot = record.as_mce(SerializationEnum.restli)
        url = self.get_ingest_endpoint(snapshot_type)
        logger.info(f'url={url}')
        logger.info(f'post={serialized_snapshot}')
        try:
            response = requests.post(url, headers = self.headers, json=serialized_snapshot)
            #data = response.request.body
            #encoded_body = urllib.urlencode(b)
            with open('data.json', 'w') as outfile:
                json.dump(serialized_snapshot, outfile)
            
            response.raise_for_status()
            write_callback.on_success(record_envelope, {})
        except Exception as e:
            write_callback.on_failure(record_envelope, e, {})

    def close(self):
        pass

class DataHubSinkConfig(BaseModel):
    """Configuration class for holding datahub transport configuration"""

    # transport protocol
    transport: Optional[TransportEnum] = TransportEnum.kafka

    # kafka sink configuration
    kafka: Optional[KafkaSinkConfig] = KafkaSinkConfig()

    # rest connection configuration
    rest:  Optional[DataHubHttpSinkConfig] = DataHubHttpSinkConfig()

class DataHubSink(Sink):
    """A Sink for writing metadata to DataHub"""
 #  kakfa_producer_conf = get_kafka_producer_conf(self.config.kafka_sink)
 #   record_schema = avro.load(AVROLOADPATH)
 #   self.kafka_producer = AvroProducer(kafka_producer_conf, default_value_schema=record_schema)
    
 # def get_kafka_producer_conf(kafka_sink_conf: KafkaSinkConfig):
 #   conf = {'bootstrap.servers': kafka_sink_conf.connection.bootstrap,
 #                   'schema.registry.url': kafka_sink_conf.connection.schema_registry_url}
 #   return conf
    def __init__(self):
        self.configured = False


    def configure(self, config_dict:dict):
        logger.info(f"sink configured with {config_dict}")
        self.config = DataHubSinkConfig.parse_obj(config_dict)
        if self.config.transport == TransportEnum.kafka:
            self.sink = KafkaSink().configure(self.config.kafka)
        elif self.config.transport == TransportEnum.rest:
            self.sink = DataHubHttpSink().configure(self.config.rest)

        self.configured = True
        
    def write_record_async(self, record_envelope, write_callback):
        assert self.configured
        try:
            self.sink.write_record_async(record_envelope, write_callback)
            #sys.stdout.write('\n%s has been successfully produced!\n' % mce)
        except ValueError as e:
            logger.exception('Message serialization failed %s' % e)

    def close(self):
        self.sink.close()
