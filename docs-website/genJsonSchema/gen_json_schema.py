import glob
import json
import os
import sys
from typing import Any, Dict, List


def get_base() -> Any:
    return {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "id": "https://json.schemastore.org/datahub-ingestion",
        "title": "Datahub Ingestion",
        "description": "Root schema of Datahub Ingestion",
        "definitions": {
            "console_sink": {
                "type": "object",
                "properties": {
                    "type": {
                        "type": "string",
                        "enum": ["console"],
                    }
                },
                "required": ["type"],
            },
            "file_sink": {
                "type": "object",
                "properties": {
                    "type": {"type": "string", "enum": ["file"]},
                    "config": {"$ref": "#/definitions/file_sink_config"},
                },
                "required": ["type", "config"],
            },
            "file_sink_config": {
                "type": "object",
                "properties": {
                    "filename": {
                        "description": "Path to file to write to.",
                        "type": "string",
                    }
                },
                "required": ["filename"],
                "additionalProperties": False,
            },
            "datahub_rest_sink": {
                "type": "object",
                "properties": {
                    "type": {"type": "string", "enum": ["datahub-rest"]},
                    "config": {"$ref": "#/definitions/datahub_rest_sink_config"},
                },
                "required": ["type", "config"],
                "additionalProperties": False,
            },
            "datahub_rest_sink_config": {
                "type": "object",
                "properties": {
                    "ca_certificate_path": {
                        "type": "string",
                        "description": "Path to CA certificate for HTTPS communications.",
                    },
                    "max_threads": {
                        "type": "number",
                        "description": "Experimental: Max parallelism for REST API calls",
                        "default": 1,
                    },
                    "retry_status_codes": {
                        "type": "array",
                        "items": {"type": "number"},
                        "description": "Retry HTTP request also on these status codes",
                        "default": [429, 502, 503, 504],
                    },
                    "server": {
                        "type": "string",
                        "description": "URL of DataHub GMS endpoint.",
                    },
                    "timeout_sec": {
                        "type": "number",
                        "description": "Per-HTTP request timeout.",
                        "default": 30,
                    },
                    "token": {
                        "type": "string",
                        "description": "Bearer token used for authentication.",
                    },
                    "extra_headers": {
                        "type": "string",
                        "description": "Extra headers which will be added to the request.",
                    },
                },
                "required": ["server"],
                "additionalProperties": False,
            },
            "datahub_kafka_sink": {
                "type": "object",
                "properties": {
                    "type": {"type": "string", "enum": ["datahub-kafka"]},
                    "config": {"$ref": "#/definitions/datahub_kafka_sink_config"},
                },
                "required": ["type", "config"],
                "additionalProperties": False,
            },
            "datahub_kafka_sink_config": {
                "type": "object",
                "properties": {
                    "connection": {
                        "type": "object",
                        "properties": {
                            "bootstrap": {
                                "type": "string",
                                "description": "Kafka bootstrap URL.",
                            },
                            "producer_config": {
                                "type": "object",
                                "description": "Passed to https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.SerializingProducer",
                            },
                            "schema_registry_url": {
                                "type": "string",
                                "description": "URL of schema registry being used.",
                            },
                            "schema_registry_config": {
                                "type": "object",
                                "description": "Passed to https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.schema_registry.SchemaRegistryClient",
                            },
                        },
                        "additionalProperties": False,
                        "required": ["bootstrap", "schema_registry_url"],
                    },
                    "topic_routes": {
                        "type": "object",
                        "properties": {
                            "MetadataChangeEvent": {
                                "type": "string",
                                "description": "Overridden Kafka topic name for the MetadataChangeEvent",
                                "default": "MetadataChangeEvent",
                            },
                            "MetadataChangeProposal": {
                                "type": "string",
                                "description": "Overridden Kafka topic name for the MetadataChangeProposal",
                                "default": "MetadataChangeProposal",
                            },
                        },
                        "additionalProperties": False,
                    },
                },
                "required": ["connection"],
                "additionalProperties": False,
            },
        },
        "type": "object",
        "properties": {
            "source": {"anyOf": []},
            "transformers": {
                "type": "array",
                "items": {
                    "type": "object",
                    "description": "Transformer configs see at https://datahubproject.io/docs/metadata-ingestion/transformers",
                    "properties": {
                        "type": {"type": "string", "description": "Transformer type"},
                        "config": {
                            "type": "object",
                            "description": "Transformer config",
                        },
                    },
                    "required": ["type"],
                    "additionalProperties": False,
                },
            },
            "sink": {
                "description": "sink",
                "anyOf": [
                    {"$ref": "#/definitions/datahub_kafka_sink"},
                    {"$ref": "#/definitions/datahub_rest_sink"},
                    {"$ref": "#/definitions/console_sink"},
                    {"$ref": "#/definitions/file_sink"},
                ],
            },
        },
        "required": ["source", "sink"],
    }


configs: Dict[str, Any] = {}
definitions = {}
refs: List[Dict] = []

if len(sys.argv) != 3:
    print(
        """\
Usage:
    gen_json_schema.py config_schema_dir output_file
"""
    )
    sys.exit(0)

config_schemas_dir: str = sys.argv[1]
output_file: str = sys.argv[2]

for jfile in glob.glob(f"{config_schemas_dir}/*"):
    config_name: str = os.path.splitext(os.path.basename(jfile))[0].split("_")[0]
    print(f"ConfigName: {config_name}")

    f = open(jfile)
    data = json.load(f)

    source_obj = {
        "type": "object",
        "properties": {
            "type": {"type": "string", "enum": [f"{config_name}"]},
            "config": {"$ref": f"#/definitions/{config_name}_config"},
        },
        "required": ["type", "config"],
    }
    configs[f"{config_name}"] = source_obj
    if "definitions" in data:
        definitions.update(data["definitions"])
        data.pop("definitions", None)

    configs[f"{config_name}_config"] = data
    ref = {"$ref": f"#/definitions/{config_name}"}
    refs.append(ref)

base = get_base()
base["definitions"].update(configs)
base["definitions"].update(definitions)

print(base["properties"]["source"])

base["properties"]["source"]["anyOf"] = base["properties"]["source"]["anyOf"] + refs
with open(f"{output_file}", "w") as outfile:
    json.dump(base, outfile, indent=4)
