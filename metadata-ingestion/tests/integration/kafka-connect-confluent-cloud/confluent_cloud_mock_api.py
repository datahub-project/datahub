import base64
import logging
import os
from functools import wraps
from typing import Any, Dict

from flask import Flask, jsonify, request

app = Flask(__name__)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@app.before_request
def log_request_info():
    client_ip = request.environ.get("REMOTE_ADDR", "unknown")
    logger.info(f"Request - {request.method} {request.path} from {client_ip}")


VALID_AUTH = {
    "DEV_CONFLUENT_AUTH": "YWRtaW46YWRtaW4=",  # admin:admin in base64
    "test_user": "dGVzdDp0ZXN0",  # test:test in base64
}

CONNECTORS_DATA = {
    "source_postgres_cdc_01": {
        "status": None,
        "info": {
            "name": "source_postgres_cdc_01",
            "type": "source",
            "config": {
                "cloud.environment": "prod",
                "cloud.provider": "aws",
                "connector.class": "PostgresCdcSource",
                "database.dbname": "ecommerce_db",
                "database.hostname": "postgres-primary.us-east-1.rds.amazonaws.com",
                "database.password": "****************",
                "database.port": "5432",
                "database.server.name": "source_postgres_cdc_01",
                "database.sslmode": "require",
                "database.user": "kafka_connect_user",
                "decimal.handling.mode": "double",
                "heartbeat.action.query": "insert into public.heartbeat_table values(default, default, 'source_postgres_cdc_01')",
                "heartbeat.interval.ms": "600000",
                "kafka.auth.mode": "****************",
                "kafka.endpoint": "SASL_SSL://***.aws.confluent.cloud:9092",
                "kafka.region": "us-east-1",
                "kafka.service.account.id": "****************",
                "name": "source_postgres_cdc_01",
                "output.data.format": "AVRO",
                "output.key.format": "AVRO",
                "slot.name": "replication_slot_01",
                "snapshot.mode": "never",
                "table.include.list": "public.customer_addresses,analytics.order_events",
                "tasks.max": "1",
                "time.precision.mode": "connect",
                "transforms": "Transform ",
                "transforms.Transform.regex": "(.*)\\.(.*)\\.(.*)",
                "transforms.Transform.replacement": "$2.$3",
                "transforms.Transform.type": "io.confluent.connect.cloud.transforms.TopicRegexRouter",
            },
            "tasks": [{"connector": "source_postgres_cdc_01", "task": 0}],
        },
        "id": None,
        "extensions": {},
    },
    "sink_postgres_01": {
        "status": None,
        "info": {
            "name": "sink_postgres_01",
            "type": "sink",
            "config": {
                "Transform.Transform": "Transform",
                "Transform.Transform0 ": "Transform0 ",
                "cloud.environment": "prod",
                "cloud.provider": "aws",
                "connection.host": "postgres-replica.us-west-2.rds.amazonaws.com",
                "connection.password": "****************",
                "connection.port": "5432",
                "connection.user": "kafka_connect_sink",
                "connector.class": "PostgresSink",
                "db.name": "reporting_db",
                "delete.enabled": "true",
                "input.data.format": "AVRO",
                "input.key.format": "AVRO",
                "insert.mode": "UPSERT",
                "kafka.auth.mode": "****************",
                "kafka.endpoint": "SASL_SSL://***.aws.confluent.cloud:9092",
                "kafka.region": "us-east-1",
                "kafka.service.account.id": "****************",
                "name": "sink_postgres_01",
                "pk.mode": "record_key",
                "ssl.mode": "require",
                "tasks.max": "5",
                "topics": "public.customer_profiles,public.product_catalog",
                "transforms": "Transform,Transform0 ",
                "transforms.Transform.exclude": "__deleted",
                "transforms.Transform.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
                "transforms.Transform0.exclude": "audit_timestamp, audit_user, audit_txn_id, version_range",
                "transforms.Transform0.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
            },
            "tasks": [
                {"connector": "sink_postgres_01", "task": 0},
                {"connector": "sink_postgres_01", "task": 1},
                {"connector": "sink_postgres_01", "task": 2},
                {"connector": "sink_postgres_01", "task": 3},
                {"connector": "sink_postgres_01", "task": 4},
            ],
        },
        "id": None,
        "extensions": {},
    },
    "sink_snowflake_01": {
        "status": None,
        "info": {
            "name": "sink_snowflake_01",
            "type": "sink",
            "config": {
                "cloud.environment": "prod",
                "cloud.provider": "aws",
                "connector.class": "SnowflakeSink",
                "input.data.format": "AVRO",
                "input.key.format": "AVRO",
                "kafka.auth.mode": "****************",
                "kafka.endpoint": "SASL_SSL://***.aws.confluent.cloud:9092",
                "kafka.region": "us-east-1",
                "kafka.service.account.id": "****************",
                "name": "sink_snowflake_01",
                "snowflake.database.name": "ANALYTICS_DB",
                "snowflake.private.key": "****************",
                "snowflake.private.key.passphrase": "",
                "snowflake.schema.name": "public",
                "snowflake.topic2table.map": "analytics.transactions:analytics_transactions, analytics.order_events:analytics_order_events",
                "snowflake.url.name": "https://account123.us-east-1.snowflakecomputing.com",
                "snowflake.user.name": "kafka_connect_user",
                "tasks.max": "5",
                "topics": "analytics.transactions, analytics.order_events",
            },
            "tasks": [
                {"connector": "sink_snowflake_01", "task": 0},
                {"connector": "sink_snowflake_01", "task": 1},
                {"connector": "sink_snowflake_01", "task": 2},
                {"connector": "sink_snowflake_01", "task": 3},
                {"connector": "sink_snowflake_01", "task": 4},
            ],
        },
        "id": None,
        "extensions": {},
    },
    "outbox-source-connector": {
        "status": None,
        "info": {
            "name": "outbox-source-connector",
            "type": "source",
            "config": {
                "after.state.only": "false",
                "cloud.environment": "prod",
                "cloud.provider": "aws",
                "connector.class": "MySqlCdcSource",
                "database.connectionTimeZone": "UTC",
                "database.dbname": "outbox_db",
                "database.hostname": "mysql-primary.us-west-2.rds.amazonaws.com",
                "database.password": "****************",
                "database.port": "3306",
                "database.server.name": "outbox-db-server",
                "database.ssl.mode": "required",
                "database.sslmode": "require",
                "database.user": "outbox_connector",
                "hstore.handling.mode": "json",
                "interval.handling.mode": "numeric",
                "kafka.auth.mode": "SERVICE_ACCOUNT",
                "kafka.endpoint": "SASL_SSL://pkc-abc123.us-west-2.aws.confluent.cloud:9092",
                "kafka.region": "us-west-2",
                "kafka.service.account.id": "sa-123456",
                "max.batch.size": "2000",
                "name": "outbox-source-connector",
                "output.data.format": "JSON",
                "output.key.format": "JSON",
                "poll.interval.ms": "500",
                "provide.transaction.metadata": "true",
                "snapshot.locking.mode": "none",
                "snapshot.mode": "schema_only",
                "table.include.list": "outbox_db.outbox",
                "tasks.max": "1",
                "tombstones.on.delete": "false",
                "transforms": "EventRouter,RegexRouter",
                "transforms.EventRouter.route.by.field": "_route_suffix",
                "transforms.EventRouter.table.expand.json.payload": "true",
                "transforms.EventRouter.table.field.event.id": "event_id",
                "transforms.EventRouter.table.field.event.key": "aggregate_id",
                "transforms.EventRouter.table.field.event.payload": "payload",
                "transforms.EventRouter.table.field.event.type": "event_type",
                "transforms.EventRouter.table.fields.additional.placement": "event_timestamp:header,event_version:header,aggregate_type:header",
                "transforms.EventRouter.type": "io.debezium.transforms.outbox.EventRouter",
                "transforms.RegexRouter.regex": "outbox.event.(.*)",
                "transforms.RegexRouter.replacement": "app.events.$1",
                "transforms.RegexRouter.type": "io.confluent.connect.cloud.transforms.TopicRegexRouter",
            },
            "tasks": [{"connector": "outbox-source-connector", "task": 0}],
        },
        "id": None,
        "extensions": {},
    },
}

TOPICS_DATA: Dict[str, Any] = {
    "kind": "KafkaTopicList",
    "data": [
        {
            "topic_name": "public.customer_profiles",
            "partitions": {
                "related": "https://api.confluent.cloud/kafka/v3/clusters/4k0R9d1GTS5tI9f4Y2xZ0Q/topics/public.customer_profiles/partitions"
            },
            "configs": {
                "related": "https://api.confluent.cloud/kafka/v3/clusters/4k0R9d1GTS5tI9f4Y2xZ0Q/topics/public.customer_profiles/configs"
            },
            "replication_factor": 3,
            "is_internal": False,
            "cluster_id": "4k0R9d1GTS5tI9f4Y2xZ0Q",
        },
        {
            "topic_name": "public.product_catalog",
            "partitions": {
                "related": "https://api.confluent.cloud/kafka/v3/clusters/4k0R9d1GTS5tI9f4Y2xZ0Q/topics/public.product_catalog/partitions"
            },
            "configs": {
                "related": "https://api.confluent.cloud/kafka/v3/clusters/4k0R9d1GTS5tI9f4Y2xZ0Q/topics/public.product_catalog/configs"
            },
            "replication_factor": 3,
            "is_internal": False,
            "cluster_id": "4k0R9d1GTS5tI9f4Y2xZ0Q",
        },
        {
            "topic_name": "analytics.transactions",
            "partitions": {
                "related": "https://api.confluent.cloud/kafka/v3/clusters/4k0R9d1GTS5tI9f4Y2xZ0Q/topics/analytics.transactions/partitions"
            },
            "configs": {
                "related": "https://api.confluent.cloud/kafka/v3/clusters/4k0R9d1GTS5tI9f4Y2xZ0Q/topics/analytics.transactions/configs"
            },
            "replication_factor": 3,
            "is_internal": False,
            "cluster_id": "4k0R9d1GTS5tI9f4Y2xZ0Q",
        },
        {
            "topic_name": "analytics.order_events",
            "partitions": {
                "related": "https://api.confluent.cloud/kafka/v3/clusters/4k0R9d1GTS5tI9f4Y2xZ0Q/topics/analytics.order_events/partitions"
            },
            "configs": {
                "related": "https://api.confluent.cloud/kafka/v3/clusters/4k0R9d1GTS5tI9f4Y2xZ0Q/topics/analytics.order_events/configs"
            },
            "replication_factor": 3,
            "is_internal": False,
            "cluster_id": "4k0R9d1GTS5tI9f4Y2xZ0Q",
        },
        {
            "topic_name": "app.events.order_created",
            "partitions": {
                "related": "https://api.confluent.cloud/kafka/v3/clusters/4k0R9d1GTS5tI9f4Y2xZ0Q/topics/app.events.order_created/partitions"
            },
            "configs": {
                "related": "https://api.confluent.cloud/kafka/v3/clusters/4k0R9d1GTS5tI9f4Y2xZ0Q/topics/app.events.order_created/configs"
            },
            "replication_factor": 3,
            "is_internal": False,
            "cluster_id": "4k0R9d1GTS5tI9f4Y2xZ0Q",
        },
        {
            "topic_name": "app.events.order_updated",
            "partitions": {
                "related": "https://api.confluent.cloud/kafka/v3/clusters/4k0R9d1GTS5tI9f4Y2xZ0Q/topics/app.events.order_updated/partitions"
            },
            "configs": {
                "related": "https://api.confluent.cloud/kafka/v3/clusters/4k0R9d1GTS5tI9f4Y2xZ0Q/topics/app.events.order_updated/configs"
            },
            "replication_factor": 3,
            "is_internal": False,
            "cluster_id": "4k0R9d1GTS5tI9f4Y2xZ0Q",
        },
    ],
}


def require_auth(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        client_ip = request.environ.get("REMOTE_ADDR", "unknown")
        endpoint = request.endpoint
        method = request.method

        auth_header = request.headers.get("Authorization")
        if not auth_header:
            logger.warning(
                f"Authentication failed - No Authorization header from {client_ip} for {method} {endpoint}"
            )
            return jsonify({"error": "Authorization header required"}), 401

        if not auth_header.startswith("Basic "):
            logger.warning(
                f"Authentication failed - Invalid auth type from {client_ip} for {method} {endpoint}"
            )
            return jsonify({"error": "Basic authentication required"}), 401

        auth_token = auth_header.split(" ")[1]

        if auth_token not in VALID_AUTH.values():
            try:
                decoded_token = base64.b64decode(auth_token).decode("utf-8")
                logger.warning(
                    f"Authentication failed - Invalid credentials '{decoded_token}' from {client_ip} for {method} {endpoint}"
                )
            except Exception:
                logger.warning(
                    f"Authentication failed - Invalid/malformed token from {client_ip} for {method} {endpoint}"
                )
            return jsonify({"error": "Invalid authentication credentials"}), 401

        try:
            decoded_token = base64.b64decode(auth_token).decode("utf-8")
            logger.info(
                f"Authentication successful - User '{decoded_token}' from {client_ip} for {method} {endpoint}"
            )
        except Exception:
            logger.info(
                f"Authentication successful - Valid token from {client_ip} for {method} {endpoint}"
            )

        return f(*args, **kwargs)

    return decorated_function


@app.route(
    "/connect/v1/environments/<environment_id>/clusters/<cluster_id>/connectors",
    methods=["GET"],
)
@require_auth
def get_connectors(environment_id, cluster_id):
    """Get list of connectors with optional expansion"""
    expand = request.args.get("expand", "")

    if expand == "info":
        return jsonify(CONNECTORS_DATA)
    else:
        return jsonify(list(CONNECTORS_DATA.keys()))


@app.route(
    "/connect/v1/environments/<environment_id>/clusters/<cluster_id>/connectors/<connector_name>",
    methods=["GET"],
)
@require_auth
def get_connector(environment_id, cluster_id, connector_name):
    """Get specific connector details"""
    if connector_name not in CONNECTORS_DATA:
        return jsonify({"error": "Connector not found"}), 404

    return jsonify(CONNECTORS_DATA[connector_name])


@app.route(
    "/connect/v1/environments/<environment_id>/clusters/<cluster_id>/connectors/<connector_name>/status",
    methods=["GET"],
)
@require_auth
def get_connector_status(environment_id, cluster_id, connector_name):
    """Get connector status"""
    if connector_name not in CONNECTORS_DATA:
        return jsonify({"error": "Connector not found"}), 404

    connector_data = CONNECTORS_DATA[connector_name]
    connector_info = connector_data["info"]  # type: ignore[index]
    connector_type = connector_info["type"]  # type: ignore[index]

    status_response = {
        "name": connector_name,
        "connector": {"state": "RUNNING", "worker_id": "connect-worker-1"},
        "tasks": [{"id": 0, "state": "RUNNING", "worker_id": "connect-worker-1"}],
        "type": connector_type,
    }

    return jsonify(status_response)


@app.route("/kafka/v3/clusters/<cluster_id>/topics", methods=["GET"])
@require_auth
def get_topics(cluster_id):
    """Get list of topics"""
    return jsonify(TOPICS_DATA)


@app.route("/kafka/v3/clusters/<cluster_id>/topics/<topic_name>", methods=["GET"])
@require_auth
def get_topic(cluster_id, topic_name):
    """Get specific topic details"""
    for topic in TOPICS_DATA["data"]:
        if topic["topic_name"] == topic_name:
            return jsonify(topic)

    return jsonify({"error": "Topic not found"}), 404


@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "service": "confluent-cloud-mock-api"})


@app.errorhandler(404)
def not_found(error):
    client_ip = request.environ.get("REMOTE_ADDR", "unknown")
    logger.warning(f"404 Not Found - {request.method} {request.path} from {client_ip}")
    return jsonify({"error": "Endpoint not found"}), 404


@app.errorhandler(500)
def internal_error(error):
    client_ip = request.environ.get("REMOTE_ADDR", "unknown")
    logger.error(
        f"500 Internal Server Error - {request.method} {request.path} from {client_ip}: {str(error)}"
    )
    return jsonify({"error": "Internal server error"}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8888))

    print("Starting Confluent Cloud Mock API Server...")
    print("Available endpoints:")
    print(
        "  GET /connect/v1/environments/{env}/clusters/{cluster}/connectors?expand=info"
    )
    print("  GET /connect/v1/environments/{env}/clusters/{cluster}/connectors/{name}")
    print(
        "  GET /connect/v1/environments/{env}/clusters/{cluster}/connectors/{name}/status"
    )
    print("  GET /kafka/v3/clusters/{cluster}/topics")
    print("  GET /kafka/v3/clusters/{cluster}/topics/{topic}")
    print("  GET /health")
    print(f"\nServer will be available at: http://localhost:{port}")
    print("\nAuthentication:")
    print("  Use Basic auth with any of these credentials:")
    for name, token in VALID_AUTH.items():
        decoded = base64.b64decode(token).decode("utf-8")
        print(f"    {name}: {decoded}")

    app.run(host="0.0.0.0", port=port, debug=False)
