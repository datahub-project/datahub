#!/bin/sh
base_dir=$1
set -e  # Exit immediately if a command exits with a non-zero status
echo "Starting data generation"

kafka_server="localhost:29092"
schema_reg="http://localhost:28081"

echo "Generating test data..."
# Generate test data - reduced count for faster processing
python ${base_dir}/generate_test_data.py --type user --count 20 > ${base_dir}/user_data.json
python ${base_dir}/generate_test_data.py --type numeric --count 20 > ${base_dir}/numeric_data.json

echo "Converting to JSONL..."
jq -c '.[]' ${base_dir}/user_data.json > ${base_dir}/user_records.jsonl
jq -c '.[]' ${base_dir}/numeric_data.json > ${base_dir}/numeric_records.jsonl

echo "Creating topics..."

# Create a Python script for batch sending
cat > ${base_dir}/batch_send.py << 'EOF'
import argparse
import json
from confluent_kafka.avro import AvroProducer
from confluent_kafka import avro

def send_batch_messages():
    parser = argparse.ArgumentParser()
    parser.add_argument('--topic', required=True)
    parser.add_argument('--bootstrap-servers', required=True)
    parser.add_argument('--schema-registry', required=True)
    parser.add_argument('--value-schema-file', required=True)
    parser.add_argument('--key-schema-file')
    parser.add_argument('--records-file', required=True)
    args = parser.parse_args()

    # Load schemas
    value_schema = avro.load(args.value_schema_file)
    key_schema = avro.load(args.key_schema_file) if args.key_schema_file else None

    # Configure producer
    config = {
        'bootstrap.servers': args.bootstrap_servers,
        'schema.registry.url': args.schema_registry
    }

    producer = AvroProducer(config, default_value_schema=value_schema, default_key_schema=key_schema)

    # Read and send records
    with open(args.records_file, 'r') as f:
        for i, line in enumerate(f):
            record = json.loads(line)
            if key_schema:
                key = {"id": 100 + i, "namespace": "test"}
                producer.produce(topic=args.topic, value=record, key=key)
            else:
                producer.produce(topic=args.topic, value=record)

            if i % 10 == 0:  # Flush every 10 messages
                producer.flush()

    producer.flush()  # Final flush

if __name__ == '__main__':
    send_batch_messages()
EOF

# Send records in batch mode
echo "Sending records to value_topic..."
python ${base_dir}/batch_send.py \
    --topic value_topic \
    --bootstrap-servers "${kafka_server}" \
    --schema-registry "${schema_reg}" \
    --value-schema-file "${base_dir}/value_schema.avsc" \
    --records-file "${base_dir}/user_records.jsonl"

echo "Sending records to key_value_topic..."
python ${base_dir}/batch_send.py \
    --topic key_value_topic \
    --bootstrap-servers "${kafka_server}" \
    --schema-registry "${schema_reg}" \
    --key-schema-file "${base_dir}/key_schema.avsc" \
    --value-schema-file "${base_dir}/value_schema.avsc" \
    --records-file "${base_dir}/user_records.jsonl"

echo "Sending records to numeric_topic..."
python ${base_dir}/batch_send.py \
    --topic numeric_topic \
    --bootstrap-servers "${kafka_server}" \
    --schema-registry "${schema_reg}" \
    --value-schema-file "${base_dir}/numeric_schema.avsc" \
    --records-file "${base_dir}/numeric_records.jsonl"

# Clean up
echo "Cleaning up temporary files..."
rm -f ${base_dir}/user_data.json ${base_dir}/numeric_data.json \
    ${base_dir}/user_records.jsonl ${base_dir}/numeric_records.jsonl \
    ${base_dir}/batch_send.py

echo "Data generation complete"