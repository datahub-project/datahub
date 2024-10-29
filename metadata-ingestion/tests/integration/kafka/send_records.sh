#!/bin/sh
base_dir=$1

#echo $base_dir
kafka_server="localhost:29092"
schema_reg="http://localhost:28081"
# create a topic with value schemas but no keys
python ${base_dir}/create_key_value_topic.py --topic value_topic --bootstrap-servers "${kafka_server}" --schema-registry "${schema_reg}" --value-schema-file ${base_dir}/value_schema.avsc --record-value '{"email": "foo@foo.com", "firstName": "Jane", "lastName": "Doe"}'
# create a key-value topic with key and value schemas
python ${base_dir}/create_key_value_topic.py --topic key_value_topic --bootstrap-servers "${kafka_server}" --schema-registry "${schema_reg}" --key-schema-file ${base_dir}/key_schema.avsc --value-schema-file ${base_dir}/value_schema.avsc --record-key '{"id": 100, "namespace": "bar"}' --record-value '{"email": "foo@foo.com", "firstName": "Jane", "lastName": "Doe"}'
# create a key-value topic without any value schema
python ${base_dir}/create_key_value_topic.py --topic key_topic --bootstrap-servers "${kafka_server}" --schema-registry "${schema_reg}" --key-schema-file ${base_dir}/key_schema.avsc --record-key '{"id": 100, "namespace": "bar"}'
