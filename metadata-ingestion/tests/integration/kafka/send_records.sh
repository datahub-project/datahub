#!/bin/sh
base_dir=$1

#echo $base_dir
python ${base_dir}/create_key_value_topic.py --topic foo --bootstrap-servers "localhost:59092" --schema-registry "http://localhost:58081" --key-schema-file ${base_dir}/key_schema.avsc --value-schema-file ${base_dir}/value_schema.avsc --record-key '{"id": 100, "namespace": "bar"}' --record-value '{"email": "foo@foo.com", "firstName": "Jane", "lastName": "Doe"}'
