#!/bin/bash

threshold=600
queues=$(aws --region us-west-2 sqs list-queues --queue-name-prefix re-ci-smoke-test | jq -r '.QueueUrls[]')
now=$(date +%s)

echo post-list

for url in $queues; do
  created_at=$(aws --region us-west-2 sqs get-queue-attributes --queue-url $url --attribute-names CreatedTimestamp | jq -r .Attributes.CreatedTimestamp)
  if [ -n "${created_at}" -a -n "${now}" ]; then
    age_seconds=$(($now - $created_at))
    if [ "${age_seconds}" -gt "${threshold}" ]; then
      echo "SQS queue ${url} is ${age_seconds} seconds old. Deleting."
      aws --region us-west-2 sqs delete-queue --queue-url "${url}"
    else
      echo "Skipping $url"
    fi
  fi
done
